// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc
 *
 * Author: Casey Bodley <cbodley@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <mutex>
#include <boost/container/flat_map.hpp>

#include "common/bounded_key_counter.h"
#include "common/errno.h"
#include "rgw_sync_log_trim.h"
#include "rgw_cr_rados.h"
#include "rgw_metadata.h"
#include "rgw_rados.h"
#include "rgw_sync.h"

#include <boost/asio/yield.hpp>
#include "include/assert.h"

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "trim: ")

using rgw::BucketTrimConfig;
using BucketChangeCounter = BoundedKeyCounter<std::string, int>;

const std::string rgw::BucketTrimStatus::oid = "bilog.trim";
using rgw::BucketTrimStatus;


// watch/notify api for gateways to coordinate about which buckets to trim
enum TrimNotifyType {
  NotifyTrimCounters = 0,
};
WRITE_RAW_ENCODER(TrimNotifyType);

struct TrimNotifyHandler {
  virtual ~TrimNotifyHandler() = default;

  virtual void handle(bufferlist::iterator& input, bufferlist& output) = 0;
};

/// api to share the bucket trim counters between gateways in the same zone.
/// each gateway will process different datalog shards, so the gateway that runs
/// the trim process needs to accumulate their counters
struct TrimCounters {
  /// counter for a single bucket
  struct BucketCounter {
    std::string bucket;
    int count{0};

    BucketCounter() = default;
    BucketCounter(const std::string& bucket, int count)
      : bucket(bucket), count(count) {}

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator& p);
  };
  using Vector = std::vector<BucketCounter>;

  /// request bucket trim counters from peer gateways
  struct Request {
    uint16_t max_buckets; //< maximum number of bucket counters to return

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator& p);
  };

  /// return the current bucket trim counters
  struct Response {
    Vector bucket_counters;

    void encode(bufferlist& bl) const;
    void decode(bufferlist::iterator& p);
  };

  /// server interface to query the hottest buckets
  struct Server {
    virtual ~Server() = default;

    virtual void get_bucket_counters(int count, Vector& counters) = 0;
  };

  /// notify handler
  class Handler : public TrimNotifyHandler {
    Server *const server;
   public:
    Handler(Server *server) : server(server) {}

    void handle(bufferlist::iterator& input, bufferlist& output) override;
  };
};
std::ostream& operator<<(std::ostream& out, const TrimCounters::BucketCounter& rhs)
{
  return out << rhs.bucket << ":" << rhs.count;
}

void TrimCounters::BucketCounter::encode(bufferlist& bl) const
{
  // no versioning to save space
  ::encode(bucket, bl);
  ::encode(count, bl);
}
void TrimCounters::BucketCounter::decode(bufferlist::iterator& p)
{
  ::decode(bucket, p);
  ::decode(count, p);
}
WRITE_CLASS_ENCODER(TrimCounters::BucketCounter);

void TrimCounters::Request::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(max_buckets, bl);
  ENCODE_FINISH(bl);
}
void TrimCounters::Request::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(max_buckets, p);
  DECODE_FINISH(p);
}
WRITE_CLASS_ENCODER(TrimCounters::Request);

void TrimCounters::Response::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ::encode(bucket_counters, bl);
  ENCODE_FINISH(bl);
}
void TrimCounters::Response::decode(bufferlist::iterator& p)
{
  DECODE_START(1, p);
  ::decode(bucket_counters, p);
  DECODE_FINISH(p);
}
WRITE_CLASS_ENCODER(TrimCounters::Response);

void TrimCounters::Handler::handle(bufferlist::iterator& input,
                                   bufferlist& output)
{
  Request request;
  ::decode(request, input);
  auto count = std::min<uint16_t>(request.max_buckets, 128);

  Response response;
  server->get_bucket_counters(count, response.bucket_counters);
  ::encode(response, output);
}


/// rados watcher for bucket trim notifications
class BucketTrimWatcher : public librados::WatchCtx2 {
  RGWRados *const store;
  const rgw_raw_obj& obj;
  rgw_rados_ref ref;
  uint64_t handle{0};

  using HandlerPtr = std::unique_ptr<TrimNotifyHandler>;
  boost::container::flat_map<TrimNotifyType, HandlerPtr> handlers;

 public:
  BucketTrimWatcher(RGWRados *store, const rgw_raw_obj& obj,
                    TrimCounters::Server *counters)
    : store(store), obj(obj)
  {
    handlers.emplace(NotifyTrimCounters, new TrimCounters::Handler(counters));
  }

  ~BucketTrimWatcher()
  {
    stop();
  }

  int start()
  {
    int r = store->get_raw_obj_ref(obj, &ref);
    if (r < 0) {
      return r;
    }

    // register a watch on the realm's control object
    r = ref.ioctx.watch2(ref.oid, &handle, this);
    if (r == -ENOENT) {
      constexpr bool exclusive = true;
      r = ref.ioctx.create(ref.oid, exclusive);
      if (r == -EEXIST || r == 0) {
        r = ref.ioctx.watch2(ref.oid, &handle, this);
      }
    }
    if (r < 0) {
      lderr(store->ctx()) << "Failed to watch " << ref.oid
          << " with " << cpp_strerror(-r) << dendl;
      ref.ioctx.close();
      return r;
    }

    ldout(store->ctx(), 10) << "Watching " << ref.oid << dendl;
    return 0;
  }

  int restart()
  {
    int r = ref.ioctx.unwatch2(handle);
    if (r < 0) {
      lderr(store->ctx()) << "Failed to unwatch on " << ref.oid
          << " with " << cpp_strerror(-r) << dendl;
    }
    r = ref.ioctx.watch2(ref.oid, &handle, this);
    if (r < 0) {
      lderr(store->ctx()) << "Failed to restart watch on " << ref.oid
          << " with " << cpp_strerror(-r) << dendl;
      ref.ioctx.close();
    }
    return r;
  }

  void stop()
  {
    ref.ioctx.unwatch2(handle);
    ref.ioctx.close();
  }

  /// respond to bucket trim notifications
  void handle_notify(uint64_t notify_id, uint64_t cookie,
                     uint64_t notifier_id, bufferlist& bl) override
  {
    if (cookie != handle) {
      return;
    }
    bufferlist reply;
    try {
      auto p = bl.begin();
      TrimNotifyType type;
      ::decode(type, p);

      auto handler = handlers.find(type);
      if (handler != handlers.end()) {
        handler->second->handle(p, reply);
      } else {
        lderr(store->ctx()) << "no handler for notify type " << type << dendl;
      }
    } catch (const buffer::error& e) {
      lderr(store->ctx()) << "Failed to decode notification: " << e.what() << dendl;
    }
    ref.ioctx.notify_ack(ref.oid, notify_id, cookie, reply);
  }

  /// reestablish the watch if it gets disconnected
  void handle_error(uint64_t cookie, int err) override
  {
    if (cookie != handle) {
      return;
    }
    if (err == -ENOTCONN) {
      ldout(store->ctx(), 4) << "Disconnected watch on " << ref.oid << dendl;
      restart();
    }
  }
};


/// trim the bilog of all of the given bucket instance's shards
class BucketTrimInstanceCR : public RGWCoroutine {
  RGWRados *const store;
  std::string bucket_instance;

 public:
  BucketTrimInstanceCR(RGWRados *store, const std::string& bucket_instance)
    : RGWCoroutine(store->ctx()), store(store),
      bucket_instance(bucket_instance)
  {}
  int operate() {
    return set_cr_done();
  }
};

/// trim each bucket instance while limiting the number of concurrent operations
class BucketTrimInstanceCollectCR : public RGWShardCollectCR {
  RGWRados *const store;
  std::vector<std::string>::const_iterator bucket;
  std::vector<std::string>::const_iterator end;
 public:
  BucketTrimInstanceCollectCR(RGWRados *store,
                              const std::vector<std::string>& buckets,
                              int max_concurrent)
    : RGWShardCollectCR(store->ctx(), max_concurrent),
      store(store),
      bucket(buckets.begin()), end(buckets.end())
  {}
  bool spawn_next() override;
};

bool BucketTrimInstanceCollectCR::spawn_next()
{
  if (bucket == end) {
    return false;
  }
  spawn(new BucketTrimInstanceCR(store, *bucket), false);
  ++bucket;
  return true;
}

/// correlate the replies from each peer gateway into the given counter
int accumulate_peer_counters(bufferlist& bl, BucketChangeCounter& counter)
{
  counter.clear();

  try {
    // decode notify responses
    auto p = bl.begin();
    std::map<std::pair<uint64_t, uint64_t>, bufferlist> replies;
    std::set<std::pair<uint64_t, uint64_t>> timeouts;
    ::decode(replies, p);
    ::decode(timeouts, p);

    for (auto& peer : replies) {
      auto q = peer.second.begin();
      TrimCounters::Response response;
      ::decode(response, q);
      for (const auto& b : response.bucket_counters) {
        counter.insert(b.bucket, b.count);
      }
    }
  } catch (const buffer::error& e) {
    return -EIO;
  }
  return 0;
}

/// metadata callback has the signature bool(string&& key, string&& marker)
using MetadataListCallback = std::function<bool(std::string&&, std::string&&)>;

/// lists metadata keys, passing each to a callback until it returns false.
/// on reaching the end, it will restart at the beginning and list up to the
/// initial marker
class AsyncMetadataList : public RGWAsyncRadosRequest {
  CephContext *const cct;
  RGWMetadataManager *const mgr;
  const std::string section;
  const std::string start_marker;
  MetadataListCallback callback;
  void *handle{nullptr};

  int _send_request() override;
 public:
  AsyncMetadataList(CephContext *cct, RGWCoroutine *caller,
                    RGWAioCompletionNotifier *cn, RGWMetadataManager *mgr,
                    const std::string& section, const std::string& start_marker,
                    const MetadataListCallback& callback)
    : RGWAsyncRadosRequest(caller, cn), cct(cct), mgr(mgr),
      section(section), start_marker(start_marker), callback(callback)
  {}
  ~AsyncMetadataList() override {
    if (handle) {
      mgr->list_keys_complete(handle);
    }
  }
};

int AsyncMetadataList::_send_request()
{
  // start a listing at the given marker
  int r = mgr->list_keys_init(section, start_marker, &handle);
  if (r < 0) {
    ldout(cct, 10) << "failed to init metadata listing: "
        << cpp_strerror(r) << dendl;
    return r;
  }
  ldout(cct, 20) << "starting metadata listing at " << start_marker << dendl;

  std::list<std::string> keys;
  bool truncated{false};
  std::string marker;

  do {
    // get the next key and marker
    r = mgr->list_keys_next(handle, 1, keys, &truncated);
    if (r < 0) {
      ldout(cct, 10) << "failed to list metadata: "
          << cpp_strerror(r) << dendl;
      return r;
    }
    marker = mgr->get_marker(handle);

    if (!keys.empty()) {
      assert(keys.size() == 1);
      auto& key = keys.front();
      if (!callback(std::move(key), std::move(marker))) {
        return 0;
      }
    }
  } while (truncated);

  if (start_marker.empty()) {
    // already listed all keys
    return 0;
  }

  // restart the listing from the beginning (empty marker)
  mgr->list_keys_complete(handle);
  handle = nullptr;

  r = mgr->list_keys_init(section, "", &handle);
  if (r < 0) {
    ldout(cct, 10) << "failed to restart metadata listing: "
        << cpp_strerror(r) << dendl;
    return r;
  }
  ldout(cct, 20) << "restarting metadata listing" << dendl;

  do {
    // get the next key and marker
    r = mgr->list_keys_next(handle, 1, keys, &truncated);
    if (r < 0) {
      ldout(cct, 10) << "failed to list metadata: "
          << cpp_strerror(r) << dendl;
      return r;
    }
    marker = mgr->get_marker(handle);

    if (!keys.empty()) {
      assert(keys.size() == 1);
      auto& key = keys.front();
      // stop at original marker
      if (marker >= start_marker) {
        return 0;
      }
      if (!callback(std::move(key), std::move(marker))) {
        return 0;
      }
    }
  } while (truncated);

  return 0;
}

/// coroutine wrapper for AsyncMetadataList
class MetadataListCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *const async_rados;
  RGWMetadataManager *const mgr;
  const std::string& section;
  const std::string& start_marker;
  MetadataListCallback callback;
  RGWAsyncRadosRequest *req{nullptr};
 public:
  MetadataListCR(CephContext *cct, RGWAsyncRadosProcessor *async_rados,
                 RGWMetadataManager *mgr, const std::string& section,
                 const std::string& start_marker,
                 const MetadataListCallback& callback)
    : RGWSimpleCoroutine(cct), async_rados(async_rados), mgr(mgr),
      section(section), start_marker(start_marker), callback(callback)
  {}
  ~MetadataListCR() override {
    request_cleanup();
  }

  int send_request() override {
    req = new AsyncMetadataList(cct, this, stack->create_completion_notifier(),
                                mgr, section, start_marker, callback);
    async_rados->queue(req);
    return 0;
  }
  int request_complete() override {
    return req->get_ret_status();
  }
  void request_cleanup() override {
    if (req) {
      req->finish();
      req = nullptr;
    }
  }
};

class BucketTrimCR : public RGWCoroutine {
  RGWRados *const store;
  const BucketTrimConfig& config;
  const rgw_raw_obj& obj;
  bufferlist notify_replies;
  BucketChangeCounter counter;
  std::vector<std::string> buckets; //< buckets selected for trim

 public:
  BucketTrimCR(RGWRados *store, const BucketTrimConfig& config,
               const rgw_raw_obj& obj)
    : RGWCoroutine(store->ctx()), store(store), config(config),
      obj(obj), counter(config.counter_size)
  {}

  int operate();
};

int BucketTrimCR::operate()
{
  reenter(this) {
    if (config.buckets_per_interval) {
      // query watch/notify for hot buckets
      ldout(cct, 10) << "fetching active bucket counters" << dendl;
      set_status("fetching active bucket counters");
      yield {
        // request the top bucket counters from each peer gateway
        const TrimNotifyType type = NotifyTrimCounters;
        TrimCounters::Request request{32};
        bufferlist bl;
        ::encode(type, bl);
        ::encode(request, bl);
        call(new RGWRadosNotifyCR(store, obj, bl, config.notify_timeout_ms,
                                  &notify_replies));
      }
      if (retcode < 0) {
        ldout(cct, 10) << "failed to fetch peer bucket counters" << dendl;
        return set_cr_error(retcode);
      }

      // select the hottest buckets for trim
      retcode = accumulate_peer_counters(notify_replies, counter);
      if (retcode < 0) {
        ldout(cct, 4) << "failed to correlate peer bucket counters" << dendl;
        return set_cr_error(retcode);
      }
      buckets.reserve(config.buckets_per_interval);

      const int max_count = config.buckets_per_interval;
      counter.get_highest(max_count,
        [this] (const std::string& bucket, int count) {
          buckets.push_back(bucket);
        });
    }

    // trim bucket instances with limited concurrency
    set_status("trimming buckets");
    ldout(cct, 4) << "collected " << buckets.size() << " buckets for trim" << dendl;
    yield call(new BucketTrimInstanceCollectCR(store, buckets,
                                               config.concurrent_buckets));
    // ignore errors from individual buckets

    return set_cr_done();
  }
  return 0;
}

class BucketTrimPollCR : public RGWCoroutine {
  RGWRados *const store;
  const BucketTrimConfig& config;
  const rgw_raw_obj& obj;
  const std::string name{"trim"}; //< lock name
  const std::string cookie;

 public:
  BucketTrimPollCR(RGWRados *store, const BucketTrimConfig& config,
                   const rgw_raw_obj& obj)
    : RGWCoroutine(store->ctx()), store(store), config(config), obj(obj),
      cookie(RGWSimpleRadosLockCR::gen_random_cookie(cct))
  {}

  int operate();
};

int BucketTrimPollCR::operate()
{
  reenter(this) {
    for (;;) {
      set_status("sleeping");
      wait(utime_t{config.trim_interval_sec, 0});

      // prevent others from trimming for our entire wait interval
      set_status("acquiring trim lock");
      yield call(new RGWSimpleRadosLockCR(store->get_async_rados(), store,
                                          obj, name, cookie,
                                          config.trim_interval_sec));
      if (retcode < 0) {
        ldout(cct, 4) << "failed to lock: " << cpp_strerror(retcode) << dendl;
        continue;
      }

      set_status("trimming");
      yield call(new BucketTrimCR(store, config, obj));
      if (retcode < 0) {
        // on errors, unlock so other gateways can try
        set_status("unlocking");
        yield call(new RGWSimpleRadosUnlockCR(store->get_async_rados(), store,
                                              obj, name, cookie));
      }
    }
  }
  return 0;
}

namespace rgw {

class BucketTrimManager::Impl : public TrimCounters::Server {
 public:
  RGWRados *const store;
  const BucketTrimConfig config;

  const rgw_raw_obj status_obj;

  /// count frequency of bucket instance entries in the data changes log
  BucketChangeCounter counter;

  /// serve the bucket trim watch/notify api
  BucketTrimWatcher watcher;

  /// protect data shared between data sync, trim, and watch/notify threads
  std::mutex mutex;

  Impl(RGWRados *store, const BucketTrimConfig& config)
    : store(store), config(config),
      status_obj(store->get_zone_params().log_pool, BucketTrimStatus::oid),
      counter(config.counter_size),
      watcher(store, status_obj, this)
  {}

  /// TrimCounters::Server interface for watch/notify api
  void get_bucket_counters(int count, TrimCounters::Vector& buckets)
  {
    buckets.reserve(count);
    std::lock_guard<std::mutex> lock(mutex);
    counter.get_highest(count, [&buckets] (const std::string& key, int count) {
                          buckets.emplace_back(key, count);
                        });
    ldout(store->ctx(), 20) << "get_bucket_counters: " << buckets << dendl;
  }
};

BucketTrimManager::BucketTrimManager(RGWRados *store,
                                     const BucketTrimConfig& config)
  : impl(new Impl(store, config))
{
}
BucketTrimManager::~BucketTrimManager() = default;

int BucketTrimManager::init()
{
  return impl->watcher.start();
}

void BucketTrimManager::on_bucket_changed(const boost::string_view& bucket)
{
  std::lock_guard<std::mutex> lock(impl->mutex);
  impl->counter.insert(bucket.to_string());
}

RGWCoroutine* BucketTrimManager::create_bucket_trim_cr()
{
  return new BucketTrimPollCR(impl->store, impl->config, impl->status_obj);
}

} // namespace rgw
