// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

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
#include <boost/circular_buffer.hpp>
#include <boost/container/flat_map.hpp>

#include "include/scope_guard.h"
#include "common/bounded_key_counter.h"
#include "common/errno.h"
#include "rgw_trim_bilog.h"
#include "rgw_cr_rados.h"
#include "rgw_cr_rest.h"
#include "rgw_cr_tools.h"
#include "rgw_data_sync.h"
#include "rgw_metadata.h"
#include "rgw_sal.h"
#include "rgw_zone.h"
#include "rgw_sync.h"
#include "rgw_bucket.h"

#include "services/svc_zone.h"
#include "services/svc_bilog_rados.h"

#include <boost/asio/yield.hpp>
#include "include/ceph_assert.h"

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "trim: ")

using namespace std;

using rgw::BucketTrimConfig;
using BucketChangeCounter = BoundedKeyCounter<std::string, int>;

const std::string rgw::BucketTrimStatus::oid = "bilog.trim";
using rgw::BucketTrimStatus;


// watch/notify api for gateways to coordinate about which buckets to trim
enum TrimNotifyType {
  NotifyTrimCounters = 0,
  NotifyTrimComplete,
};
WRITE_RAW_ENCODER(TrimNotifyType);

struct TrimNotifyHandler {
  virtual ~TrimNotifyHandler() = default;

  virtual void handle(bufferlist::const_iterator& input, bufferlist& output) = 0;
};

/// api to share the bucket trim counters between gateways in the same zone.
/// each gateway will process different datalog shards, so the gateway that runs
/// the trim process needs to accumulate their counters
struct TrimCounters {
  /// counter for a single bucket
  struct BucketCounter {
    std::string bucket; //< bucket instance metadata key
    int count{0};

    BucketCounter() = default;
    BucketCounter(const std::string& bucket, int count)
      : bucket(bucket), count(count) {}

    void encode(bufferlist& bl) const;
    void decode(bufferlist::const_iterator& p);
  };
  using Vector = std::vector<BucketCounter>;

  /// request bucket trim counters from peer gateways
  struct Request {
    uint16_t max_buckets; //< maximum number of bucket counters to return

    void encode(bufferlist& bl) const;
    void decode(bufferlist::const_iterator& p);
  };

  /// return the current bucket trim counters
  struct Response {
    Vector bucket_counters;

    void encode(bufferlist& bl) const;
    void decode(bufferlist::const_iterator& p);
  };

  /// server interface to query the hottest buckets
  struct Server {
    virtual ~Server() = default;

    virtual void get_bucket_counters(int count, Vector& counters) = 0;
    virtual void reset_bucket_counters() = 0;
  };

  /// notify handler
  class Handler : public TrimNotifyHandler {
    Server *const server;
   public:
    explicit Handler(Server *server) : server(server) {}

    void handle(bufferlist::const_iterator& input, bufferlist& output) override;
  };
};
std::ostream& operator<<(std::ostream& out, const TrimCounters::BucketCounter& rhs)
{
  return out << rhs.bucket << ":" << rhs.count;
}

void TrimCounters::BucketCounter::encode(bufferlist& bl) const
{
  using ceph::encode;
  // no versioning to save space
  encode(bucket, bl);
  encode(count, bl);
}
void TrimCounters::BucketCounter::decode(bufferlist::const_iterator& p)
{
  using ceph::decode;
  decode(bucket, p);
  decode(count, p);
}
WRITE_CLASS_ENCODER(TrimCounters::BucketCounter);

void TrimCounters::Request::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  encode(max_buckets, bl);
  ENCODE_FINISH(bl);
}
void TrimCounters::Request::decode(bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(max_buckets, p);
  DECODE_FINISH(p);
}
WRITE_CLASS_ENCODER(TrimCounters::Request);

void TrimCounters::Response::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  encode(bucket_counters, bl);
  ENCODE_FINISH(bl);
}
void TrimCounters::Response::decode(bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  decode(bucket_counters, p);
  DECODE_FINISH(p);
}
WRITE_CLASS_ENCODER(TrimCounters::Response);

void TrimCounters::Handler::handle(bufferlist::const_iterator& input,
                                   bufferlist& output)
{
  Request request;
  decode(request, input);
  auto count = std::min<uint16_t>(request.max_buckets, 128);

  Response response;
  server->get_bucket_counters(count, response.bucket_counters);
  encode(response, output);
}

/// api to notify peer gateways that trim has completed and their bucket change
/// counters can be reset
struct TrimComplete {
  struct Request {
    void encode(bufferlist& bl) const;
    void decode(bufferlist::const_iterator& p);
  };
  struct Response {
    void encode(bufferlist& bl) const;
    void decode(bufferlist::const_iterator& p);
  };

  /// server interface to reset bucket counters
  using Server = TrimCounters::Server;

  /// notify handler
  class Handler : public TrimNotifyHandler {
    Server *const server;
   public:
    explicit Handler(Server *server) : server(server) {}

    void handle(bufferlist::const_iterator& input, bufferlist& output) override;
  };
};

void TrimComplete::Request::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ENCODE_FINISH(bl);
}
void TrimComplete::Request::decode(bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  DECODE_FINISH(p);
}
WRITE_CLASS_ENCODER(TrimComplete::Request);

void TrimComplete::Response::encode(bufferlist& bl) const
{
  ENCODE_START(1, 1, bl);
  ENCODE_FINISH(bl);
}
void TrimComplete::Response::decode(bufferlist::const_iterator& p)
{
  DECODE_START(1, p);
  DECODE_FINISH(p);
}
WRITE_CLASS_ENCODER(TrimComplete::Response);

void TrimComplete::Handler::handle(bufferlist::const_iterator& input,
                                   bufferlist& output)
{
  Request request;
  decode(request, input);

  server->reset_bucket_counters();

  Response response;
  encode(response, output);
}


/// rados watcher for bucket trim notifications
class BucketTrimWatcher : public librados::WatchCtx2 {
  rgw::sal::RadosStore* const store;
  const rgw_raw_obj& obj;
  rgw_rados_ref ref;
  uint64_t handle{0};

  using HandlerPtr = std::unique_ptr<TrimNotifyHandler>;
  boost::container::flat_map<TrimNotifyType, HandlerPtr> handlers;

 public:
  BucketTrimWatcher(rgw::sal::RadosStore* store, const rgw_raw_obj& obj,
                    TrimCounters::Server *counters)
    : store(store), obj(obj) {
    handlers.emplace(NotifyTrimCounters,
        std::make_unique<TrimCounters::Handler>(counters));
    handlers.emplace(NotifyTrimComplete,
        std::make_unique<TrimComplete::Handler>(counters));
  }

  ~BucketTrimWatcher() {
    stop();
  }

  int start(const DoutPrefixProvider *dpp) {
    int r = store->getRados()->get_raw_obj_ref(dpp, obj, &ref);
    if (r < 0) {
      return r;
    }

    // register a watch on the realm's control object
    r = ref.ioctx.watch2(ref.obj.oid, &handle, this);
    if (r == -ENOENT) {
      constexpr bool exclusive = true;
      r = ref.ioctx.create(ref.obj.oid, exclusive);
      if (r == -EEXIST || r == 0) {
        r = ref.ioctx.watch2(ref.obj.oid, &handle, this);
      }
    }
    if (r < 0) {
      ldpp_dout(dpp, -1) << "Failed to watch " << ref.obj
          << " with " << cpp_strerror(-r) << dendl;
      ref.ioctx.close();
      return r;
    }

    ldpp_dout(dpp, 10) << "Watching " << ref.obj.oid << dendl;
    return 0;
  }

  int restart() {
    int r = ref.ioctx.unwatch2(handle);
    if (r < 0) {
      lderr(store->ctx()) << "Failed to unwatch on " << ref.obj
          << " with " << cpp_strerror(-r) << dendl;
    }
    r = ref.ioctx.watch2(ref.obj.oid, &handle, this);
    if (r < 0) {
      lderr(store->ctx()) << "Failed to restart watch on " << ref.obj
          << " with " << cpp_strerror(-r) << dendl;
      ref.ioctx.close();
    }
    return r;
  }

  void stop() {
    if (handle) {
      ref.ioctx.unwatch2(handle);
      ref.ioctx.close();
    }
  }

  /// respond to bucket trim notifications
  void handle_notify(uint64_t notify_id, uint64_t cookie,
                     uint64_t notifier_id, bufferlist& bl) override {
    if (cookie != handle) {
      return;
    }
    bufferlist reply;
    try {
      auto p = bl.cbegin();
      TrimNotifyType type;
      decode(type, p);

      auto handler = handlers.find(type);
      if (handler != handlers.end()) {
        handler->second->handle(p, reply);
      } else {
        lderr(store->ctx()) << "no handler for notify type " << type << dendl;
      }
    } catch (const buffer::error& e) {
      lderr(store->ctx()) << "Failed to decode notification: " << e.what() << dendl;
    }
    ref.ioctx.notify_ack(ref.obj.oid, notify_id, cookie, reply);
  }

  /// reestablish the watch if it gets disconnected
  void handle_error(uint64_t cookie, int err) override {
    if (cookie != handle) {
      return;
    }
    if (err == -ENOTCONN) {
      ldout(store->ctx(), 4) << "Disconnected watch on " << ref.obj << dendl;
      restart();
    }
  }
};


/// Interface to communicate with the trim manager about completed operations
struct BucketTrimObserver {
  virtual ~BucketTrimObserver() = default;

  virtual void on_bucket_trimmed(std::string&& bucket_instance) = 0;
  virtual bool trimmed_recently(const std::string_view& bucket_instance) = 0;
};

/// trim each bilog shard to the given marker, while limiting the number of
/// concurrent requests
class BucketTrimShardCollectCR : public RGWShardCollectCR {
  static constexpr int MAX_CONCURRENT_SHARDS = 16;
  const DoutPrefixProvider *dpp;
  rgw::sal::RadosStore* const store;
  const RGWBucketInfo& bucket_info;
  rgw::bucket_index_layout_generation generation;
  const std::vector<std::string>& markers; //< shard markers to trim
  size_t i{0}; //< index of current shard marker

  int handle_result(int r) override {
    if (r == -ENOENT) { // ENOENT is not a fatal error
      return 0;
    }
    if (r < 0) {
      ldout(cct, 4) << "failed to trim bilog shard: " << cpp_strerror(r) << dendl;
    }
    return r;
  }
 public:
  BucketTrimShardCollectCR(const DoutPrefixProvider *dpp,
                           rgw::sal::RadosStore* store, const RGWBucketInfo& bucket_info,
			   const rgw::bucket_index_layout_generation& generation,
                           const std::vector<std::string>& markers)
    : RGWShardCollectCR(store->ctx(), MAX_CONCURRENT_SHARDS),
      dpp(dpp), store(store), bucket_info(bucket_info),
      generation(generation), markers(markers)
  {}
  bool spawn_next() override;
};

bool BucketTrimShardCollectCR::spawn_next()
{
  while (i < markers.size()) {
    const auto& marker = markers[i];
    const auto shard_id = i++;

    // skip empty markers
    if (!marker.empty()) {
      ldpp_dout(dpp, 10) << "trimming bilog shard " << shard_id
          << " of " << bucket_info.bucket << " at marker " << marker << dendl;
      spawn(new RGWRadosBILogTrimCR(dpp, store, bucket_info, shard_id,
                                    generation, std::string{}, marker),
            false);
      return true;
    }
  }
  return false;
}

/// Delete a BI generation, limiting the number of requests in flight.
class BucketCleanIndexCollectCR : public RGWShardCollectCR {
  static constexpr int MAX_CONCURRENT_SHARDS = 16;
  const DoutPrefixProvider *dpp;
  rgw::sal::RadosStore* const store;
  const RGWBucketInfo& bucket_info;
  rgw::bucket_index_layout_generation index;
  uint32_t shard = 0;
  const uint32_t num_shards = rgw::num_shards(index);

  int handle_result(int r) override {
    if (r == -ENOENT) { // ENOENT is not a fatal error
      return 0;
    }
    if (r < 0) {
      ldout(cct, 4) << "clean index: " << cpp_strerror(r) << dendl;
    }
    return r;
  }
 public:
  BucketCleanIndexCollectCR(const DoutPrefixProvider *dpp,
			    rgw::sal::RadosStore* store,
			    const RGWBucketInfo& bucket_info,
			    rgw::bucket_index_layout_generation index)
    : RGWShardCollectCR(store->ctx(), MAX_CONCURRENT_SHARDS),
      dpp(dpp), store(store), bucket_info(bucket_info),
      index(index)
  {}
  bool spawn_next() override {
    if (shard < num_shards) {
      RGWRados::BucketShard bs(store->getRados());
      bs.init(dpp, bucket_info, index, shard, null_yield);
      spawn(new RGWRadosRemoveOidCR(store, std::move(bs.bucket_obj), nullptr),
	    false);
      ++shard;
      return true;
    } else {
      return false;
    }
  }
};


/// trim the bilog of all of the given bucket instance's shards
class BucketTrimInstanceCR : public RGWCoroutine {
  static constexpr auto MAX_RETRIES = 25u;
  rgw::sal::RadosStore* const store;
  RGWHTTPManager *const http;
  BucketTrimObserver *const observer;
  std::string bucket_instance;
  rgw_bucket_get_sync_policy_params get_policy_params;
  std::shared_ptr<rgw_bucket_get_sync_policy_result> source_policy;
  rgw_bucket bucket;
  const std::string& zone_id; //< my zone id
  RGWBucketInfo _bucket_info;
  const RGWBucketInfo *pbucket_info; //< pointer to bucket instance info to locate bucket indices
  int child_ret = 0;
  const DoutPrefixProvider *dpp;
public:
  struct StatusShards {
    uint64_t generation = 0;
    std::vector<rgw_bucket_shard_sync_info> shards;
  };
private:
  std::vector<StatusShards> peer_status; //< sync status for each peer
  std::vector<std::string> min_markers; //< min marker per shard

  /// The log generation to trim
  rgw::bucket_log_layout_generation totrim;

  /// Generation to be cleaned/New bucket info (if any)
  std::optional<std::pair<RGWBucketInfo,
			  rgw::bucket_log_layout_generation>> clean_info;
  /// Maximum number of times to attempt to put bucket info
  unsigned retries = 0;

  int take_min_generation() {
    // Initialize the min_generation to the bucket's current
    // generation, used in case we have no peers.
    auto min_generation = pbucket_info->layout.logs.back().gen;

    // Determine the minimum generation
    if (auto m = std::min_element(peer_status.begin(),
				  peer_status.end(),
				  [](const StatusShards& l,
				     const StatusShards& r) {
				    return l.generation < r.generation;
				  }); m != peer_status.end()) {
      min_generation = m->generation;
    }

    auto& logs = pbucket_info->layout.logs;
    auto log = std::find_if(logs.begin(), logs.end(),
			    rgw::matches_gen(min_generation));
    if (log == logs.end()) {
      ldpp_dout(dpp, 5) << __PRETTY_FUNCTION__ << ":" << __LINE__
			<< "ERROR: No log layout for min_generation="
			<< min_generation << dendl;
      return -ENOENT;
    }

    totrim = *log;
    return 0;
  }

  /// If there is a generation below the minimum, prepare to clean it up.
  int maybe_remove_generation() {
    if (clean_info)
      return 0;


    if (pbucket_info->layout.logs.front().gen < totrim.gen) {
      clean_info = {*pbucket_info, {}};
      auto log = clean_info->first.layout.logs.cbegin();
      clean_info->second = *log;

      if (clean_info->first.layout.logs.size() == 1) {
	ldpp_dout(dpp, -1)
	  << "Critical error! Attempt to remove only log generation! "
	  << "log.gen=" << log->gen << ", totrim.gen=" << totrim.gen
	  << dendl;
	return -EIO;
      }
      clean_info->first.layout.logs.erase(log);
    }
    return 0;
  }

 public:
  BucketTrimInstanceCR(rgw::sal::RadosStore* store, RGWHTTPManager *http,
                       BucketTrimObserver *observer,
                       const std::string& bucket_instance,
                       const DoutPrefixProvider *dpp)
    : RGWCoroutine(store->ctx()), store(store),
      http(http), observer(observer),
      bucket_instance(bucket_instance),
      zone_id(store->svc()->zone->get_zone().id),
      dpp(dpp) {
    rgw_bucket_parse_bucket_key(cct, bucket_instance, &bucket, nullptr);
    source_policy = make_shared<rgw_bucket_get_sync_policy_result>();
  }

  int operate(const DoutPrefixProvider *dpp) override;
};

namespace {
/// populate the status with the minimum stable marker of each shard
int take_min_status(
  CephContext *cct,
  const uint64_t min_generation,
  std::vector<BucketTrimInstanceCR::StatusShards>::const_iterator first,
  std::vector<BucketTrimInstanceCR::StatusShards>::const_iterator last,
  std::vector<std::string> *status) {
  for (auto peer = first; peer != last; ++peer) {
    // Peers on later generations don't get a say in the matter
    if (peer->generation > min_generation) {
      continue;
    }
    if (peer->shards.size() != status->size()) {
      // all peers must agree on the number of shards
      return -EINVAL;
    }

    auto m = status->begin();
    for (auto& shard : peer->shards) {
      auto& marker = *m++;
      // always take the first marker, or any later marker that's smaller
      if (peer == first || marker > shard.inc_marker.position) {
	marker = std::move(shard.inc_marker.position);
      }
    }
  }
  return 0;
}
}

template<>
inline int parse_decode_json<BucketTrimInstanceCR::StatusShards>(
  BucketTrimInstanceCR::StatusShards& s, bufferlist& bl)
{
  JSONParser p;
  if (!p.parse(bl.c_str(), bl.length())) {
    return -EINVAL;
  }

  try {
    bilog_status_v2 v;
    decode_json_obj(v, &p);
    s.generation = v.sync_status.incremental_gen;
    s.shards = std::move(v.inc_status);
  } catch (JSONDecoder::err& e) {
    try {
      // Fall back if we're talking to an old node that can't give v2
      // output.
      s.generation = 0;
      decode_json_obj(s.shards, &p);
    } catch (JSONDecoder::err& e) {
      return -EINVAL;
    }
  }
  return 0;
}

int BucketTrimInstanceCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    ldpp_dout(dpp, 4) << "starting trim on bucket=" << bucket_instance << dendl;

    get_policy_params.zone = zone_id;
    get_policy_params.bucket = bucket;
    yield call(new RGWBucketGetSyncPolicyHandlerCR(store->svc()->async_processor,
                                                   store,
                                                   get_policy_params,
                                                   source_policy,
                                                   dpp));
    if (retcode < 0) {
      if (retcode != -ENOENT) {
        ldpp_dout(dpp, 0) << "ERROR: failed to fetch policy handler for bucket=" << bucket << dendl;
      }

      return set_cr_error(retcode);
    }

    if (auto& opt_bucket_info = source_policy->policy_handler->get_bucket_info();
        opt_bucket_info) {
      pbucket_info = &(*opt_bucket_info);
    } else {
      /* this shouldn't really happen */
      return set_cr_error(-ENOENT);
    }

    if (pbucket_info->layout.logs.empty()) {
      return set_cr_done(); // no bilogs to trim
    }

    // query peers for sync status
    set_status("fetching sync status from relevant peers");
    yield {
      const auto& all_dests = source_policy->policy_handler->get_all_dests();

      vector<rgw_zone_id> zids;
      rgw_zone_id last_zid;
      for (auto& diter : all_dests) {
        const auto& zid = diter.first;
        if (zid == last_zid) {
          continue;
        }
        last_zid = zid;
        zids.push_back(zid);
      }

      peer_status.resize(zids.size());

      auto& zone_conn_map = store->svc()->zone->get_zone_conn_map();

      auto p = peer_status.begin();
      for (auto& zid : zids) {
        // query data sync status from each sync peer
        rgw_http_param_pair params[] = {
          { "type", "bucket-index" },
          { "status", nullptr },
          { "options", "merge" },
          { "bucket", bucket_instance.c_str() }, /* equal to source-bucket when `options==merge` and source-bucket
                                                    param is not provided */
          { "source-zone", zone_id.c_str() },
          { "version", "2" },
          { nullptr, nullptr }
        };

        auto ziter = zone_conn_map.find(zid);
        if (ziter == zone_conn_map.end()) {
          ldpp_dout(dpp, 0) << "WARNING: no connection to zone " << zid << ", can't trim bucket: " << bucket << dendl;
          return set_cr_error(-ECANCELED);
        }

	using StatusCR = RGWReadRESTResourceCR<StatusShards>;
        spawn(new StatusCR(cct, ziter->second, http, "/admin/log/", params, &*p),
              false);
        ++p;
      }
    }
    // wait for a response from each peer. all must respond to attempt trim
    while (num_spawned()) {
      yield wait_for_child();
      collect(&child_ret, nullptr);
      if (child_ret < 0) {
        drain_all();
        return set_cr_error(child_ret);
      }
    }

    // Determine the minimum generation
    retcode = take_min_generation();
    if (retcode < 0) {
      ldpp_dout(dpp, 4) << "failed to find minimum generation" << dendl;
      return set_cr_error(retcode);
    }
    retcode = maybe_remove_generation();
    if (retcode < 0) {
      ldpp_dout(dpp, 4) << "error removing old generation from log: "
			<< cpp_strerror(retcode) << dendl;
      return set_cr_error(retcode);
    }

    if (clean_info) {
      if (clean_info->second.layout.type != rgw::BucketLogType::InIndex) {
	ldpp_dout(dpp, 0) << "Unable to convert log of unknown type "
			  << clean_info->second.layout.type
			  << " to rgw::bucket_index_layout_generation " << dendl;
	return set_cr_error(-EINVAL);
      }

      yield call(new BucketCleanIndexCollectCR(dpp, store, clean_info->first,
					       clean_info->second.layout.in_index));
      if (retcode < 0) {
	ldpp_dout(dpp, 0) << "failed to remove previous generation: "
			  << cpp_strerror(retcode) << dendl;
	return set_cr_error(retcode);
      }
      while (clean_info && retries < MAX_RETRIES) {
	yield call(new RGWPutBucketInstanceInfoCR(
		     store->svc()->async_processor,
		     store, clean_info->first, false, {},
		     no_change_attrs(), dpp));

	// Raced, try again.
	if (retcode == -ECANCELED) {
	  yield call(new RGWGetBucketInstanceInfoCR(
		       store->svc()->async_processor,
		       store, clean_info->first.bucket,
		       &(clean_info->first), nullptr, dpp));
	  if (retcode < 0) {
	    ldpp_dout(dpp, 0) << "failed to get bucket info: "
			      << cpp_strerror(retcode) << dendl;
	    return set_cr_error(retcode);
	  }
	  if (clean_info->first.layout.logs.front().gen ==
	      clean_info->second.gen) {
	    clean_info->first.layout.logs.erase(
	      clean_info->first.layout.logs.begin());
	    ++retries;
	    continue;
	  }
	  // Raced, but someone else did what we needed to.
	  retcode = 0;
	}

	if (retcode < 0) {
	  ldpp_dout(dpp, 0) << "failed to put bucket info: "
			    << cpp_strerror(retcode) << dendl;
	  return set_cr_error(retcode);
	}
	clean_info = std::nullopt;
      }
    } else {
      if (totrim.layout.type != rgw::BucketLogType::InIndex) {
	ldpp_dout(dpp, 0) << "Unable to convert log of unknown type "
			  << totrim.layout.type
			  << " to rgw::bucket_index_layout_generation " << dendl;
	return set_cr_error(-EINVAL);
      }
      // To avoid hammering the OSD too hard, either trim old
      // generations OR trim the current one.

      // determine the minimum marker for each shard

      // initialize each shard with the maximum marker, which is only used when
      // there are no peers syncing from us
      min_markers.assign(std::max(1u, rgw::num_shards(totrim.layout.in_index)),
			 RGWSyncLogTrimCR::max_marker);


      retcode = take_min_status(cct, totrim.gen, peer_status.cbegin(),
				peer_status.cend(), &min_markers);
      if (retcode < 0) {
	ldpp_dout(dpp, 4) << "failed to correlate bucket sync status from peers" << dendl;
	return set_cr_error(retcode);
      }

      // trim shards with a ShardCollectCR
      ldpp_dout(dpp, 10) << "trimming bilogs for bucket=" << pbucket_info->bucket
			 << " markers=" << min_markers << ", shards=" << min_markers.size() << dendl;
      set_status("trimming bilog shards");
      yield call(new BucketTrimShardCollectCR(dpp, store, *pbucket_info, totrim.layout.in_index,
					      min_markers));
      // ENODATA just means there were no keys to trim
      if (retcode == -ENODATA) {
	retcode = 0;
      }
      if (retcode < 0) {
	ldpp_dout(dpp, 4) << "failed to trim bilog shards: "
			  << cpp_strerror(retcode) << dendl;
	return set_cr_error(retcode);
      }
    }

    observer->on_bucket_trimmed(std::move(bucket_instance));
    return set_cr_done();
  }
  return 0;
}

/// trim each bucket instance while limiting the number of concurrent operations

class BucketTrimInstanceCollectCR : public RGWShardCollectCR {
  rgw::sal::RadosStore* const store;
  RGWHTTPManager *const http;
  BucketTrimObserver *const observer;
  std::vector<std::string>::const_iterator bucket;
  std::vector<std::string>::const_iterator end;
  const DoutPrefixProvider *dpp;

  int handle_result(int r) override {
    if (r == -ENOENT) { // ENOENT is not a fatal error
      return 0;
    }
    if (r < 0) {
      ldout(cct, 4) << "failed to trim bucket instance: " << cpp_strerror(r) << dendl;
    }
    return r;
  }
 public:
  BucketTrimInstanceCollectCR(rgw::sal::RadosStore* store, RGWHTTPManager *http,
                              BucketTrimObserver *observer,
                              const std::vector<std::string>& buckets,
                              int max_concurrent,
                              const DoutPrefixProvider *dpp)
    : RGWShardCollectCR(store->ctx(), max_concurrent),
      store(store), http(http), observer(observer),
      bucket(buckets.begin()), end(buckets.end()),
      dpp(dpp)
  {}
  bool spawn_next() override;
};

bool BucketTrimInstanceCollectCR::spawn_next()
{
  if (bucket == end) {
    return false;
  }
  spawn(new BucketTrimInstanceCR(store, http, observer, *bucket, dpp), false);
  ++bucket;
  return true;
}

/// correlate the replies from each peer gateway into the given counter
int accumulate_peer_counters(bufferlist& bl, BucketChangeCounter& counter)
{
  counter.clear();

  try {
    // decode notify responses
    auto p = bl.cbegin();
    std::map<std::pair<uint64_t, uint64_t>, bufferlist> replies;
    std::set<std::pair<uint64_t, uint64_t>> timeouts;
    decode(replies, p);
    decode(timeouts, p);

    for (auto& peer : replies) {
      auto q = peer.second.cbegin();
      TrimCounters::Response response;
      decode(response, q);
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

  int _send_request(const DoutPrefixProvider *dpp) override;
 public:
  AsyncMetadataList(CephContext *cct, RGWCoroutine *caller,
                    RGWAioCompletionNotifier *cn, RGWMetadataManager *mgr,
                    const std::string& section, const std::string& start_marker,
                    const MetadataListCallback& callback)
    : RGWAsyncRadosRequest(caller, cn), cct(cct), mgr(mgr),
      section(section), start_marker(start_marker), callback(callback)
  {}
};

int AsyncMetadataList::_send_request(const DoutPrefixProvider *dpp)
{
  void* handle = nullptr;
  std::list<std::string> keys;
  bool truncated{false};
  std::string marker;

  // start a listing at the given marker
  int r = mgr->list_keys_init(dpp, section, start_marker, &handle);
  if (r == -EINVAL) {
    // restart with empty marker below
  } else if (r < 0) {
    ldpp_dout(dpp, 10) << "failed to init metadata listing: "
        << cpp_strerror(r) << dendl;
    return r;
  } else {
    ldpp_dout(dpp, 20) << "starting metadata listing at " << start_marker << dendl;

    // release the handle when scope exits
    auto g = make_scope_guard([=, this] { mgr->list_keys_complete(handle); });

    do {
      // get the next key and marker
      r = mgr->list_keys_next(dpp, handle, 1, keys, &truncated);
      if (r < 0) {
        ldpp_dout(dpp, 10) << "failed to list metadata: "
            << cpp_strerror(r) << dendl;
        return r;
      }
      marker = mgr->get_marker(handle);

      if (!keys.empty()) {
        ceph_assert(keys.size() == 1);
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
  }

  // restart the listing from the beginning (empty marker)
  handle = nullptr;

  r = mgr->list_keys_init(dpp, section, "", &handle);
  if (r < 0) {
    ldpp_dout(dpp, 10) << "failed to restart metadata listing: "
        << cpp_strerror(r) << dendl;
    return r;
  }
  ldpp_dout(dpp, 20) << "restarting metadata listing" << dendl;

  // release the handle when scope exits
  auto g = make_scope_guard([=, this] { mgr->list_keys_complete(handle); });
  do {
    // get the next key and marker
    r = mgr->list_keys_next(dpp, handle, 1, keys, &truncated);
    if (r < 0) {
      ldpp_dout(dpp, 10) << "failed to list metadata: "
          << cpp_strerror(r) << dendl;
      return r;
    }
    marker = mgr->get_marker(handle);

    if (!keys.empty()) {
      ceph_assert(keys.size() == 1);
      auto& key = keys.front();
      // stop at original marker
      if (marker > start_marker) {
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

  int send_request(const DoutPrefixProvider *dpp) override {
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
  rgw::sal::RadosStore* const store;
  RGWHTTPManager *const http;
  const BucketTrimConfig& config;
  BucketTrimObserver *const observer;
  const rgw_raw_obj& obj;
  ceph::mono_time start_time;
  bufferlist notify_replies;
  BucketChangeCounter counter;
  std::vector<std::string> buckets; //< buckets selected for trim
  BucketTrimStatus status;
  RGWObjVersionTracker objv; //< version tracker for trim status object
  std::string last_cold_marker; //< position for next trim marker
  const DoutPrefixProvider *dpp;

  static const std::string section; //< metadata section for bucket instances
 public:
  BucketTrimCR(rgw::sal::RadosStore* store, RGWHTTPManager *http,
               const BucketTrimConfig& config, BucketTrimObserver *observer,
               const rgw_raw_obj& obj, const DoutPrefixProvider *dpp)
    : RGWCoroutine(store->ctx()), store(store), http(http), config(config),
      observer(observer), obj(obj), counter(config.counter_size), dpp(dpp)
  {}

  int operate(const DoutPrefixProvider *dpp) override;
};

const std::string BucketTrimCR::section{"bucket.instance"};

int BucketTrimCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    start_time = ceph::mono_clock::now();

    if (config.buckets_per_interval) {
      // query watch/notify for hot buckets
      ldpp_dout(dpp, 10) << "fetching active bucket counters" << dendl;
      set_status("fetching active bucket counters");
      yield {
        // request the top bucket counters from each peer gateway
        const TrimNotifyType type = NotifyTrimCounters;
        TrimCounters::Request request{32};
        bufferlist bl;
        encode(type, bl);
        encode(request, bl);
        call(new RGWRadosNotifyCR(store, obj, bl, config.notify_timeout_ms,
                                  &notify_replies));
      }
      if (retcode < 0) {
        ldpp_dout(dpp, 10) << "failed to fetch peer bucket counters" << dendl;
        return set_cr_error(retcode);
      }

      // select the hottest buckets for trim
      retcode = accumulate_peer_counters(notify_replies, counter);
      if (retcode < 0) {
        ldout(cct, 4) << "failed to correlate peer bucket counters" << dendl;
        return set_cr_error(retcode);
      }
      buckets.reserve(config.buckets_per_interval);

      const int max_count = config.buckets_per_interval -
                            config.min_cold_buckets_per_interval;
      counter.get_highest(max_count,
        [this] (const std::string& bucket, int count) {
          buckets.push_back(bucket);
        });
    }

    if (buckets.size() < config.buckets_per_interval) {
      // read BucketTrimStatus for marker position
      set_status("reading trim status");
      using ReadStatus = RGWSimpleRadosReadCR<BucketTrimStatus>;
      yield call(new ReadStatus(dpp, store, obj, &status, true, &objv));
      if (retcode < 0) {
        ldpp_dout(dpp, 10) << "failed to read bilog trim status: "
            << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }
      if (status.marker == "MAX") {
        status.marker.clear(); // restart at the beginning
      }
      ldpp_dout(dpp, 10) << "listing cold buckets from marker="
          << status.marker << dendl;

      set_status("listing cold buckets for trim");
      yield {
        // capture a reference so 'this' remains valid in the callback
        auto ref = boost::intrusive_ptr<RGWCoroutine>{this};
        // list cold buckets to consider for trim
        auto cb = [this, ref] (std::string&& bucket, std::string&& marker) {
          // filter out keys that we trimmed recently
          if (observer->trimmed_recently(bucket)) {
            return true;
          }
          // filter out active buckets that we've already selected
          auto i = std::find(buckets.begin(), buckets.end(), bucket);
          if (i != buckets.end()) {
            return true;
          }
          buckets.emplace_back(std::move(bucket));
          // remember the last cold bucket spawned to update the status marker
          last_cold_marker = std::move(marker);
          // return true if there's room for more
          return buckets.size() < config.buckets_per_interval;
        };

        call(new MetadataListCR(cct, store->svc()->async_processor,
                                store->ctl()->meta.mgr,
                                section, status.marker, cb));
      }
      if (retcode < 0) {
        ldout(cct, 4) << "failed to list bucket instance metadata: "
            << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }
    }

    // trim bucket instances with limited concurrency
    set_status("trimming buckets");
    ldpp_dout(dpp, 4) << "collected " << buckets.size() << " buckets for trim" << dendl;
    yield call(new BucketTrimInstanceCollectCR(store, http, observer, buckets,
                                               config.concurrent_buckets, dpp));
    // ignore errors from individual buckets

    // write updated trim status
    if (!last_cold_marker.empty() && status.marker != last_cold_marker) {
      set_status("writing updated trim status");
      status.marker = std::move(last_cold_marker);
      ldpp_dout(dpp, 20) << "writing bucket trim marker=" << status.marker << dendl;
      using WriteStatus = RGWSimpleRadosWriteCR<BucketTrimStatus>;
      yield call(new WriteStatus(dpp, store, obj, status, &objv));
      if (retcode < 0) {
        ldpp_dout(dpp, 4) << "failed to write updated trim status: "
            << cpp_strerror(retcode) << dendl;
        return set_cr_error(retcode);
      }
    }

    // notify peers that trim completed
    set_status("trim completed");
    yield {
      const TrimNotifyType type = NotifyTrimComplete;
      TrimComplete::Request request;
      bufferlist bl;
      encode(type, bl);
      encode(request, bl);
      call(new RGWRadosNotifyCR(store, obj, bl, config.notify_timeout_ms,
                                nullptr));
    }
    if (retcode < 0) {
      ldout(cct, 10) << "failed to notify peers of trim completion" << dendl;
      return set_cr_error(retcode);
    }

    ldpp_dout(dpp, 4) << "bucket index log processing completed in "
        << ceph::mono_clock::now() - start_time << dendl;
    return set_cr_done();
  }
  return 0;
}

class BucketTrimPollCR : public RGWCoroutine {
  rgw::sal::RadosStore* const store;
  RGWHTTPManager *const http;
  const BucketTrimConfig& config;
  BucketTrimObserver *const observer;
  const rgw_raw_obj& obj;
  const std::string name{"trim"}; //< lock name
  const std::string cookie;
  const DoutPrefixProvider *dpp;

 public:
  BucketTrimPollCR(rgw::sal::RadosStore* store, RGWHTTPManager *http,
                   const BucketTrimConfig& config,
                   BucketTrimObserver *observer, const rgw_raw_obj& obj,
                   const DoutPrefixProvider *dpp)
    : RGWCoroutine(store->ctx()), store(store), http(http),
      config(config), observer(observer), obj(obj),
      cookie(RGWSimpleRadosLockCR::gen_random_cookie(cct)),
      dpp(dpp) {}

  int operate(const DoutPrefixProvider *dpp) override;
};

int BucketTrimPollCR::operate(const DoutPrefixProvider *dpp)
{
  reenter(this) {
    for (;;) {
      set_status("sleeping");
      wait(utime_t{static_cast<time_t>(config.trim_interval_sec), 0});

      // prevent others from trimming for our entire wait interval
      set_status("acquiring trim lock");
      yield call(new RGWSimpleRadosLockCR(store->svc()->async_processor, store,
                                          obj, name, cookie,
                                          config.trim_interval_sec));
      if (retcode < 0) {
        ldout(cct, 4) << "failed to lock: " << cpp_strerror(retcode) << dendl;
        continue;
      }

      set_status("trimming");
      yield call(new BucketTrimCR(store, http, config, observer, obj, dpp));
      if (retcode < 0) {
        // on errors, unlock so other gateways can try
        set_status("unlocking");
        yield call(new RGWSimpleRadosUnlockCR(store->svc()->async_processor, store,
                                              obj, name, cookie));
      }
    }
  }
  return 0;
}

/// tracks a bounded list of events with timestamps. old events can be expired,
/// and recent events can be searched by key. expiration depends on events being
/// inserted in temporal order
template <typename T, typename Clock = ceph::coarse_mono_clock>
class RecentEventList {
 public:
  using clock_type = Clock;
  using time_point = typename clock_type::time_point;

  RecentEventList(size_t max_size, const ceph::timespan& max_duration)
    : events(max_size), max_duration(max_duration)
  {}

  /// insert an event at the given point in time. this time must be at least as
  /// recent as the last inserted event
  void insert(T&& value, const time_point& now) {
    // ceph_assert(events.empty() || now >= events.back().time)
    events.push_back(Event{std::move(value), now});
  }

  /// performs a linear search for an event matching the given key, whose type
  /// U can be any that provides operator==(U, T)
  template <typename U>
  bool lookup(const U& key) const {
    for (const auto& event : events) {
      if (key == event.value) {
        return true;
      }
    }
    return false;
  }

  /// remove events that are no longer recent compared to the given point in time
  void expire_old(const time_point& now) {
    const auto expired_before = now - max_duration;
    while (!events.empty() && events.front().time < expired_before) {
      events.pop_front();
    }
  }

 private:
  struct Event {
    T value;
    time_point time;
  };
  boost::circular_buffer<Event> events;
  const ceph::timespan max_duration;
};

namespace rgw {

// read bucket trim configuration from ceph context
void configure_bucket_trim(CephContext *cct, BucketTrimConfig& config)
{
  const auto& conf = cct->_conf;

  config.trim_interval_sec =
      conf.get_val<int64_t>("rgw_sync_log_trim_interval");
  config.counter_size = 512;
  config.buckets_per_interval =
      conf.get_val<int64_t>("rgw_sync_log_trim_max_buckets");
  config.min_cold_buckets_per_interval =
      conf.get_val<int64_t>("rgw_sync_log_trim_min_cold_buckets");
  config.concurrent_buckets =
      conf.get_val<int64_t>("rgw_sync_log_trim_concurrent_buckets");
  config.notify_timeout_ms = 10000;
  config.recent_size = 128;
  config.recent_duration = std::chrono::hours(2);
}

class BucketTrimManager::Impl : public TrimCounters::Server,
                                public BucketTrimObserver {
 public:
   rgw::sal::RadosStore* const store;
  const BucketTrimConfig config;

  const rgw_raw_obj status_obj;

  /// count frequency of bucket instance entries in the data changes log
  BucketChangeCounter counter;

  using RecentlyTrimmedBucketList = RecentEventList<std::string>;
  using clock_type = RecentlyTrimmedBucketList::clock_type;
  /// track recently trimmed buckets to focus trim activity elsewhere
  RecentlyTrimmedBucketList trimmed;

  /// serve the bucket trim watch/notify api
  BucketTrimWatcher watcher;

  /// protect data shared between data sync, trim, and watch/notify threads
  std::mutex mutex;

  Impl(rgw::sal::RadosStore* store, const BucketTrimConfig& config)
    : store(store), config(config),
      status_obj(store->svc()->zone->get_zone_params().log_pool, BucketTrimStatus::oid),
      counter(config.counter_size),
      trimmed(config.recent_size, config.recent_duration),
      watcher(store, status_obj, this)
  {}

  /// TrimCounters::Server interface for watch/notify api
  void get_bucket_counters(int count, TrimCounters::Vector& buckets) {
    buckets.reserve(count);
    std::lock_guard<std::mutex> lock(mutex);
    counter.get_highest(count, [&buckets] (const std::string& key, int count) {
                          buckets.emplace_back(key, count);
                        });
    ldout(store->ctx(), 20) << "get_bucket_counters: " << buckets << dendl;
  }

  void reset_bucket_counters() override {
    ldout(store->ctx(), 20) << "bucket trim completed" << dendl;
    std::lock_guard<std::mutex> lock(mutex);
    counter.clear();
    trimmed.expire_old(clock_type::now());
  }

  /// BucketTrimObserver interface to remember successfully-trimmed buckets
  void on_bucket_trimmed(std::string&& bucket_instance) override {
    ldout(store->ctx(), 20) << "trimmed bucket instance " << bucket_instance << dendl;
    std::lock_guard<std::mutex> lock(mutex);
    trimmed.insert(std::move(bucket_instance), clock_type::now());
  }

  bool trimmed_recently(const std::string_view& bucket_instance) override {
    std::lock_guard<std::mutex> lock(mutex);
    return trimmed.lookup(bucket_instance);
  }
};

BucketTrimManager::BucketTrimManager(rgw::sal::RadosStore* store,
                                     const BucketTrimConfig& config)
  : impl(new Impl(store, config))
{
}
BucketTrimManager::~BucketTrimManager() = default;

int BucketTrimManager::init()
{
  return impl->watcher.start(this);
}

void BucketTrimManager::on_bucket_changed(const std::string_view& bucket)
{
  std::lock_guard<std::mutex> lock(impl->mutex);
  // filter recently trimmed bucket instances out of bucket change counter
  if (impl->trimmed.lookup(bucket)) {
    return;
  }
  impl->counter.insert(std::string(bucket));
}

RGWCoroutine* BucketTrimManager::create_bucket_trim_cr(RGWHTTPManager *http)
{
  return new BucketTrimPollCR(impl->store, http, impl->config,
                              impl.get(), impl->status_obj, this);
}

RGWCoroutine* BucketTrimManager::create_admin_bucket_trim_cr(RGWHTTPManager *http)
{
  // return the trim coroutine without any polling
  return new BucketTrimCR(impl->store, http, impl->config,
                          impl.get(), impl->status_obj, this);
}

CephContext* BucketTrimManager::get_cct() const
{
  return impl->store->ctx();
}

unsigned BucketTrimManager::get_subsys() const
{
  return dout_subsys;
}

std::ostream& BucketTrimManager::gen_prefix(std::ostream& out) const
{
  return out << "rgw bucket trim manager: ";
}

} // namespace rgw

int bilog_trim(const DoutPrefixProvider* p, rgw::sal::RadosStore* store,
	       RGWBucketInfo& bucket_info, uint64_t gen, int shard_id,
	       std::string_view start_marker, std::string_view end_marker)
{
  auto& logs = bucket_info.layout.logs;
  auto log = std::find_if(logs.begin(), logs.end(), rgw::matches_gen(gen));
  if (log == logs.end()) {
    ldpp_dout(p, 5) << __PRETTY_FUNCTION__ << ":" << __LINE__
		    << "ERROR: no log layout with gen=" << gen << dendl;
    return -ENOENT;
  }

  auto log_layout = *log;

  auto r = store->svc()->bilog_rados->log_trim(p, bucket_info, log_layout, shard_id, start_marker, end_marker);
  if (r < 0) {
    ldpp_dout(p, 5) << __PRETTY_FUNCTION__ << ":" << __LINE__
		    << "ERROR: bilog_rados->log_trim returned r=" << r << dendl;
  }
  return r;
}
