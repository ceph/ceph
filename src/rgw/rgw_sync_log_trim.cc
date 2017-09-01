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
#include "rgw_rados.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "trim: ")

using rgw::BucketTrimConfig;
using BucketChangeCounter = BoundedKeyCounter<std::string, int>;


// watch/notify api for gateways to coordinate about which buckets to trim
enum TrimNotifyType {
};
WRITE_RAW_ENCODER(TrimNotifyType);

struct TrimNotifyHandler {
  virtual ~TrimNotifyHandler() = default;

  virtual void handle(bufferlist::iterator& input, bufferlist& output) = 0;
};

/// rados watcher for bucket trim notifications
class BucketTrimWatcher : public librados::WatchCtx2 {
  RGWRados *const store;
  const rgw_raw_obj& obj;
  rgw_rados_ref ref;
  uint64_t handle{0};

  using HandlerPtr = std::unique_ptr<TrimNotifyHandler>;
  boost::container::flat_map<TrimNotifyType, HandlerPtr> handlers;

 public:
  BucketTrimWatcher(RGWRados *store, const rgw_raw_obj& obj)
    : store(store), obj(obj)
  {
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


namespace rgw {

class BucketTrimManager::Impl {
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
      status_obj(store->get_zone_params().log_pool, "bilog.trim"),
      counter(config.counter_size),
      watcher(store, status_obj)
  {}
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

} // namespace rgw
