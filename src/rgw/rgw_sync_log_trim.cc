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

#include "common/bounded_key_counter.h"
#include "rgw_sync_log_trim.h"

#define dout_subsys ceph_subsys_rgw

#undef dout_prefix
#define dout_prefix (*_dout << "trim: ")

using rgw::BucketTrimConfig;
using BucketChangeCounter = BoundedKeyCounter<std::string, int>;

namespace rgw {

class BucketTrimManager::Impl {
 public:
  RGWRados *const store;
  const BucketTrimConfig config;

  /// count frequency of bucket instance entries in the data changes log
  BucketChangeCounter counter;

  /// protect data shared between data sync and trim threads
  std::mutex mutex;

  Impl(RGWRados *store, const BucketTrimConfig& config)
    : store(store), config(config),
      counter(config.counter_size)
  {}
};

BucketTrimManager::BucketTrimManager(RGWRados *store,
                                     const BucketTrimConfig& config)
  : impl(new Impl(store, config))
{
}
BucketTrimManager::~BucketTrimManager() = default;

void BucketTrimManager::on_bucket_changed(const boost::string_view& bucket)
{
  std::lock_guard<std::mutex> lock(impl->mutex);
  impl->counter.insert(bucket.to_string());
}

} // namespace rgw
