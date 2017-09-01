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

#ifndef RGW_SYNC_LOG_TRIM_H
#define RGW_SYNC_LOG_TRIM_H

#include <memory>

class CephContext;
class RGWRados;

namespace rgw {

/// Configuration for BucketTrimManager
struct BucketTrimConfig {
};

/// fill out the BucketTrimConfig from the ceph context
void configure_bucket_trim(CephContext *cct, BucketTrimConfig& config);

/// Determines the buckets on which to focus trim activity, using two sources of
/// input: the frequency of entries read from the data changes log, and a global
/// listing of the bucket.instance metadata. This allows us to trim active
/// buckets quickly, while also ensuring that all buckets will eventually trim
class BucketTrimManager {
  class Impl;
  std::unique_ptr<Impl> impl;
 public:
  BucketTrimManager(RGWRados *store, const BucketTrimConfig& config);
  ~BucketTrimManager();
};

} // namespace rgw

#endif // RGW_SYNC_LOG_TRIM_H
