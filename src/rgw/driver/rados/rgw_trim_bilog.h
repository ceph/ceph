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

#pragma once

#include <memory>
#include <string_view>

#include "include/common_fwd.h"
#include "include/encoding.h"
#include "common/ceph_time.h"
#include "common/dout.h"
#include "rgw_common.h"

class RGWCoroutine;
class RGWHTTPManager;

namespace rgw {

namespace sal {
  class RadosStore;
}

/// Interface to inform the trim process about which buckets are most active
struct BucketChangeObserver {
  virtual ~BucketChangeObserver() = default;

  virtual void on_bucket_changed(const std::string_view& bucket_instance) = 0;
};

/// Configuration for BucketTrimManager
struct BucketTrimConfig {
  /// time interval in seconds between bucket trim attempts
  uint32_t trim_interval_sec{0};
  /// maximum number of buckets to track with BucketChangeObserver
  size_t counter_size{0};
  /// maximum number of buckets to process each trim interval
  uint32_t buckets_per_interval{0};
  /// minimum number of buckets to choose from the global bucket instance list
  uint32_t min_cold_buckets_per_interval{0};
  /// maximum number of buckets to process in parallel
  uint32_t concurrent_buckets{0};
  /// timeout in ms for bucket trim notify replies
  uint64_t notify_timeout_ms{0};
  /// maximum number of recently trimmed buckets to remember (should be small
  /// enough for a linear search)
  size_t recent_size{0};
  /// maximum duration to consider a trim as 'recent' (should be some multiple
  /// of the trim interval, at least)
  ceph::timespan recent_duration{0};
};

/// fill out the BucketTrimConfig from the ceph context
void configure_bucket_trim(CephContext *cct, BucketTrimConfig& config);

/// Determines the buckets on which to focus trim activity, using two sources of
/// input: the frequency of entries read from the data changes log, and a global
/// listing of the bucket.instance metadata. This allows us to trim active
/// buckets quickly, while also ensuring that all buckets will eventually trim
class BucketTrimManager : public BucketChangeObserver, public DoutPrefixProvider {
  class Impl;
  std::unique_ptr<Impl> impl;
 public:
  BucketTrimManager(sal::RadosStore *store, const BucketTrimConfig& config);
  ~BucketTrimManager();

  int init();

  /// increment a counter for the given bucket instance
  void on_bucket_changed(const std::string_view& bucket_instance) override;

  /// create a coroutine to run the bucket trim process every trim interval
  RGWCoroutine* create_bucket_trim_cr(RGWHTTPManager *http);

  /// create a coroutine to trim buckets directly via radosgw-admin
  RGWCoroutine* create_admin_bucket_trim_cr(RGWHTTPManager *http);

  CephContext *get_cct() const override;
  unsigned get_subsys() const;
  std::ostream& gen_prefix(std::ostream& out) const;
};

/// provides persistent storage for the trim manager's current position in the
/// list of bucket instance metadata
struct BucketTrimStatus {
  std::string marker; //< metadata key of current bucket instance

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(marker, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& p) {
    DECODE_START(1, p);
    decode(marker, p);
    DECODE_FINISH(p);
  }

  static const std::string oid;
};

} // namespace rgw

WRITE_CLASS_ENCODER(rgw::BucketTrimStatus);

int bilog_trim(const DoutPrefixProvider* p, rgw::sal::RadosStore* store,
	       RGWBucketInfo& bucket_info, uint64_t gen, int shard_id,
	       std::string_view start_marker, std::string_view end_marker);
