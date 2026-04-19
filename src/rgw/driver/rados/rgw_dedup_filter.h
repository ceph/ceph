// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 sts=2 expandtab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once
#include "common/dout.h"
#include "include/rados/buffer.h"
#include "include/encoding.h"
#include <string>
#include <unordered_set>

namespace rgw::dedup {

  enum class filter_mode_t : uint8_t {
    FILTER_NONE,   // no filter active
    FILTER_ALLOW,  // allowlist: only listed items pass
    FILTER_DENY    // denylist:  listed items are blocked
  };

  struct dedup_filter_t {
    // Bucket filter
    filter_mode_t bucket_mode = filter_mode_t::FILTER_NONE;
    std::unordered_set<std::string> bucket_set;

    // Storage-class filter
    filter_mode_t storage_class_mode = filter_mode_t::FILTER_NONE;
    std::unordered_set<std::string> storage_class_set;

    // Returns true if the bucket should be processed
    bool allow_bucket(const std::string& bucket_name) const;
    // Returns true if the storage class should be processed
    bool allow_storage_class(const std::string& storage_class) const;
  };

  // Read a filter file: one name per line, '#' starts a comment, whitespace trimmed.
  // On success returns 0 and populates name_set; on error returns a negative errno.
  int read_filter_file(const std::string& path,
                       std::unordered_set<std::string>& name_set /*OUT*/,
                       const DoutPrefixProvider* dpp);

  void encode(const dedup_filter_t& f, ceph::bufferlist& bl);
  void decode(dedup_filter_t& f, ceph::bufferlist::const_iterator& bl);

} // namespace rgw::dedup
