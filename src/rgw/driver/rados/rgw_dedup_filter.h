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
#include <vector>

namespace rgw::dedup {

  enum class filter_mode_t : uint8_t {
    FILTER_NONE,   // no filter active
    FILTER_ALLOW,  // allowlist: only listed items pass
    FILTER_DENY    // denylist:  listed items are blocked
  };

  struct dedup_filter_t {
    friend void encode(const dedup_filter_t& f, ceph::bufferlist& bl);
    friend void decode(dedup_filter_t& f, ceph::bufferlist::const_iterator& bl);

    // Default constructor: no filter (all buckets/storage classes pass)
    dedup_filter_t() = default;

    // Constructor from file paths. Empty string = no filter for that dimension.
    // allow and deny are mutually exclusive per dimension.
    // Check errcode() after construction to detect any error.
    dedup_filter_t(const std::string& allow_bucket_file,
                   const std::string& deny_bucket_file,
                   const std::string& allow_sc_file,
                   const std::string& deny_sc_file,
                   const DoutPrefixProvider* dpp);

    // Returns 0 on success, negative errno if construction failed.
    int errcode() const { return d_errcode; }

    // Returns true if any filter dimension is active.
    bool is_active() const {
      return (bucket_mode != filter_mode_t::FILTER_NONE ||
              storage_class_mode != filter_mode_t::FILTER_NONE);
    }

    // Returns true if the bucket should be processed
    bool allow_bucket(const std::string& bucket_name) const;
    // Returns true if the storage class should be processed
    bool allow_storage_class(const std::string& storage_class) const;

    const std::unordered_set<std::string>& get_bucket_filter() const {
      return bucket_set;
    }

    const std::vector<std::string>& get_storage_class_filter() const {
      return sc_vec;
    }

  private:
    // Read filter file: one name per line, '#' starts a comment, whitespace trimmed
    // On success returns 0 and populates name_set; on error returns negative errno.
    static int read_filter_file(const std::string& path,
                                std::unordered_set<std::string>& name_set,
                                int (*validator)(const std::string&),
                                const DoutPrefixProvider* dpp);

    filter_mode_t bucket_mode = filter_mode_t::FILTER_NONE;
    // we can have many buckets, use unordered_set for a quick search
    std::unordered_set<std::string> bucket_set;

    filter_mode_t storage_class_mode = filter_mode_t::FILTER_NONE;
    // there are only a few active storage_classes, a vector will suffice
    std::vector<std::string> sc_vec;

    int d_errcode = 0;
  };

  void encode(const dedup_filter_t& f, ceph::bufferlist& bl);
  void decode(dedup_filter_t& f, ceph::bufferlist::const_iterator& bl);

} // namespace rgw::dedup
