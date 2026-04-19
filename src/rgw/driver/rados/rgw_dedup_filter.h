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

#include <set>
#include <string>

#include "include/encoding.h"

namespace rgw::dedup {

  enum class filter_mode_t : uint8_t {
    FILTER_NONE  = 0,
    FILTER_ALLOW = 1,
    FILTER_DENY  = 2
  };

  inline std::ostream& operator<<(std::ostream &out, filter_mode_t mode) {
    switch (mode) {
      case filter_mode_t::FILTER_NONE:  out << "NONE";  break;
      case filter_mode_t::FILTER_ALLOW: out << "ALLOW"; break;
      case filter_mode_t::FILTER_DENY:  out << "DENY";  break;
    }
    return out;
  }

  //---------------------------------------------------------------------------
  struct dedup_filter_t {
    filter_mode_t bucket_mode = filter_mode_t::FILTER_NONE;
    filter_mode_t storage_class_mode = filter_mode_t::FILTER_NONE;
    std::set<std::string> bucket_list;
    std::set<std::string> storage_class_list;

    // default ctor -- no filtering (used by decode and by workers)
    dedup_filter_t() = default;

    // construct from CLI file paths; sets @init_result to 0 on success
    dedup_filter_t(const std::string& allow_bucket_file,
                   const std::string& deny_bucket_file,
                   const std::string& allow_sc_file,
                   const std::string& deny_sc_file,
                   int& init_result);

    bool bucket_allowed(const std::string& bucket_name) const {
      switch (bucket_mode) {
        case filter_mode_t::FILTER_ALLOW:
          return bucket_list.count(bucket_name) > 0;
        case filter_mode_t::FILTER_DENY:
          return bucket_list.count(bucket_name) == 0;
        default:
          return true;
      }
    }

    bool storage_class_allowed(const std::string& sc) const {
      switch (storage_class_mode) {
        case filter_mode_t::FILTER_ALLOW:
          return storage_class_list.count(sc) > 0;
        case filter_mode_t::FILTER_DENY:
          return storage_class_list.count(sc) == 0;
        default:
          return true;
      }
    }
  };

  //---------------------------------------------------------------------------
  inline void encode(const dedup_filter_t& f, ceph::bufferlist& bl)
  {
    ENCODE_START(1, 1, bl);
    encode(static_cast<uint8_t>(f.bucket_mode), bl);
    encode(static_cast<uint8_t>(f.storage_class_mode), bl);
    encode(f.bucket_list, bl);
    encode(f.storage_class_list, bl);
    ENCODE_FINISH(bl);
  }

  //---------------------------------------------------------------------------
  inline void decode(dedup_filter_t& f, ceph::bufferlist::const_iterator& bl)
  {
    DECODE_START(1, bl);
    uint8_t bm, sm;
    decode(bm, bl);
    decode(sm, bl);
    f.bucket_mode = static_cast<filter_mode_t>(bm);
    f.storage_class_mode = static_cast<filter_mode_t>(sm);
    decode(f.bucket_list, bl);
    decode(f.storage_class_list, bl);
    DECODE_FINISH(bl);
  }

} //namespace rgw::dedup
