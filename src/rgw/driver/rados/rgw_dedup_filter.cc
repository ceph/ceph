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

#include "rgw_dedup_filter.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/encoding.h"
#include "rgw_rest_s3.h"
#include <cctype>
#include <fstream>
#include <iostream>
#include <string>
#include <algorithm>

#define dout_subsys ceph_subsys_rgw_dedup

namespace rgw::dedup {

  //---------------------------------------------------------------------------
  bool dedup_filter_t::allow_bucket(const std::string& bucket_name) const
  {
    switch (bucket_mode) {
    case filter_mode_t::FILTER_NONE:
      return true;
    case filter_mode_t::FILTER_ALLOW:
      return bucket_set.contains(bucket_name);
    case filter_mode_t::FILTER_DENY:
      return !bucket_set.contains(bucket_name);
    default:
      return true;
    }
  }

  //---------------------------------------------------------------------------
  bool dedup_filter_t::allow_storage_class(const std::string& storage_class) const
  {
    if (storage_class_mode == filter_mode_t::FILTER_NONE) {
      return true;
    }

    auto it = std::find(sc_vec.begin(), sc_vec.end(), storage_class);
    if (storage_class_mode == filter_mode_t::FILTER_ALLOW) {
      return it != sc_vec.end();
    }
    else {
      return it == sc_vec.end();
    }
  }

  //---------------------------------------------------------------------------
  static inline int valid_bucket_name(const std::string& name)
  {
    // per Casey instructions - nothing should be filtered here
    return 0;

    // return -(valid_s3_bucket_name(name, true /* relaxed */));
  }

  //---------------------------------------------------------------------------
  static int valid_storage_class_name(const std::string& name)
  {
    // per Casey instructions - nothing should be filtered here
    return 0;
  }

  //---------------------------------------------------------------------------
  // Private static helper: read filter file, one name per line.
  int dedup_filter_t::read_filter_file(const std::string& path,
                                       std::unordered_set<std::string>& name_set,
                                       int (*validator)(const std::string&),
                                       const DoutPrefixProvider* dpp)
  {
    std::ifstream f(path);
    if (!f.is_open()) {
      ldpp_dout(dpp, 1) << __func__ << "::Failed to open filter file: " << path << dendl;
      return -ENOENT;
    }

    std::string line;
    int line_num = 0;
    while (std::getline(f, line)) {
      line_num++;

      // Strip comment (everything from '#' onward)
      auto comment_pos = line.find('#');
      if (comment_pos != std::string::npos) {
        line = line.substr(0, comment_pos);
      }

      // Trim leading and trailing whitespace
      const char* ws = " \t\r\n";
      auto start = line.find_first_not_of(ws);
      if (start == std::string::npos) {
        continue; // blank line after stripping
      }

      auto end = line.find_last_not_of(ws);
      std::string name = line.substr(start, end - start + 1);
      if (!name.empty()) {
        int ret = validator(name);
        if (ret == 0) {
          ldpp_dout(dpp, 20) << __func__ << "::" << name << dendl;
          name_set.insert(std::move(name));
        }
        else {
          ldpp_dout(dpp, 1) << __func__ << "::" << path << "::" << line_num
                            << "::invalid name '" << name << "'" << dendl;
          return -EINVAL;
        }
      }
    }

    if (name_set.empty()) {
      // we should have at least one valid filter
      ldpp_dout(dpp, 1) << __func__ << "::No valid filter in file: " << path << dendl;
      return -ENODATA;
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  dedup_filter_t::dedup_filter_t(const std::string& allow_bucket_file,
                                 const std::string& deny_bucket_file,
                                 const std::string& allow_sc_file,
                                 const std::string& deny_sc_file,
                                 const DoutPrefixProvider* dpp)
  {
    // Validate mutual exclusivity
    if (!allow_bucket_file.empty() && !deny_bucket_file.empty()) {
      ldpp_dout(dpp, 1) << __func__
                        << ":: --allow-bucket-list and --deny-bucket-list are mutually exclusive"
                        << dendl;
      d_errcode = -EINVAL;
      return;
    }
    if (!allow_sc_file.empty() && !deny_sc_file.empty()) {
      ldpp_dout(dpp, 1) << __func__
                        << ":: --allow-storage-class-list and --deny-storage-class-list are mutually exclusive"
                        << dendl;
      d_errcode = -EINVAL;
      return;
    }

    // Bucket filter
    if (!allow_bucket_file.empty()) {
      d_errcode = read_filter_file(allow_bucket_file, bucket_set,
                                   valid_bucket_name, dpp);
      if (d_errcode != 0) {
        return;
      }
      bucket_mode = filter_mode_t::FILTER_ALLOW;
    }
    else if (!deny_bucket_file.empty()) {
      d_errcode = read_filter_file(deny_bucket_file, bucket_set,
                                   valid_bucket_name, dpp);
      if (d_errcode != 0) {
        return;
      }
      bucket_mode = filter_mode_t::FILTER_DENY;
    }

    // Storage-class filter
    // First, read the filtered storage-class names into a set to prevent duplicates
    //       and then move them into a vector for better efficiency.
    std::unordered_set<std::string> name_set;
    if (!allow_sc_file.empty()) {
      d_errcode = read_filter_file(allow_sc_file, name_set,
                                   valid_storage_class_name, dpp);
      if (d_errcode != 0) {
        return;
      }
      storage_class_mode = filter_mode_t::FILTER_ALLOW;
    }
    else if (!deny_sc_file.empty()) {
      d_errcode = read_filter_file(deny_sc_file, name_set,
                                   valid_storage_class_name, dpp);
      if (d_errcode != 0) {
        return;
      }
      storage_class_mode = filter_mode_t::FILTER_DENY;
    }
    // move elements from set to vector
    sc_vec.assign(name_set.begin(), name_set.end());
  }

  //---------------------------------------------------------------------------
  void encode(const dedup_filter_t& f, ceph::bufferlist& bl)
  {
    ENCODE_START(1, 1, bl);
    encode(static_cast<uint8_t>(f.bucket_mode), bl);
    encode(f.bucket_set, bl);
    encode(static_cast<uint8_t>(f.storage_class_mode), bl);
    encode(f.sc_vec, bl);
    ENCODE_FINISH(bl);
  }

  //---------------------------------------------------------------------------
  void decode(dedup_filter_t& f, ceph::bufferlist::const_iterator& bl)
  {
    DECODE_START(1, bl);
    uint8_t mode;
    decode(mode, bl);
    f.bucket_mode = static_cast<filter_mode_t>(mode);
    decode(f.bucket_set, bl);
    decode(mode, bl);
    f.storage_class_mode = static_cast<filter_mode_t>(mode);
    decode(f.sc_vec, bl);
    DECODE_FINISH(bl);
  }

} // namespace rgw::dedup
