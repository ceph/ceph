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
#include <fstream>
#include <string>

#define dout_subsys ceph_subsys_rgw_dedup

namespace rgw::dedup {

  //---------------------------------------------------------------------------
  bool dedup_filter_t::allow_bucket(const std::string& bucket_name) const
  {
    switch (bucket_mode) {
    case filter_mode_t::FILTER_NONE:
      return true;
    case filter_mode_t::FILTER_ALLOW:
      return bucket_set.count(bucket_name) > 0;
    case filter_mode_t::FILTER_DENY:
      return bucket_set.count(bucket_name) == 0;
    }
    return true;
  }

  //---------------------------------------------------------------------------
  bool dedup_filter_t::allow_storage_class(const std::string& storage_class) const
  {
    switch (storage_class_mode) {
    case filter_mode_t::FILTER_NONE:
      return true;
    case filter_mode_t::FILTER_ALLOW:
      return storage_class_set.count(storage_class) > 0;
    case filter_mode_t::FILTER_DENY:
      return storage_class_set.count(storage_class) == 0;
    }
    return true;
  }

  //---------------------------------------------------------------------------
  // Read filter file: one name per line, '#' starts a comment, whitespace trimmed.
  int read_filter_file(const std::string& path,
                       std::unordered_set<std::string>& name_set /*OUT*/,
                       const DoutPrefixProvider* dpp)
  {
    std::ifstream f(path);
    if (!f.is_open()) {
      ldpp_dout(dpp, 1) << __func__ << ":: failed to open filter file: " << path << dendl;
      return -ENOENT;
    }

    std::string line;
    while (std::getline(f, line)) {
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
        name_set.insert(std::move(name));
      }
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  void encode(const dedup_filter_t& f, ceph::bufferlist& bl)
  {
    ENCODE_START(1, 1, bl);
    encode(static_cast<uint8_t>(f.bucket_mode), bl);
    encode(f.bucket_set, bl);
    encode(static_cast<uint8_t>(f.storage_class_mode), bl);
    encode(f.storage_class_set, bl);
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
    decode(f.storage_class_set, bl);
    DECODE_FINISH(bl);
  }

} // namespace rgw::dedup
