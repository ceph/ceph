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
#include "rgw_rest_s3.h"

#include <cctype>
#include <fstream>
#include <iostream>

namespace rgw::dedup {

  //---------------------------------------------------------------------------
  static int valid_bucket_name(const std::string& name)
  {
    return valid_s3_bucket_name(name, true /* relaxed */);
  }

  //---------------------------------------------------------------------------
  static int valid_storage_class_name(const std::string& name)
  {
    if (name.empty() || name.size() > 128) {
      return -EINVAL;
    }
    for (char c : name) {
      if (!(std::isupper(c) || std::isdigit(c) || c == '_')) {
        return -EINVAL;
      }
    }
    return 0;
  }

  //---------------------------------------------------------------------------
  // Parse a file containing one name per line.  Trailing whitespace after the
  // name is allowed; any other content on the line is rejected.
  // @validator is called on each trimmed name; it must return 0 on success.
  static int parse_name_list_file(
      const std::string& path,
      int (*validator)(const std::string&),
      std::set<std::string>& out)
  {
    std::ifstream f(path);
    if (!f.is_open()) {
      std::cerr << "ERROR: failed to open file: " << path << std::endl;
      return -ENOENT;
    }

    std::string line;
    int line_num = 0;
    while (std::getline(f, line)) {
      line_num++;

      if (line.empty() || line[0] == '#') {
        continue;
      }

      auto name_end = line.find_first_of(" \t\r");
      std::string name = (name_end == std::string::npos)
                         ? line : line.substr(0, name_end);

      if (name_end != std::string::npos) {
        auto non_ws = line.find_first_not_of(" \t\r", name_end);
        if (non_ws != std::string::npos) {
          std::cerr << "ERROR: " << path << ":" << line_num
                    << ": unexpected content after name '" << name << "'"
                    << std::endl;
          return -EINVAL;
        }
      }

      if (name.empty()) {
        continue;
      }

      int ret = validator(name);
      if (ret != 0) {
        std::cerr << "ERROR: " << path << ":" << line_num
                  << ": invalid name '" << name << "'" << std::endl;
        return ret;
      }

      out.insert(std::move(name));
    }

    if (out.empty()) {
      std::cerr << "ERROR: " << path << ": file contains no valid entries"
                << std::endl;
      return -EINVAL;
    }

    return 0;
  }

  //---------------------------------------------------------------------------
  dedup_filter_t::dedup_filter_t(const std::string& allow_bucket_file,
                                 const std::string& deny_bucket_file,
                                 const std::string& allow_sc_file,
                                 const std::string& deny_sc_file,
                                 int& init_result)
  {
    init_result = 0;

    if (!allow_bucket_file.empty() && !deny_bucket_file.empty()) {
      std::cerr << "ERROR: --allow-bucket-list and --deny-bucket-list"
                   " are mutually exclusive" << std::endl;
      init_result = -EINVAL;
      return;
    }
    if (!allow_sc_file.empty() && !deny_sc_file.empty()) {
      std::cerr << "ERROR: --allow-storage-class-list and"
                   " --deny-storage-class-list are mutually exclusive"
                << std::endl;
      init_result = -EINVAL;
      return;
    }

    if (!allow_bucket_file.empty()) {
      init_result = parse_name_list_file(allow_bucket_file,
                                         valid_bucket_name, bucket_list);
      if (init_result != 0) return;
      bucket_mode = filter_mode_t::FILTER_ALLOW;
    } else if (!deny_bucket_file.empty()) {
      init_result = parse_name_list_file(deny_bucket_file,
                                         valid_bucket_name, bucket_list);
      if (init_result != 0) return;
      bucket_mode = filter_mode_t::FILTER_DENY;
    }

    if (!allow_sc_file.empty()) {
      init_result = parse_name_list_file(allow_sc_file,
                                         valid_storage_class_name,
                                         storage_class_list);
      if (init_result != 0) return;
      storage_class_mode = filter_mode_t::FILTER_ALLOW;
    } else if (!deny_sc_file.empty()) {
      init_result = parse_name_list_file(deny_sc_file,
                                         valid_storage_class_name,
                                         storage_class_list);
      if (init_result != 0) return;
      storage_class_mode = filter_mode_t::FILTER_DENY;
    }
  }

} //namespace rgw::dedup
