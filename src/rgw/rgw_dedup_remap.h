// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
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
#include <unordered_map>
#include <cstring>
#include <string>


namespace rgw::dedup {
  class remapper_t
  {
  public:
    static inline constexpr uint8_t NULL_IDX = 0xFF;
    remapper_t(uint32_t max_entries) : d_max_entries(max_entries) {}
    uint8_t remap(const std::string &key,
                  const DoutPrefixProvider* dpp,
                  uint64_t *p_overflow_count) { // IN-OUT
      uint8_t idx;

      auto itr = d_map.find(key);
      if (itr != d_map.end()) {
        idx = itr->second;
        ldpp_dout(dpp, 20) << __func__ << "::Existing key: " << key
                           << " is mapped to idx=" << (int)idx << dendl;
      }
      else if (d_num_entries < d_max_entries) {
        // assign it the next entry
        idx = d_num_entries++;
        d_map[key] = idx;
        ldpp_dout(dpp, 20) << __func__ << "::New key: " << key
                           << " was mapped to idx=" << (int)idx << dendl;
      }
      else {
        (*p_overflow_count) ++;
        ldpp_dout(dpp, 10) << __func__ << "::ERR: Failed adding key: "
                           << key << dendl;
        idx = NULL_IDX;
      }

      return idx;
    }

  private:
    uint32_t d_num_entries = 0;
    const uint32_t d_max_entries;
    std::unordered_map<std::string, uint8_t> d_map;
  };

} //namespace rgw::dedup
