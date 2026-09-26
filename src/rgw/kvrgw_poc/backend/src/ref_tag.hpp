// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "constants.hpp"

#include <array>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>

namespace kvrgw {

using RefTag = std::array<uint8_t, kRefTagSize>;

inline std::string_view ref_tag_view(const RefTag& rt) {
  return {reinterpret_cast<const char*>(rt.data()), rt.size()};
}

class RefTagGenerator {
 public:
  explicit RefTagGenerator(uint32_t rgw_id);

  RefTag next();

  static std::string to_hex(std::string_view ref_tag);
  static std::string from_hex(std::string_view ref_tag_hex);
  static std::string filename_for(std::string_view ref_tag);
  static bool equal(std::string_view a, std::string_view b);

 private:
  uint32_t rgw_id_;
  uint64_t seq_id_{0};
  std::mutex mu_;
};

}  // namespace kvrgw
