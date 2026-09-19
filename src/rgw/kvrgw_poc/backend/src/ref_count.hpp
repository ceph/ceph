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

#include "object_value.hpp"

#include <cstdint>
#include <cstring>
#include <endian.h>
#include <limits>
#include <string>
#include <string_view>

namespace kvrgw {

// --- R: value (uint64_be ref_count + chunk_descriptor) ---

struct RValue {
  uint64_t ref_count{};
  std::string chunk_descriptor;
};

inline RValue parse_r_value(std::string_view raw) {
  RValue r;
  if (raw.size() < 8) return r;
  uint64_t be{};
  std::memcpy(&be, raw.data(), 8);
  r.ref_count = be64toh(be);
  if (raw.size() > 8) {
    r.chunk_descriptor.assign(raw.data() + 8, raw.size() - 8);
  }
  return r;
}

inline std::string write_r_value(uint64_t count, std::string_view chunk_descriptor) {
  const uint64_t be = htobe64(count);
  std::string out(reinterpret_cast<const char*>(&be), 8);
  out.append(chunk_descriptor);
  return out;
}

struct DRefInfo {
  bool shared{false};
  uint64_t ref_count{};
};

inline DRefInfo read_d_ref_count(std::string_view d_value) {
  DRefInfo info;
  ChildValueHeader hdr{};
  if (!parse_child_value_header(d_value, hdr)) {
    return info;
  }
  info.shared = child_flag_shared(hdr.flags);
  info.ref_count = hdr.ref_count;
  return info;
}

inline std::string write_d_with_ref(std::string_view data, uint64_t new_count) {
  ChildValueHeader hdr{};
  if (new_count > 1) {
    hdr.flags = CHILD_FLAG_SHARED;
    hdr.ref_count = new_count > std::numeric_limits<uint32_t>::max()
        ? std::numeric_limits<uint32_t>::max()
        : static_cast<uint32_t>(new_count);
  }
  return make_child_value(hdr, data);
}

inline std::string_view d_data_portion(std::string_view d_value, uint64_t data_size) {
  return child_value_payload(d_value, data_size);
}

}  // namespace kvrgw
