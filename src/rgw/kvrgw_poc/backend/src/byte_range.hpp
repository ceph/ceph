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

#include <cstdint>
#include <optional>

namespace kvrgw {

struct ByteRange {
  int64_t start = 0;
  int64_t end = 0;
  bool from_end = false;
  bool end_unbounded = false;
};

struct ByteRangeSlice {
  int64_t start = 0;
  int64_t length = 0;
};

// Returns nullopt when range is absent (full object).
// Returns empty optional and sets invalid=true when range is unsatisfiable.
std::optional<ByteRangeSlice> resolve_byte_range(
    const ByteRange& range, int64_t object_size, bool* invalid);

}  // namespace kvrgw
