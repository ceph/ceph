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

#include "byte_range.hpp"

namespace kvrgw {

std::optional<ByteRangeSlice>
resolve_byte_range(const ByteRange &range, int64_t object_size, bool *invalid)
{
  if (invalid != nullptr) {
    *invalid = false;
  }

  int64_t start = 0;
  int64_t length = 0;

  if (range.from_end) {
    const int64_t suffix = range.end;
    start = object_size - suffix;
    length = object_size - start;
  }
  else {
    start = range.start;
    if (range.end_unbounded) {
      length = object_size - start;
    }
    else {
      length = range.end - start + 1;
    }
  }

  if (start < 0 || length < 0 || start >= object_size) {
    if (invalid != nullptr) {
      *invalid = true;
    }
    return std::nullopt;
  }

  if (start + length > object_size) {
    length = object_size - start;
  }

  return ByteRangeSlice{start, length};
}

} // namespace kvrgw
