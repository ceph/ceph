// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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

#include "error_codes.hpp"
#include "kv_store.hpp"
#include "keys.hpp"

#include <cstdint>
#include <span>
#include <string_view>

namespace kvrgw {

// ---------------------------------------------------------------------------
// Pure Endianness / Codec for 64-bit Big-Endian Numeric Counter
// ---------------------------------------------------------------------------
bool decode_numeric_counter(std::string_view bytes, uint64_t& out_val);
void encode_numeric_counter(uint64_t val, std::span<char, sizeof(uint64_t)> out_buf);

// ---------------------------------------------------------------------------
// Standalone Numeric Counter Allocator
// Runs a dedicated transaction loop with retries to atomically increment and
// return the next 64-bit counter value for counter_name.
// ---------------------------------------------------------------------------
class LKeyNumericCounter {
 public:
  static KvrgwErrorCode allocate(KvStore& store,
				 std::string_view counter_name,
				 uint64_t& out_id);
};

} // namespace kvrgw
