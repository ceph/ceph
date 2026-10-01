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

#include "l_keys.hpp"
#include "constants.hpp"
#include "error_codes.hpp"
#include "fdb_latency.hpp"
#include "keys.hpp"

#include <endian.h>
#include <cstring>
#include <thread>
#include <chrono>

namespace kvrgw {

// ---------------------------------------------------------------------------
// Pure Endianness / Codec
// ---------------------------------------------------------------------------
bool decode_numeric_counter(std::string_view bytes, uint64_t& out_val)
{
  if (bytes.size() != sizeof(uint64_t)) {
    return false;
  }
  uint64_t raw = 0;
  std::memcpy(&raw, bytes.data(), sizeof(raw));
  out_val = be64toh(raw);
  return true;
}

//--------------------------------------------------------------------------------
void encode_numeric_counter(uint64_t val, std::span<char, sizeof(uint64_t)> out_buf)
{
  const uint64_t raw = htobe64(val);
  std::memcpy(out_buf.data(), &raw, sizeof(raw));
}

// ---------------------------------------------------------------------------
// Standalone Numeric Counter Allocator
// ---------------------------------------------------------------------------
KvrgwErrorCode LKeyNumericCounter::allocate( KvStore& store,
					     std::string_view counter_name,
					     uint64_t& out_id)
{
  constexpr int kMaxRetries = 3;
  const auto key = make_l_key(kLocalTypeNumeric, counter_name);

  for (int attempt = 0; attempt < kMaxRetries; ++attempt) {
    auto tr_res = store.begin_transaction();
    if (!tr_res) {
      auto ec = fdb_to_error(tr_res.error());
      if (is_retriable(ec)) {
	sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }

    auto& tr = *tr_res;
    auto val_res = tr->kv_get(key.view());
    if (!val_res) {
      auto ec = fdb_to_error(val_res.error());
      if (is_retriable(ec)) {
	sleep_for_msec(10 * (attempt + 1));
        continue;
      }
      return ec;
    }

    uint64_t cur = 0;
    if (val_res->has_value()) {
      if (!decode_numeric_counter(**val_res, cur)) {
        return KVRGW_ERR_CORRUPT_VALUE;
      }
    }

    const uint64_t next = cur + 1;
    char buf[sizeof(uint64_t)];
    encode_numeric_counter(next, std::span<char, sizeof(uint64_t)>(buf, sizeof(buf)));
    tr->kv_put(key.view(), std::string_view(buf, sizeof(buf)));

    auto rc = tr->commit();
    if (rc) {
      out_id = next;
      return KVRGW_ERR_OK;
    }

    auto ec = fdb_to_error(rc.error());
    if (!is_retriable(ec)) {
      return ec;
    }
    sleep_for_msec(10 * (attempt + 1));
  }

  return KVRGW_ERR_MAX_RETRIES_EXCEEDED;
}

} // namespace kvrgw
