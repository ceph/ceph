// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <list>
#include <map>
#include <utility>

#include "include/buffer.h"
#include "include/crc32c.h"

namespace ceph::osd::oob {

/// what take_pushed() found in a gather window
struct pushed_take_t {
  uint64_t bytes = 0;     ///< bytes copied out of the window
  uint32_t crc = 0;       ///< their crc32c, seeded with -1
  bool overrun = false;   ///< the extents run past the end of the window
  bool mismatch = false;  ///< the copy does not match the shard's crc32c
  bool ok() const { return !overrun && !mismatch; }
};

/// take_pushed() copies and checks an extent this much at a time
inline constexpr uint64_t pushed_slice = 256 << 10;

/**
 * Rebuild the buffers a shard pushed into a gather window instead of
 * returning them in its reply: its extents sit there back to back, in
 * reply order. They are copied out a slice at a time, and each slice is
 * checked in the copy, while it is still in cache: a write that lands in
 * the window meanwhile cannot change what was checked.
 *
 * The copy has to match the crc32c the shard sent of what it pushed.
 * Without the check, the window's memory would pass for the shard's data
 * whatever put it there: a push that completed short, a stray or late
 * write that landed after it, or a window lent to two gathers.
 *
 * On success the extents are appended to out; on failure out is left
 * as it was.
 *
 * Each extent is copied into make(len) when make returns a buffer of at
 * least len bytes, such as one of the OSD's arena that a transport then
 * writes from without another copy, and trimmed to len; into a new
 * buffer of the heap otherwise.
 */
template <typename Key>
pushed_take_t take_pushed(
  const char* win, uint64_t size,
  const std::map<Key, std::list<std::pair<uint64_t, uint64_t>>>& pushed,
  uint32_t want_crc,
  std::map<Key, std::list<std::pair<uint64_t, ceph::buffer::list>>>& out,
  const std::function<ceph::buffer::ptr(uint64_t)>& make = {})
{
  pushed_take_t t;
  t.crc = static_cast<uint32_t>(-1);
  std::map<Key, std::list<std::pair<uint64_t, ceph::buffer::list>>> got;
  for (const auto& [key, extents] : pushed) {
    for (const auto& [offset, len] : extents) {
      // past the window's end, or more than a bufferlist holds
      if (len > size - t.bytes || len > std::numeric_limits<unsigned>::max()) {
	t.overrun = true;
	return t;
      }
      ceph::buffer::list bl;
      if (len) {
	ceph::buffer::ptr bp;
	if (make) {
	  bp = make(len);
	}
	if (bp.length() < len) {
	  bp = ceph::buffer::create(len);
	} else if (bp.length() > len) {
	  // a buffer rounded up: the extent is len bytes
	  bp.set_length(len);
	}
	for (uint64_t o = 0; o < len; o += pushed_slice) {
	  const uint64_t n = std::min(pushed_slice, len - o);
	  char* c = bp.c_str() + o;
	  std::memcpy(c, win + t.bytes + o, n);
	  t.crc = ceph_crc32c(t.crc, reinterpret_cast<unsigned char*>(c), n);
	}
	bl.push_back(std::move(bp));
      }
      got[key].emplace_back(offset, std::move(bl));
      t.bytes += len;
    }
  }
  if (want_crc != t.crc) {
    t.mismatch = true;
    return t;
  }
  for (auto& [key, extents] : got) {
    auto& o = out[key];
    o.splice(o.end(), extents);
  }
  return t;
}

} // namespace ceph::osd::oob
