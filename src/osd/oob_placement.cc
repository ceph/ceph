// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "osd/oob_placement.h"

#include <algorithm>

namespace ceph::osd::oob {

placement_plan linear_plan(uint64_t base_offset, uint64_t data_len)
{
  placement_plan plan;
  if (data_len) {
    plan.push_back({0, base_offset, data_len});
  }
  return plan;
}

placement_plan ec_direct_plan(uint64_t base_offset,
			      uint64_t ro_off, uint64_t ro_len,
			      uint64_t chunk_size, uint32_t k,
			      uint32_t raw_shard, uint64_t data_len)
{
  placement_plan plan;
  if (ro_len == 0 || chunk_size == 0 || k == 0 || data_len == 0) {
    return plan;
  }
  const uint64_t c0 = ro_off / chunk_size;
  const uint64_t cN = (ro_off + ro_len - 1) / chunk_size;
  uint64_t reply_ofs = 0;
  // first chunk this shard owns at or after c0
  uint64_t c = c0 + (raw_shard + k - (c0 % k)) % k;
  for (; c <= cN; c += k) {
    const uint64_t chunk_ro_start = std::max(c * chunk_size, ro_off);
    const uint64_t chunk_ro_end = std::min((c + 1) * chunk_size,
					   ro_off + ro_len);
    uint64_t len = chunk_ro_end - chunk_ro_start;
    if (reply_ofs >= data_len) {
      break;  // short shard read: nothing left to place
    }
    len = std::min(len, data_len - reply_ofs);
    plan.push_back({reply_ofs, base_offset + (chunk_ro_start - ro_off), len});
    reply_ofs += len;
  }
  return plan;
}

placement_plan sparse_plan(uint64_t base_offset, uint64_t ro_off,
			   const std::map<uint64_t, uint64_t>& extents,
			   uint64_t data_len)
{
  placement_plan plan;
  uint64_t reply_ofs = 0;
  for (const auto& [off, len] : extents) {
    if (len == 0) {
      continue;
    }
    if (off < ro_off || reply_ofs >= data_len) {
      // malformed map or short data blob: place what we can account for
      break;
    }
    const uint64_t l = std::min(len, data_len - reply_ofs);
    plan.push_back({reply_ofs, base_offset + (off - ro_off), l});
    reply_ofs += l;
    if (l < len) {
      break;
    }
  }
  return plan;
}

} // namespace ceph::osd::oob
