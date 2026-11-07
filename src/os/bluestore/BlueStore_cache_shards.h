// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_OS_BLUESTORE_CACHE_SHARDS_H
#define CEPH_OS_BLUESTORE_CACHE_SHARDS_H

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

/// The data cache budget split among the buffer cache shards, and what the
/// data cache asks the memory autotuner for; pure functions for unit tests.
namespace bluestore_cache_shards {

/// A shard keeps 1/FLOOR_DIV of an even split whatever it holds, so a shard
/// that receives its first PG can take data before the next resize.
constexpr uint64_t FLOOR_DIV = 8;

/// Split `alloc` among the shards: the floor each, the rest in proportion to
/// the bytes held. The limits sum to at most alloc; with nothing held the
/// split is even, and full shards at the even split stay there.
inline std::vector<uint64_t> weighted_limits(uint64_t alloc,
					     const std::vector<uint64_t>& held)
{
  size_t shards = held.size();
  std::vector<uint64_t> limits(shards, 0);
  if (shards == 0) {
    return limits;
  }
  uint64_t even = alloc / shards;
  uint64_t min_limit = even / FLOOR_DIV;
  uint64_t rest = alloc - min_limit * shards;
  uint64_t total_held = 0;
  for (auto h : held) {
    total_held += h;
  }
  for (size_t s = 0; s < shards; ++s) {
    if (total_held == 0) {
      limits[s] = even;
    } else {
      // double: rest * held overflows uint64 past 4 GiB each
      limits[s] = min_limit + static_cast<uint64_t>(
	static_cast<double>(rest) * held[s] / total_held);
    }
  }
  return limits;
}

/// The fraction of the bytes held that an even split of `alloc` would have
/// held: a shard over its even share counts as full, one under it as is. The
/// data cache asks for this fraction of its bins by age and for the rest at
/// the lowest priority.
inline double even_split_fraction(uint64_t alloc,
				  const std::vector<uint64_t>& held)
{
  size_t shards = held.size();
  if (shards == 0 || alloc == 0) {
    return 1.0;
  }
  uint64_t even = alloc / shards;
  uint64_t total = 0;
  uint64_t capped = 0;
  for (auto h : held) {
    total += h;
    capped += std::min(h, even);
  }
  return total ? static_cast<double>(capped) / total : 1.0;
}

} // namespace bluestore_cache_shards

#endif
