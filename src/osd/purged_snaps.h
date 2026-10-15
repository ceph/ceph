// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

/**
 * \file
 * \brief Store-independent helpers for the OSD's purged_snaps records.
 *
 * The OSD records the purged snaps of all pools as omap rows of the
 * 'purged_snaps' meta object, one row per contiguous interval:
 *   key:   "PSN__<pool>_<end - 1, as %016x>"
 *   value: encode(int64_t pool), encode(snapid_t begin), encode(snapid_t end)
 * (the interval is half-open: [begin, end)).
 *
 * The snap-mapper's mapping keys start with "SNA_<pool>_<snap, as %016X>_".
 *
 * Note that the pool is formatted in decimal. Thus keys of different pools
 * do not sort numerically (e.g. pool 20 sorts before pool 3). Keys of a
 * single pool do.
 *
 * Used by both the Classic and the Crimson OSD.
 */

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <iterator>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "include/buffer.h"
#include "include/ceph_assert.h"
#include "include/encoding.h"
#include "include/object.h"
#include "include/rados.h"
#include "osd/osd_types.h"

namespace ceph::osd::purged_snaps {

inline constexpr std::string_view row_prefix{"PSN_"};
inline constexpr std::string_view mapping_prefix{"SNA_"};

/// a half-open interval [begin, end) of purged snaps of one pool
struct interval_t {
  int64_t pool{0};
  snapid_t begin;
  snapid_t end;

  auto operator<=>(const interval_t&) const = default;

  [[nodiscard]] constexpr bool contains(snapid_t snap) const noexcept
  {
    return begin <= snap && snap < end;
  }
};

/// the purged snaps reported by the OSDMap (or by the monitor), per epoch
using by_epoch_t =
  std::map<epoch_t, mempool::osdmap::map<int64_t, snap_interval_set_t>>;

// --- purged_snaps rows

/// the key of the row of 'pool' whose interval ends at 'last' (inclusive)
[[nodiscard]] inline std::string make_key(int64_t pool, snapid_t last)
{
  ceph_assert(last != CEPH_NOSNAP && last != CEPH_SNAPDIR);
  return fmt::format(
    "{}_{}_{:016x}", row_prefix, pool, static_cast<uint64_t>(last));
}

[[nodiscard]] inline std::string make_key(const interval_t& iv)
{
  return make_key(iv.pool, iv.end - 1);
}

/// the common prefix of the keys of all rows of 'pool'
[[nodiscard]] inline std::string make_pool_prefix(int64_t pool)
{
  return fmt::format("{}_{}_", row_prefix, pool);
}

[[nodiscard]] inline ceph::buffer::list encode_value(const interval_t& iv)
{
  ceph::buffer::list bl;
  ceph::encode(iv.pool, bl);
  ceph::encode(iv.begin, bl);
  ceph::encode(iv.end, bl);
  return bl;
}

/**
 * decode one purged_snaps row
 *
 * \returns std::nullopt if the key is not a purged_snaps key, if the value
 * cannot be decoded, if the interval is empty or out of range, or if the
 * key does not match the decoded interval.
 */
[[nodiscard]] inline std::optional<interval_t> decode_row(
  std::string_view key,
  std::string_view value)
{
  if (!key.starts_with(row_prefix)) {
    return std::nullopt;
  }
  interval_t iv;
  try {
    ceph::buffer::list bl;
    bl.append(value);
    auto p = bl.cbegin();
    ceph::decode(iv.pool, p);
    ceph::decode(iv.begin, p);
    ceph::decode(iv.end, p);
  } catch (const ceph::buffer::error&) {
    return std::nullopt;
  }
  if (iv.begin >= iv.end || iv.end > CEPH_MAXSNAP + 1 || key != make_key(iv)) {
    return std::nullopt;
  }
  return iv;
}

// --- snap-mapper mapping keys

struct mapping_key_t {
  int64_t pool{0};
  snapid_t snap;

  auto operator<=>(const mapping_key_t&) const = default;
};

/// the prefix shared by all mapping keys of 'snap' in 'pool'
[[nodiscard]] inline std::string make_mapping_prefix(
  int64_t pool,
  snapid_t snap)
{
  // note: the snap is formatted as a 64-bit hex number, and not
  // according to the text representation of snapid_t
  ceph_assert(snap != CEPH_NOSNAP && snap != CEPH_SNAPDIR);
  return fmt::format(
    "{}{}_{:016X}_", mapping_prefix, pool, static_cast<uint64_t>(snap));
}

/// extract the pool and the snap from a mapping key
[[nodiscard]] inline std::optional<mapping_key_t> parse_mapping_key(
  std::string_view key)
{
  if (!key.starts_with(mapping_prefix)) {
    return std::nullopt;
  }
  key.remove_prefix(mapping_prefix.size());
  const auto* const key_end = key.data() + key.size();

  mapping_key_t parsed;
  const auto [pool_end, pool_ec] =
    std::from_chars(key.data(), key_end, parsed.pool);
  if (pool_ec != std::errc{} || pool_end == key_end || *pool_end != '_') {
    return std::nullopt;
  }

  constexpr std::size_t snap_digits = 16;
  const std::string_view snap_part{pool_end + 1, key_end};
  if (snap_part.size() <= snap_digits || snap_part[snap_digits] != '_') {
    return std::nullopt;
  }
  uint64_t snap{0};
  const auto* const digits_end = snap_part.data() + snap_digits;
  const auto [snap_end, snap_ec] =
    std::from_chars(snap_part.data(), digits_end, snap, 16);
  if (snap_ec != std::errc{} || snap_end != digits_end) {
    return std::nullopt;
  }
  parsed.snap = snap;
  return parsed;
}

// --- interval manipulation

/**
 * sort the intervals by (pool, begin), and merge the overlapping and the
 * adjacent intervals of each pool. Empty intervals are dropped.
 *
 * Within a pool, the resulting intervals are disjoint and non-adjacent,
 * and thus sorted by 'end' as well.
 */
[[nodiscard]] inline std::vector<interval_t> normalize(
  std::vector<interval_t> rows)
{
  std::erase_if(rows, [](const interval_t& iv) { return iv.begin >= iv.end; });
  std::ranges::sort(rows);

  std::vector<interval_t> merged;
  merged.reserve(rows.size());
  for (const auto& iv : rows) {
    if (!merged.empty() && merged.back().pool == iv.pool &&
        iv.begin <= merged.back().end) {
      merged.back().end = std::max(merged.back().end, iv.end);
    } else {
      merged.push_back(iv);
    }
  }
  return merged;
}

/// all the intervals of all the epochs, as reported. Not normalized.
[[nodiscard]] inline std::vector<interval_t> flatten(const by_epoch_t& purged)
{
  std::vector<interval_t> rows;
  for (const auto& [epoch, by_pool] : purged) {
    for (const auto& [pool, snaps] : by_pool) {
      for (const auto& [start, len] : snaps) {
        rows.push_back({pool, start, start + len});
      }
    }
  }
  return rows;
}

/// the omap changes that bring the rows on disk to their merged form
struct row_update_t {
  std::set<std::string> rm_keys;
  std::map<std::string, ceph::buffer::list> set_rows;

  [[nodiscard]] bool empty() const noexcept
  {
    return rm_keys.empty() && set_rows.empty();
  }
};

/**
 * compute the omap changes required to merge 'added' into the 'existing'
 * rows (the rows read from disk - possibly overlapping or adjacent).
 *
 * The rows after the update are the normalized union of both inputs: no
 * two rows of a pool overlap or touch. Rows that are already in their final
 * form are not rewritten, and no key is both removed and set.
 */
[[nodiscard]] inline row_update_t merge_rows(
  std::span<const interval_t> existing,
  std::span<const interval_t> added)
{
  std::vector<interval_t> all{existing.begin(), existing.end()};
  std::ranges::copy(added, std::back_inserter(all));
  const auto merged = normalize(std::move(all));

  std::vector<interval_t> sorted_existing{existing.begin(), existing.end()};
  std::ranges::sort(sorted_existing);

  row_update_t upd;
  for (const auto& iv : merged) {
    if (!std::ranges::binary_search(sorted_existing, iv)) {
      upd.set_rows.emplace(make_key(iv), encode_value(iv));
    }
  }

  // a row's key is derived from its (pool, end). 'merged' is sorted by
  // (pool, begin) and is disjoint within a pool - so it is sorted by
  // (pool, end) as well.
  const auto pool_and_end = [](const interval_t& iv) {
    return std::pair{iv.pool, static_cast<uint64_t>(iv.end)};
  };
  for (const auto& iv : existing) {
    if (!std::ranges::binary_search(
          merged, pool_and_end(iv), {}, pool_and_end)) {
      upd.rm_keys.insert(make_key(iv));
    }
  }
  return upd;
}

/**
 * a lookup table of purged snaps, built from rows in any order, that may
 * overlap or touch each other.
 */
class index_t {
 public:
  index_t() = default;
  explicit index_t(std::vector<interval_t> rows)
      : intervals{normalize(std::move(rows))}
  {}

  /// the purged interval containing 'snap' of 'pool', if any
  [[nodiscard]] std::optional<interval_t> find(
    int64_t pool,
    snapid_t snap) const
  {
    const auto rows = pool_view(pool);
    // the only candidate is the last interval that starts at or before 'snap'
    const auto after = std::ranges::upper_bound(
      rows, static_cast<uint64_t>(snap), {},
      [](const interval_t& iv) { return static_cast<uint64_t>(iv.begin); });
    if (after == rows.begin()) {
      return std::nullopt;
    }
    if (const auto& candidate = *std::prev(after); candidate.contains(snap)) {
      return candidate;
    }
    return std::nullopt;
  }

  /// the (sorted, disjoint) intervals of one pool
  [[nodiscard]] std::span<const interval_t> pool_view(int64_t pool) const
  {
    const auto [first, last] =
      std::ranges::equal_range(intervals, pool, {}, &interval_t::pool);
    return {first, last};
  }

  [[nodiscard]] std::size_t size() const noexcept { return intervals.size(); }
  [[nodiscard]] bool empty() const noexcept { return intervals.empty(); }

 private:
  /// sorted by (pool, begin); disjoint and non-adjacent within a pool
  std::vector<interval_t> intervals;
};

}  // namespace ceph::osd::purged_snaps
