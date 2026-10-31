// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <algorithm>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "osd/purged_snaps.h"

using namespace ceph::osd::purged_snaps;

namespace {

constexpr uint64_t max_snap = CEPH_MAXSNAP;

interval_t iv(int64_t pool, uint64_t begin, uint64_t end)
{
  return {pool, snapid_t{begin}, snapid_t{end}};
}

/// apply a row_update_t to an in-memory copy of the purged_snaps omap
std::map<std::string, ceph::buffer::list> apply_update(
  std::map<std::string, ceph::buffer::list> omap,
  const row_update_t& upd)
{
  for (const auto& k : upd.rm_keys) {
    omap.erase(k);
  }
  for (const auto& [k, v] : upd.set_rows) {
    omap.insert_or_assign(k, v);
  }
  return omap;
}

std::map<std::string, ceph::buffer::list> to_omap(
  const std::vector<interval_t>& rows)
{
  std::map<std::string, ceph::buffer::list> omap;
  for (const auto& r : rows) {
    omap.emplace(make_key(r), encode_value(r));
  }
  return omap;
}

/// decode an omap back into rows (in key order)
std::vector<interval_t> from_omap(
  const std::map<std::string, ceph::buffer::list>& omap)
{
  std::vector<interval_t> rows;
  for (const auto& [k, v] : omap) {
    auto r = decode_row(k, v.to_str());
    EXPECT_TRUE(r) << "undecodable row " << k;
    if (r) {
      rows.push_back(*r);
    }
  }
  return rows;
}

}  // namespace

// --- key and value formats

TEST(PurgedSnapsKeys, RowKeyFormat)
{
  // must match the keys written by existing OSDs
  EXPECT_EQ("PSN__20_000000000000001e", make_key(20, snapid_t{30}));
  EXPECT_EQ(
    "PSN__9223372036854775787_ffffffffffffffec",
    make_key(
      std::numeric_limits<int64_t>::max() - 20,
      snapid_t{std::numeric_limits<uint64_t>::max() - 19}));
  EXPECT_EQ("PSN__1_0000000000000013", make_key(iv(1, 10, 20)));
  EXPECT_EQ("PSN__3_", make_pool_prefix(3));
  EXPECT_TRUE(make_key(iv(3, 1, 2)).starts_with(make_pool_prefix(3)));
  EXPECT_FALSE(make_key(iv(30, 1, 2)).starts_with(make_pool_prefix(3)));
}

TEST(PurgedSnapsKeys, RowRoundTrip)
{
  for (const auto& r :
       {iv(1, 10, 20), iv(0, 0, 1), iv(9999999999, 1, 3),
        iv(std::numeric_limits<int64_t>::max(), max_snap - 12, max_snap + 1)}) {
    const auto decoded = decode_row(make_key(r), encode_value(r).to_str());
    ASSERT_TRUE(decoded);
    EXPECT_EQ(r, *decoded);
  }
}

TEST(PurgedSnapsKeys, RowRejectsBadInput)
{
  const auto r = iv(1, 10, 20);
  const auto value = encode_value(r).to_str();
  // not a purged_snaps key
  EXPECT_FALSE(decode_row("SNA_1_0000000000000013", value));
  // key does not match the value's interval
  EXPECT_FALSE(decode_row(make_key(iv(1, 10, 21)), value));
  // a single-underscore key (as written by a buggy tool)
  EXPECT_FALSE(decode_row("PSN_1_0000000000000013", value));
  // truncated value
  EXPECT_FALSE(decode_row(make_key(r), value.substr(0, 10)));
  // empty interval
  ceph::buffer::list empty;
  ceph::encode(int64_t{1}, empty);
  ceph::encode(snapid_t{20}, empty);
  ceph::encode(snapid_t{20}, empty);
  EXPECT_FALSE(decode_row(make_key(1, snapid_t{19}), empty.to_str()));
}

TEST(PurgedSnapsKeys, MappingKeyRoundTrip)
{
  const auto prefix = make_mapping_prefix(5, snapid_t{0x1e});
  EXPECT_EQ("SNA_5_000000000000001E_", prefix);
  // both an unsharded and a sharded mapping key
  for (const auto& key :
       {prefix + "0000000000000005.0001.1e.obj..", prefix + ".1_obj"}) {
    const auto parsed = parse_mapping_key(key);
    ASSERT_TRUE(parsed) << key;
    EXPECT_EQ((mapping_key_t{5, snapid_t{0x1e}}), *parsed);
  }
  const auto big = make_mapping_prefix(9999999999, snapid_t{max_snap});
  const auto parsed_big = parse_mapping_key(big + "x");
  ASSERT_TRUE(parsed_big);
  EXPECT_EQ((mapping_key_t{9999999999, snapid_t{max_snap}}), *parsed_big);
}

TEST(PurgedSnapsKeys, MappingKeyRejectsBadInput)
{
  EXPECT_FALSE(parse_mapping_key("OBJ_5_000000000000001E_x"));
  EXPECT_FALSE(parse_mapping_key("SNA_"));
  EXPECT_FALSE(parse_mapping_key("SNA_5"));
  EXPECT_FALSE(parse_mapping_key("SNA_x_000000000000001E_"));
  EXPECT_FALSE(parse_mapping_key("SNA_5_000000000000001E"));  // no separator
  EXPECT_FALSE(parse_mapping_key("SNA_5_00000000000001E_x"));  // 15 digits
  EXPECT_FALSE(parse_mapping_key("SNA_5_00000000000000ZE_x"));
}

// --- normalize() and flatten()

TEST(PurgedSnapsNormalize, MergesOverlappingAndAdjacent)
{
  // the staggered overlap left by Classic's writer, nested and duplicated
  // rows, adjacent rows - and pools given in their on-disk (decimal
  // string) order: 1, 20, 3
  const auto merged = normalize({
    iv(1, 8, 20), iv(1, 10, 25),  // staggered
    iv(1, 30, 40), iv(1, 40, 45),  // adjacent
    iv(20, 2000, 2010), iv(20, 2003, 2005), iv(20, 2000, 2010),  // nested, dup
    iv(3, 5, 6), iv(3, 1, 2), iv(3, 7, 7),  // unsorted; one empty
  });
  EXPECT_EQ(
    (std::vector{
      iv(1, 8, 25), iv(1, 30, 45), iv(3, 1, 2), iv(3, 5, 6),
      iv(20, 2000, 2010)}),
    merged);
}

TEST(PurgedSnapsNormalize, ContainmentChain)
{
  EXPECT_EQ(
    (std::vector{iv(1, 2, 10)}),
    normalize({iv(1, 4, 5), iv(1, 3, 7), iv(1, 2, 10)}));
}

TEST(PurgedSnapsNormalize, Flatten)
{
  by_epoch_t purged;
  purged[10][1].insert(snapid_t{8}, snapid_t{2});
  purged[10][3].insert(snapid_t{30}, snapid_t{10});
  purged[11][1].insert(snapid_t{20}, snapid_t{5});
  purged[11][1].insert(snapid_t{40}, snapid_t{1});
  auto rows = flatten(purged);
  std::ranges::sort(rows);
  EXPECT_EQ(
    (std::vector{iv(1, 8, 10), iv(1, 20, 25), iv(1, 40, 41), iv(3, 30, 40)}),
    rows);
}

// --- index_t

TEST(PurgedSnapsIndex, Find)
{
  const index_t idx{{
    iv(20, 2000, 2010), iv(3, 30, 40), iv(1, 10, 20), iv(1, 15, 30),
    iv(1, 50, 60)}};
  EXPECT_EQ(4u, idx.size());

  EXPECT_FALSE(idx.find(1, snapid_t{9}));
  EXPECT_EQ(iv(1, 10, 30), idx.find(1, snapid_t{10}));
  EXPECT_EQ(iv(1, 10, 30), idx.find(1, snapid_t{25}));  // the wider row
  EXPECT_EQ(iv(1, 10, 30), idx.find(1, snapid_t{29}));
  EXPECT_FALSE(idx.find(1, snapid_t{30}));
  EXPECT_FALSE(idx.find(1, snapid_t{49}));
  EXPECT_EQ(iv(1, 50, 60), idx.find(1, snapid_t{59}));
  EXPECT_FALSE(idx.find(1, snapid_t{60}));

  // per-pool: pool 1's intervals must not leak into pool 3's lookups
  EXPECT_FALSE(idx.find(3, snapid_t{25}));
  EXPECT_EQ(iv(3, 30, 40), idx.find(3, snapid_t{35}));
  EXPECT_EQ(iv(20, 2000, 2010), idx.find(20, snapid_t{2005}));
  EXPECT_FALSE(idx.find(2, snapid_t{15}));  // no rows for the pool
  EXPECT_FALSE(idx.find(21, snapid_t{2005}));
}

TEST(PurgedSnapsIndex, PoolView)
{
  const index_t idx{{iv(3, 30, 40), iv(1, 10, 20), iv(3, 1, 2)}};
  EXPECT_TRUE(std::ranges::equal(
    std::vector{iv(3, 1, 2), iv(3, 30, 40)}, idx.pool_view(3)));
  EXPECT_TRUE(idx.pool_view(2).empty());
  EXPECT_TRUE(index_t{}.empty());
  EXPECT_FALSE(index_t{}.find(1, snapid_t{1}));
}

TEST(PurgedSnapsIndex, ExtremeIds)
{
  const auto big_pool = std::numeric_limits<int64_t>::max();
  const index_t idx{{iv(big_pool, max_snap - 12, max_snap + 1), iv(0, 0, 1)}};
  EXPECT_TRUE(idx.find(big_pool, snapid_t{max_snap}));
  EXPECT_TRUE(idx.find(big_pool, snapid_t{max_snap - 12}));
  EXPECT_FALSE(idx.find(big_pool, snapid_t{max_snap - 13}));
  EXPECT_TRUE(idx.find(0, snapid_t{0}));
  EXPECT_FALSE(idx.find(0, snapid_t{1}));
}

// --- merge_rows()

TEST(PurgedSnapsMerge, NewRow)
{
  const auto upd = merge_rows({}, std::vector{iv(1, 10, 20)});
  EXPECT_TRUE(upd.rm_keys.empty());
  EXPECT_EQ(to_omap({iv(1, 10, 20)}), upd.set_rows);
}

TEST(PurgedSnapsMerge, AlreadyContainedIsNoop)
{
  const std::vector existing{iv(1, 10, 20)};
  EXPECT_TRUE(merge_rows(existing, std::vector{iv(1, 12, 15)}).empty());
  EXPECT_TRUE(merge_rows(existing, {}).empty());
}

TEST(PurgedSnapsMerge, JoinLeftRightAndBridge)
{
  const std::vector existing{iv(1, 10, 20), iv(1, 30, 40)};
  struct {
    std::vector<interval_t> added;
    std::vector<interval_t> want;
  } const cases[] = {
    {{iv(1, 8, 10)}, {iv(1, 8, 20), iv(1, 30, 40)}},    // join on the left
    {{iv(1, 40, 45)}, {iv(1, 10, 20), iv(1, 30, 45)}},  // join on the right
    {{iv(1, 20, 30)}, {iv(1, 10, 40)}},                 // bridge both rows
    {{iv(1, 5, 50)}, {iv(1, 5, 50)}},                   // spans both rows
    {{iv(2, 20, 30)}, {iv(1, 10, 20), iv(1, 30, 40), iv(2, 20, 30)}},
  };
  for (const auto& [added, want] : cases) {
    const auto upd = merge_rows(existing, added);
    for (const auto& k : upd.rm_keys) {
      EXPECT_FALSE(upd.set_rows.contains(k)) << "key both removed and set";
    }
    EXPECT_EQ(to_omap(want), apply_update(to_omap(existing), upd));
  }
}

TEST(PurgedSnapsMerge, TwoEpochsJoiningOneRow)
{
  // Classic's writer turns this into the overlapping rows [8,20) and
  // [10,25). Here it must result in a single row.
  const std::vector existing{iv(1, 10, 20)};
  by_epoch_t purged;
  purged[100][1].insert(snapid_t{8}, snapid_t{2});
  purged[101][1].insert(snapid_t{20}, snapid_t{5});
  const auto upd = merge_rows(existing, flatten(purged));
  EXPECT_EQ(
    (std::vector{iv(1, 8, 25)}), from_omap(apply_update(to_omap(existing), upd)));
}

TEST(PurgedSnapsMerge, RepairsOverlappingRows)
{
  // rows as left by Classic's writer (and some adjacent ones): merging
  // nothing into them normalizes them
  const std::vector existing{
    iv(1, 8, 20), iv(1, 10, 25), iv(1, 25, 30), iv(3, 1, 2), iv(20, 1, 9)};
  const auto upd = merge_rows(existing, {});
  EXPECT_EQ(
    (std::vector{iv(1, 8, 30), iv(20, 1, 9), iv(3, 1, 2)}),  // key order
    from_omap(apply_update(to_omap(existing), upd)));
  // the untouched rows are not rewritten
  EXPECT_FALSE(upd.set_rows.contains(make_key(iv(3, 1, 2))));
  EXPECT_FALSE(upd.rm_keys.contains(make_key(iv(3, 1, 2))));
  // the merged row reuses the key of the last row it covers
  EXPECT_TRUE(upd.set_rows.contains(make_key(iv(1, 25, 30))));
  EXPECT_FALSE(upd.rm_keys.contains(make_key(iv(1, 25, 30))));
}

TEST(PurgedSnapsMerge, Idempotent)
{
  const std::vector existing{iv(1, 10, 20)};
  const std::vector added{iv(1, 20, 30), iv(1, 40, 50)};
  const auto once = apply_update(to_omap(existing), merge_rows(existing, added));
  const auto rows = from_omap(once);
  EXPECT_TRUE(merge_rows(rows, added).empty());
}
