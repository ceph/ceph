// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"
#include "crimson/osd/object_metadata_helper.h"


TEST(head_subsets, dirty_region)
{
  uint64_t obj_size = 10;
  SnapSet empty_ss;
  hobject_t head{object_t{"foo"}, "foo", CEPH_NOSNAP, 42, 0, "nspace"};
  pg_missing_t missing;
  pg_missing_item item;
  uint64_t offset_1, len_1;
  offset_1 = 3;
  len_1 = 2;
  item.clean_regions.mark_data_region_dirty(offset_1, len_1);
  missing.add(head, std::move(item));
  hobject_t last_backfill{object_t{"foo1"}, "foo1", CEPH_NOSNAP, 42, 0, "nspace"};
  interval_set<uint64_t> expect_data_region;
  expect_data_region.insert(offset_1, len_1);

// ****

  crimson::osd::subsets_t result =
    crimson::osd::calc_head_subsets(obj_size,
                                    empty_ss,
                                    head,
                                    missing,
                                    last_backfill);

  EXPECT_TRUE(result.clone_subsets.empty());
  EXPECT_TRUE(result.data_subset == expect_data_region);
}

TEST(head_subsets, head_all_clean)
{
  uint64_t obj_size = 10;
  SnapSet empty_ss;
  hobject_t head{object_t{"foo"}, "foo", CEPH_NOSNAP, 42, 0, "nspace"};
  pg_missing_t missing;
  pg_missing_item item;
  missing.add(head, std::move(item));
  hobject_t last_backfill{object_t{"foo1"}, "foo1", CEPH_NOSNAP, 42, 0, "nspace"};

// ****

  crimson::osd::subsets_t result =
    crimson::osd::calc_head_subsets(obj_size,
                                    empty_ss,
                                    head,
                                    missing,
                                    last_backfill);

  EXPECT_TRUE(result.clone_subsets.empty());
  EXPECT_TRUE(result.data_subset.empty());
}

TEST(head_subsets, all_dirty)
{
  uint64_t obj_size = 10;
  SnapSet empty_ss;
  hobject_t head{object_t{"foo"}, "foo", CEPH_NOSNAP, 42, 0, "nspace"};
  pg_missing_t missing;
  pg_missing_item item;
  item.clean_regions.mark_fully_dirty();
  missing.add(head, std::move(item));
  hobject_t last_backfill{object_t{"foo1"}, "foo1", CEPH_NOSNAP, 42, 0, "nspace"};

// ****

  crimson::osd::subsets_t result =
    crimson::osd::calc_head_subsets(obj_size,
                                    empty_ss,
                                    head,
                                    missing,
                                    last_backfill);

  EXPECT_TRUE(result.clone_subsets.empty());
  EXPECT_TRUE(result.data_subset.size() == obj_size);
}

TEST(head_subsets, clone_overlap)
{
  uint64_t obj_size = 10;
  SnapSet ss;
  hobject_t head{object_t{"foo"}, "foo", CEPH_NOSNAP, 42, 0, "nspace"};
  pg_missing_t missing;
  pg_missing_item item;
  item.clean_regions.mark_fully_dirty();
  missing.add(head, std::move(item));
  hobject_t last_backfill{object_t{"foo1"}, "foo1", CEPH_NOSNAP, 42, 0, "nspace"};

  // Clone object:
  hobject_t clone = head;
  clone.snap = 0;
  std::map<snapid_t, interval_set<uint64_t>> clone_overlap;  // overlap w/ next
  interval_set<uint64_t> overlap;
  uint64_t offset_2, len_2;
  offset_2 = 2;
  len_2 = 2;
  overlap.insert(offset_2, len_2);
  clone_overlap[clone.snap] = overlap;

  // Snapset:
  // ss.seq = 0;
  // ss.snaps = snaps; (legacy)
  ss.clones.push_back(clone.snap);
  ss.clone_overlap = clone_overlap;
  // ss.clone_size = clone_size;
  // ss.clone_snaps = clone_snaps;

  // Expected intervals:
  interval_set<uint64_t> expect_clone_subset;
  expect_clone_subset.insert(offset_2, len_2);

// ****

  crimson::osd::subsets_t result =
    crimson::osd::calc_head_subsets(obj_size,
                                    ss,
                                    head,
                                    missing,
                                    last_backfill);
  EXPECT_TRUE(result.clone_subsets[clone] == expect_clone_subset);
}

TEST(head_subsets, dirty_region_and_clone_overlap)
{
  uint64_t obj_size = 100;
  SnapSet ss;
  hobject_t head{object_t{"foo"}, "foo", CEPH_NOSNAP, 42, 0, "nspace"};
  pg_missing_t missing;
  pg_missing_item item;
  uint64_t offset_1, len_1;
  offset_1 = 3;
  len_1 = 2;
  item.clean_regions.mark_data_region_dirty(offset_1, len_1);
  missing.add(head, std::move(item));
  hobject_t last_backfill{object_t{"foo1"}, "foo1", CEPH_NOSNAP, 42, 0, "nspace"};
  interval_set<uint64_t> expect_data_region;
  expect_data_region.insert(offset_1, len_1);

  // Clone object:
  hobject_t clone = head;
  clone.snap = 0;
  std::map<snapid_t, interval_set<uint64_t>> clone_overlap;  // overlap w/ next
  interval_set<uint64_t> overlap;
  uint64_t offset_2, len_2;
  offset_2 = 2;
  len_2 = 2;
  overlap.insert(offset_2, len_2);
  clone_overlap[clone.snap] = overlap;

  // Snapset:
  // ss.seq = 0;
  // ss.snaps = snaps; (legacy)
  ss.clones.push_back(clone.snap);
  ss.clone_overlap = clone_overlap;
  // ss.clone_size = clone_size;
  // ss.clone_snaps = clone_snaps;

  // Expected intervals:
  interval_set<uint64_t> expect_clone_subset;
  expect_clone_subset.insert(offset_2, len_2);
  expect_clone_subset.intersection_of(expect_data_region);
  expect_data_region.subtract(expect_clone_subset);

// ****

  crimson::osd::subsets_t result =
    crimson::osd::calc_head_subsets(obj_size,
                                    ss,
                                    head,
                                    missing,
                                    last_backfill);
  EXPECT_TRUE(result.clone_subsets[clone] == expect_clone_subset);
  EXPECT_TRUE(result.data_subset == expect_data_region);
}

TEST(clone_subsets, overlap)
{
  uint64_t clone_size = 10;
  SnapSet ss;
  hobject_t clone{object_t{"foo"}, "foo", 1, 42, 0, "nspace"};
  ss.clone_size[1] = clone_size;
  ss.clones.push_back(snapid_t(0));
  ss.clones.push_back(snapid_t(1));
  ss.clones.push_back(snapid_t(2));
  pg_missing_t missing;
  pg_missing_item item;
  missing.add(clone, std::move(item));
  hobject_t last_backfill{object_t{"foo1"}, "foo1", CEPH_NOSNAP, 42, 0, "nspace"};

  interval_set<uint64_t> expect_clone_subset1, expect_clone_subset2;

  // Next older clone:
  hobject_t older_clone = clone;
  older_clone.snap = 0;
  {
    std::map<snapid_t, interval_set<uint64_t>> clone_overlap;  // overlap w/ next
    interval_set<uint64_t> overlap;
    uint64_t offset_2, len_2;
    offset_2 = 4;
    len_2 = 2;
    overlap.insert(offset_2, len_2);
    ss.clone_overlap[older_clone.snap] = overlap;

    // Snapset:
    // ss.seq = 0;
    // ss.snaps = snaps; (legacy)
    // ss.clones.push_back(snapid_t());
    // ss.clone_overlap = clone_overlap;
    // ss.clone_size = clone_size;
    // ss.clone_snaps = clone_snaps;

    // Expected intervals:
    expect_clone_subset1.insert(offset_2, len_2);
  }

  // Next newest clone:
  hobject_t newest_clone = clone;
  newest_clone.snap = 2;
  {
    std::map<snapid_t, interval_set<uint64_t>> clone_overlap;  // overlap w/ next
    interval_set<uint64_t> overlap;
    uint64_t offset_2, len_2;
    offset_2 = 2;
    len_2 = 2;
    overlap.insert(offset_2, len_2);
    ss.clone_overlap[newest_clone.snap - 1] = overlap;

    // Snapset:
    // ss.seq = 0;
    // ss.snaps = snaps; (legacy)
    // ss.clones.push_back(snapid_t());
    // ss.clone_overlap = clone_overlap;
    // ss.clone_size = clone_size;
    // ss.clone_snaps = clone_snaps;

    // Expected intervals:
    expect_clone_subset2.insert(offset_2, len_2);
  }

// ****

  crimson::osd::subsets_t result =
    crimson::osd::calc_clone_subsets(ss,
                                     clone,
                                     missing,
                                     last_backfill);
  EXPECT_TRUE(result.clone_subsets[older_clone] == expect_clone_subset1);
  EXPECT_TRUE(result.clone_subsets[newest_clone] == expect_clone_subset2);
}
