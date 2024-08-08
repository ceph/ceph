// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>
#include <gtest/gtest.h>

#include "os/bluestore/HybridAllocator.h"

class TestHybridAllocator : public HybridAllocator {
public:
  TestHybridAllocator(CephContext* cct,
                      int64_t device_size,
                      int64_t _block_size,
                      uint64_t max_entries,
      const std::string& name) :
    HybridAllocator(cct, device_size, _block_size,
      max_entries,
      name) {
  }

  uint64_t get_bmap_free() {
    return get_bmap() ? get_bmap()->get_free() : 0;
  }
  uint64_t get_avl_free() {
    return AvlAllocator::get_free();
  }
};

const uint64_t _1m = 1024 * 1024;
const uint64_t _4m = 4 * 1024 * 1024;

TEST(BitmapAllocator, claim_edge)
{
  {
    uint64_t block_size = 0x1000;
    uint64_t capacity = _1m;
    BitmapAllocator ha(g_ceph_context, capacity, block_size,
      "test_allocator");

    ha.init_add_free(0x1000, _1m - 0x2000);
    auto r = ha.claim_free_to_left(0);
    ASSERT_EQ(r, 0);

    ha.foreach([&](uint64_t o, uint64_t l) {
      ASSERT_EQ(o, 0x1000);
      ASSERT_EQ(l, 0xfe000);
    });

    r = ha.claim_free_to_right(0);
    ASSERT_EQ(r, 0);
    ha.foreach([&](uint64_t o, uint64_t l) {
      ASSERT_EQ(o, 0x1000);
      ASSERT_EQ(l, 0xfe000);
    });
  }
}

TEST(HybridAllocator, basic)
{
  {
    uint64_t block_size = 0x1000;
    uint64_t capacity = 0x10000 * _1m; // = 64GB
    TestHybridAllocator ha(g_ceph_context, capacity, block_size,
      4 * sizeof(range_seg_t), "test_hybrid_allocator");

    ASSERT_EQ(0, ha.get_free());
    ASSERT_EQ(0, ha.get_avl_free());
    ASSERT_EQ(0, ha.get_bmap_free());

    ha.init_add_free(0, _4m);
    ASSERT_EQ(_4m, ha.get_free());
    ASSERT_EQ(_4m, ha.get_avl_free());
    ASSERT_EQ(0, ha.get_bmap_free());

    ha.init_add_free(2 * _4m, _4m);
    ASSERT_EQ(_4m * 2, ha.get_free());
    ASSERT_EQ(_4m * 2, ha.get_avl_free());
    ASSERT_EQ(0, ha.get_bmap_free());

    ha.init_add_free(100 * _4m, _4m);
    ha.init_add_free(102 * _4m, _4m);

    ASSERT_EQ(_4m * 4, ha.get_free());
    ASSERT_EQ(_4m * 4, ha.get_avl_free());
    ASSERT_EQ(0, ha.get_bmap_free());

    // next allocs will go to bitmap
    ha.init_add_free(4 * _4m, _4m);
    ASSERT_EQ(_4m * 5, ha.get_free());
    ASSERT_EQ(_4m * 4, ha.get_avl_free());
    ASSERT_EQ(_4m * 1, ha.get_bmap_free());

    ha.init_add_free(6 * _4m, _4m);
    ASSERT_EQ(_4m * 6, ha.get_free());
    ASSERT_EQ(_4m * 4, ha.get_avl_free());
    ASSERT_EQ(_4m * 2, ha.get_bmap_free());

    // so we have 6x4M chunks, 4 chunks at AVL and 2 at bitmap

    ha.init_rm_free(_1m, _1m); // take 1M from AVL
    ASSERT_EQ(_1m * 23, ha.get_free());
    ASSERT_EQ(_1m * 14, ha.get_avl_free());
    ASSERT_EQ(_1m * 9, ha.get_bmap_free());

    ha.init_rm_free(6 * _4m + _1m, _1m); // take 1M from bmap
    ASSERT_EQ(_1m * 22, ha.get_free());
    ASSERT_EQ(_1m * 14, ha.get_avl_free());
    ASSERT_EQ(_1m * 8, ha.get_bmap_free());

    // so we have at avl: 2M~2M, 8M~4M, 400M~4M , 408M~4M
    // and at bmap: 0~1M, 16M~4M, 24~1M, 26~2M

    PExtentVector extents;
    // allocate 4K, to be served from bitmap
    EXPECT_EQ(block_size, ha.allocate(block_size, block_size,
      0, (int64_t)0, &extents));
    ASSERT_EQ(1, extents.size());
    ASSERT_EQ(0, extents[0].offset);

    ASSERT_EQ(_1m * 14, ha.get_avl_free());
    ASSERT_EQ(_1m * 8 - block_size, ha.get_bmap_free());

    interval_set<uint64_t> release_set;
    // release 4K, to be returned to bitmap
    release_set.insert(extents[0].offset, extents[0].length);
    ha.release(release_set);

    ASSERT_EQ(_1m * 14, ha.get_avl_free());
    ASSERT_EQ(_1m * 8, ha.get_bmap_free());
    extents.clear();
    release_set.clear();

    // again we have at avl: 2M~2M, 8M~4M, 400M~4M , 408M~4M
    // and at bmap: 0~1M, 16M~4M, 24M~1M, 26~2M

    // add 12M~3M which will go to avl
    ha.init_add_free(3 * _4m, 3 * _1m);
    ASSERT_EQ(_1m * 17, ha.get_avl_free());
    ASSERT_EQ(_1m * 8, ha.get_bmap_free());


    // add 15M~4K which will be appended to existing slot
    ha.init_add_free(15 * _1m, 0x1000);
    ASSERT_EQ(_1m * 17 + 0x1000, ha.get_avl_free());
    ASSERT_EQ(_1m * 8, ha.get_bmap_free());


    // again we have at avl: 2M~2M, 8M~(7M+4K), 400M~4M , 408M~4M
    // and at bmap: 0~1M, 16M~1M, 18M~2M, 24~4M

    //some removals from bmap
    ha.init_rm_free(28 * _1m - 0x1000, 0x1000);
    ASSERT_EQ(_1m * 17 + 0x1000, ha.get_avl_free());
    ASSERT_EQ(_1m * 8 - 0x1000, ha.get_bmap_free());

    ha.init_rm_free(24 * _1m + 0x1000, 0x1000);
    ASSERT_EQ(_1m * 17 + 0x1000, ha.get_avl_free());
    ASSERT_EQ(_1m * 8 - 0x2000, ha.get_bmap_free());

    ha.init_rm_free(24 * _1m + 0x1000, _4m - 0x2000);
    ASSERT_EQ(_1m * 17 + 0x1000, ha.get_avl_free());
    ASSERT_EQ(_1m * 4, ha.get_bmap_free());

    //4K removal from avl
    ha.init_rm_free(15 * _1m, 0x1000);
    ASSERT_EQ(_1m * 17, ha.get_avl_free());
    ASSERT_EQ(_1m * 4, ha.get_bmap_free());

    //remove highest 4Ms from avl
    ha.init_rm_free(_1m * 400, _4m);
    ha.init_rm_free(_1m * 408, _4m);
    ASSERT_EQ(_1m * 9, ha.get_avl_free());
    ASSERT_EQ(_1m * 4, ha.get_bmap_free());

    // we have at avl: 2M~2M, 8M~7M
    // and at bmap: 0~1M, 16M~1M, 18M~2M

    // this will be merged with neighbors from bmap and go to avl
    ha.init_add_free(17 * _1m, _1m);
    ASSERT_EQ(_1m * 1, ha.get_bmap_free());
    ASSERT_EQ(_1m * 13, ha.get_avl_free());

    // we have at avl: 2M~2M, 8M~7M, 16M~4M
    // and at bmap: 0~1M

    // and now do some cutoffs from 0~1M span

    //cut off 4K from bmap
    ha.init_rm_free(0 * _1m, 0x1000);
    ASSERT_EQ(_1m * 13, ha.get_avl_free());
    ASSERT_EQ(_1m * 1 - 0x1000, ha.get_bmap_free());

    //cut off 1M-4K from bmap
    ha.init_rm_free(0 * _1m + 0x1000, _1m - 0x1000);
    ASSERT_EQ(_1m * 13, ha.get_avl_free());
    ASSERT_EQ(0, ha.get_bmap_free());

    //cut off 512K avl
    ha.init_rm_free(17 * _1m + 0x1000, _1m / 2);
    ASSERT_EQ(_1m * 13 - _1m / 2, ha.get_avl_free());
    ASSERT_EQ(0, ha.get_bmap_free());

    //cut off the rest from avl
    ha.init_rm_free(17 * _1m + 0x1000 + _1m / 2, _1m / 2);
    ASSERT_EQ(_1m * 12, ha.get_avl_free());
    ASSERT_EQ(0, ha.get_bmap_free());
  }

  {
    uint64_t block_size = 0x1000;
    uint64_t capacity = 0x10000 * _1m; // = 64GB
    TestHybridAllocator ha(g_ceph_context, capacity, block_size,
      4 * sizeof(range_seg_t), "test_hybrid_allocator");

    ha.init_add_free(_1m, _1m);
    ha.init_add_free(_1m * 3, _1m);
    ha.init_add_free(_1m * 5, _1m);
    ha.init_add_free(0x4000, 0x1000);

    ASSERT_EQ(_1m * 3 + 0x1000, ha.get_free());
    ASSERT_EQ(_1m * 3 + 0x1000, ha.get_avl_free());
    ASSERT_EQ(0, ha.get_bmap_free());

    // This will substitute chunk 0x4000~1000.
    // Since new chunk insertion into into AvlAllocator:range_tree
    // happens immediately before 0x4000~1000 chunk care should be taken
    // to order operations properly and do not use already disposed iterator.
    ha.init_add_free(0, 0x2000);

    ASSERT_EQ(_1m * 3 + 0x3000, ha.get_free());
    ASSERT_EQ(_1m * 3 + 0x2000, ha.get_avl_free());
    ASSERT_EQ(0x1000, ha.get_bmap_free());
  }
  {
    uint64_t block_size = 0x1000;
    uint64_t capacity = 0x10000 * _1m; // = 64GB
    TestHybridAllocator ha(g_ceph_context, capacity, block_size,
      4 * sizeof(range_seg_t), "test_hybrid_allocator");

    // to be at avl
    ha.init_add_free(0, 2 * _1m);
    ha.init_add_free(4 * _1m , 2 * _1m);
    ha.init_add_free(8 * _1m, 2 * _1m);
    ha.init_add_free(16 * _1m, 4 * _1m);

    // to be at bitmap
    ha.init_add_free(24 * _1m, 1 * _1m);
    ha.init_add_free(28 * _1m, 1 * _1m);

    ASSERT_EQ(_1m * 10, ha.get_avl_free());
    ASSERT_EQ(_1m * 2, ha.get_bmap_free());

    // allocate 12M using 2M chunks. 10M to be returned
    PExtentVector extents;
    EXPECT_EQ(10 * _1m, ha.allocate(12 * _1m, 2 * _1m,
      0, (int64_t)0, &extents));

    // release everything allocated
    for (auto& e : extents) {
      ha.init_add_free(e.offset, e.length);
    }
    extents.clear();

    ASSERT_EQ(_1m * 10, ha.get_avl_free());
    ASSERT_EQ(_1m * 2, ha.get_bmap_free());
  }
}

TEST(HybridAllocator, fragmentation)
{
  {
    uint64_t block_size = 0x1000;
    uint64_t capacity = 0x1000 * 0x1000; // = 16M
    TestHybridAllocator ha(g_ceph_context, capacity, block_size,
      4 * sizeof(range_seg_t), "test_hybrid_allocator");

    ha.init_add_free(0, 0x2000);
    ha.init_add_free(0x4000, 0x2000);
    ha.init_add_free(0x8000, 0x2000);
    ha.init_add_free(0xc000, 0x1000);

    ASSERT_EQ(0.5, ha.get_fragmentation());

    // this will got to bmap with fragmentation = 1
    ha.init_add_free(0x10000, 0x1000);

    // which results in the following total fragmentation
    ASSERT_EQ(0.5 * 7 / 8 + 1.0 / 8, ha.get_fragmentation());
  }
}
