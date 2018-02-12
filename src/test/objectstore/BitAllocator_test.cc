// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Bitmap based in-memory allocator unit test cases.
 * Author: Ramesh Chander, Ramesh.Chander@sandisk.com
 */

#include "include/Context.h"
#include "os/bluestore/BitAllocator.h"
#include <stdio.h>
#include <assert.h>
#include <math.h>
#include <sstream>
#include <gtest/gtest.h>


//#define bmap_test_assert(x) ASSERT_EQ(true, (x))
#define bmap_test_assert(x) assert((x))
#define NUM_THREADS 16
#define MAX_BLOCKS (1024 * 1024 * 1)

TEST(BitAllocator, test_bmap_iter)
{
  int num_items = 5;
  int off = 2;

  class BmapEntityTmp {
      int64_t m_num = 0;
      int64_t m_len = 0;
    public:
      void init(int index) {
        m_num = index;
      }
      BmapEntityTmp() {

      }
      BmapEntityTmp(int num) {
        m_num = num;
        m_len = num;
      }

      int64_t get_index() {
        return m_num;
      }
      bool is_allocated(int64_t s, int64_t num)
      {
        return true;
      }
  };
  BmapEntityTmp *obj = NULL;
  int i = 0;
  mempool::bluestore_alloc::vector<BmapEntityTmp> *arr = new mempool::bluestore_alloc::vector<BmapEntityTmp>(num_items);
  for (i = 0; i < num_items; i++) {
    (*arr)[i].init(i);
  }
  BitMapEntityIter<BmapEntityTmp> iter = BitMapEntityIter<BmapEntityTmp>(arr, off, false);

  i = off;
  int count = 0;
  int64_t last_idx = off;
  while ((obj = iter.next())) {
    bmap_test_assert(obj->get_index() == last_idx);
    bmap_test_assert(obj->get_index() == i);
    bmap_test_assert(obj == &(*arr)[i]);
    last_idx = iter.index();
    i++;
    count++;
  }
  bmap_test_assert(i == num_items);
  bmap_test_assert(count == num_items - off);

  iter = BitMapEntityIter<BmapEntityTmp>(arr, off, true);

  i = off;
  last_idx = off;
  count = 0;
  while ((obj = iter.next())) {
    bmap_test_assert(obj->get_index() == last_idx);
    bmap_test_assert(obj->get_index() == i);
    bmap_test_assert(obj == &(*arr)[i]);
    last_idx = iter.index();

    i = (i + 1) % num_items;
    count++;
  }
  bmap_test_assert(i == off + 1);
  bmap_test_assert(count == num_items + 1);

  delete arr;

  num_items = 4;
  off = num_items - 1;

  arr = new mempool::bluestore_alloc::vector<BmapEntityTmp>(num_items);
  for (i = 0; i < num_items; i++) {
    (*arr)[i].init(i);
  }
  iter = BitMapEntityIter<BmapEntityTmp>(arr, off, true);
  i = off;
  last_idx = off;
  count = 0;
  while ((obj = static_cast<BmapEntityTmp*>(iter.next()))) {
    bmap_test_assert(obj->get_index() == last_idx);
    bmap_test_assert(obj->get_index() == i);
    bmap_test_assert(obj == &(*arr)[i]);
    last_idx = iter.index();
    i = (i + 1) % num_items;
    count++;
  }
  bmap_test_assert(i == (off + 1)%num_items);
  bmap_test_assert(count == num_items + 1);

  delete arr;

  /*
   * BitMapArea Iter tests.
   */
  BitMapArea *area = nullptr;
  std::vector<BitMapArea*> children;
  children.reserve(num_items);
  for (i = 0; i < num_items; i++) {
    children.emplace_back(new BitMapAreaLeaf(
      g_ceph_context,
      BitMapArea::get_span_size(g_ceph_context), i, false));
  }

  off = 0;
  BitMapAreaList *area_list = \
    new BitMapAreaList(std::vector<BitMapArea*>(children));
  BmapEntityListIter area_iter = BmapEntityListIter(
                                area_list, (int64_t) 0);
  i = off;
  last_idx = off;
  count = 0;
  while ((area = area_iter.next())) {
    bmap_test_assert(area->get_index() == last_idx);
    bmap_test_assert(area->get_index() == i);
    bmap_test_assert(area == children[i]);
    last_idx = area_iter.index();
    i = (i + 1) % num_items;
    count++;
  }
  bmap_test_assert(i == off);
  bmap_test_assert(count == num_items);

  off = 0;
  area_iter = BmapEntityListIter(area_list, off, true);
  i = off;
  last_idx = off;
  count = 0;
  while ((area = area_iter.next())) {
    bmap_test_assert(area->get_index() == last_idx);
    bmap_test_assert(area->get_index() == i);
    bmap_test_assert(area == children[i]);
    last_idx = area_iter.index();
    i = (i + 1) % num_items;
    count++;
  }
  bmap_test_assert(i == (off + 1)%num_items);
  bmap_test_assert(count == num_items + 1);

  for (i = 0; i < num_items; i++)
    delete children[i];

  delete area_list;
}

TEST(BitAllocator, test_bmap_entry)
{
  int i = 0;
  int start = 0;
  int64_t scanned = 0;
  int64_t allocated = 0;
  int size = BmapEntry::size();

  BmapEntry *bmap = new BmapEntry(g_ceph_context, true);

  // Clear bits one by one and check they are cleared
  for (i = 0; i < size; i++) {
    bmap->clear_bit(i);
    bmap_test_assert(!bmap->check_bit(i));
  }

  // Set all bits again using set_bits
  bmap->set_bits(0, size);

  // clear 4 bits at a time and then check allocated
  for (i = 0; i < size/4; i++) {
    bmap->clear_bits(i * 4, 4);
    bmap_test_assert(!bmap->is_allocated(i * 4, 4));
  }

  // set all bits again
  bmap->set_bits(0, size);

  // clear alternate bits, check and set those bits
  for (i = 0; i < size/2; i++) {
    bmap->clear_bit(i * 2 + 1);
    bmap_test_assert(!bmap->check_bit(i * 2 + 1));
    bmap_test_assert(bmap->check_n_set_bit(i * 2 + 1));
  }

  // free 1, 2 and size bits at a time and try to find n cont bits
  for (i = 0; i < size / 4; i++) {
    bmap->clear_bits(i * 2 + 1, i + 1);
    bmap_test_assert(!bmap->check_bit(i * 2 + 1));
    bmap_test_assert(bmap->find_n_cont_bits(i * 2 + 1, i + 1) ==
        i + 1);
  }

  // free 1, 2 and size bits at a time and try to find any cont bits
  for (i = 0; i < size / 4; i++) {
    bmap->clear_bits(i * 2 + 1, i + 1);
    bmap_test_assert(!bmap->is_allocated(i * 2 + 1, i + 1));
  }

  for (i = 0; i < size / 4; i++) {
    bmap->clear_bits(i * 2 + 1, i + 1);
    allocated = bmap->find_first_set_bits(i + 1, 0, &start, &scanned);

    bmap_test_assert(allocated == i + 1);
    bmap_test_assert(scanned == ((i * 2 + 1) + (i + 1)));
    bmap_test_assert(start == i * 2 + 1);
    bmap->set_bits(0, BmapEntry::size());

  }



  // Find few bits at end of bitmap and find those
  bmap->clear_bits(0, 4);
  bmap->clear_bits(BmapEntry::size() - 12, 5);
  bmap->clear_bits(BmapEntry::size() - 6, 6);
  allocated = bmap->find_first_set_bits(6, 0, &start, &scanned);

  bmap_test_assert(allocated == 6);
  bmap_test_assert(scanned == BmapEntry::size() - 6 + 6);
  bmap_test_assert(start == BmapEntry::size() - 6);
  bmap_test_assert(bmap->is_allocated(start, 6));

  delete bmap;

  {

    bmap = new BmapEntry(g_ceph_context, false);
    start = -1;
    scanned = 0;
    allocated = bmap->find_first_set_bits(1, 1, &start, &scanned);
    bmap_test_assert(allocated == 1);
    bmap_test_assert(start == 1);

    allocated = bmap->find_first_set_bits(1, BmapEntry::size() - 2, &start, &scanned);
    bmap_test_assert(allocated == 1);
    bmap_test_assert(start == BmapEntry::size() - 2);

    bmap->clear_bits(0, BmapEntry::size());
    bmap->set_bits(0, BmapEntry::size() / 4);
    allocated = bmap->find_first_set_bits(4, 2, &start, &scanned);
    bmap_test_assert(allocated == 4);
    bmap_test_assert(start == BmapEntry::size() / 4);
    delete bmap;
  }

  bmap = new BmapEntry(g_ceph_context, false);
  bmap->set_bits(4, BmapEntry::size() - 4);
  bmap_test_assert(bmap->is_allocated(4, BmapEntry::size() - 4));
  bmap_test_assert(!bmap->is_allocated(0, 4));
  bmap->set_bits(0, 4);
  bmap_test_assert(bmap->is_allocated(0, BmapEntry::size()));
  delete bmap;

}

TEST(BitAllocator, test_zone_alloc)
{
  int total_blocks = 1024;
  int64_t allocated = 0;

  std::unique_ptr<BitMapZone> zone = std::make_unique<BitMapZone>(g_ceph_context, total_blocks, 0);

  // Allocate all blocks and see that it is allocating in order.
  bool lock = zone->lock_excl_try();
  bmap_test_assert(lock);

  int64_t blk_size = 1024;
  PExtentVector extents;
  AllocatorExtentList block_list(&extents, blk_size);
  allocated = zone->alloc_blocks_dis(zone->size() / 2, 1, 0, 0, &block_list);
  bmap_test_assert(allocated == zone->size() / 2);


  {
    int64_t blk_size = 1024;
    PExtentVector extents;
    AllocatorExtentList block_list(&extents, blk_size);

    zone = std::make_unique<BitMapZone>(g_ceph_context, total_blocks, 0);
    lock = zone->lock_excl_try();
    bmap_test_assert(lock);
    for (int i = 0; i < zone->size(); i += 4) {
      block_list.reset();
      allocated = zone->alloc_blocks_dis(1, 1, i, 0, &block_list);
      bmap_test_assert(allocated == 1);
      EXPECT_EQ(extents[0].offset, (uint64_t) i * blk_size);
    }

    for (int i = 0; i < zone->size(); i += 4) {
      zone->free_blocks(i, 1);
    }
  }

  /*
   * Min alloc size cases.
   */
  {
    int64_t blk_size = 1;
    PExtentVector extents;

    for (int i = 1; i <= total_blocks - BmapEntry::size(); i = i << 1) {
      for (int64_t j = 0; j <= BmapEntry::size(); j = 1 << j) {
	extents.clear();
        AllocatorExtentList block_list(&extents, blk_size);
	zone = std::make_unique<BitMapZone>(g_ceph_context, total_blocks, 0);
        lock = zone->lock_excl_try();
        bmap_test_assert(lock);

        block_list.reset();
        int64_t need_blks = (((total_blocks - j) / i) * i);
        allocated = zone->alloc_blocks_dis(need_blks, i, j, 0, &block_list);
        bmap_test_assert(allocated == need_blks);
        bmap_test_assert(extents[0].offset ==  (uint64_t) j);
      }
    }

    //allocation in loop
    {
      extents.clear();
      AllocatorExtentList block_list(&extents, blk_size);
      zone = std::make_unique<BitMapZone>(g_ceph_context, total_blocks, 0);
      lock = zone->lock_excl_try();

      for (int iter = 1; iter < 5; iter++) {
        for (int i = 1; i <= total_blocks; i = i << 1) {
          for (int j = 0; j < total_blocks; j +=i) {
            bmap_test_assert(lock);
            block_list.reset();
            int64_t need_blks = i;
            allocated = zone->alloc_blocks_dis(need_blks, i, 0, 0, &block_list);
            bmap_test_assert(allocated == need_blks);
            bmap_test_assert(extents[0].offset ==  (uint64_t) j);
            block_list.reset();
          }
          {
            allocated = zone->alloc_blocks_dis(1, 1, 0, 0, &block_list);
            bmap_test_assert(allocated == 0);
            block_list.reset();
          }
         
          for (int j = 0; j < total_blocks; j +=i) {
            zone->free_blocks(j, i);
          }
        }
      }
    }

    {
      extents.clear();
      AllocatorExtentList block_list(&extents, blk_size);
      zone = std::make_unique<BitMapZone>(g_ceph_context, total_blocks, 0);
      lock = zone->lock_excl_try();
      bmap_test_assert(lock);

      block_list.reset();
      allocated = zone->alloc_blocks_dis(total_blocks + 1, total_blocks + 1, 0, 1024, &block_list);
      bmap_test_assert(allocated == 0);

      block_list.reset();
      allocated = zone->alloc_blocks_dis(total_blocks, total_blocks, 1, 1024, &block_list);
      bmap_test_assert(allocated == 0);

      block_list.reset();
      allocated = zone->alloc_blocks_dis(total_blocks, total_blocks, 0, 0, &block_list);
      bmap_test_assert(allocated == total_blocks);
      bmap_test_assert(extents[0].offset == 0);

      zone->free_blocks(extents[0].offset, allocated);
        
      extents.clear();
      block_list = AllocatorExtentList(&extents, blk_size, total_blocks / 4 * blk_size);
      allocated = zone->alloc_blocks_dis(total_blocks, total_blocks / 4, 0, 0, &block_list);
      bmap_test_assert(allocated == total_blocks);
      for (int i = 0; i < 4; i++) {
	bmap_test_assert(extents[i].offset == (uint64_t) i * (total_blocks / 4));
      }
    }
  }
}

TEST(BitAllocator, test_bmap_alloc)
{
  const int max_iter = 3;

  for (int round = 0; round < 3; round++) {
    // Test zone of different sizes: 512, 1024, 2048
    int64_t zone_size = 512ull << round;
    ostringstream val;
    val << zone_size;
    g_conf->set_val("bluestore_bitmapallocator_blocks_per_zone", val.str());

    // choose randomized span_size
    int64_t span_size = 512ull << (rand() % 4);
    val.str("");
    val << span_size;
    g_conf->set_val("bluestore_bitmapallocator_span_size", val.str());
    g_ceph_context->_conf->apply_changes(NULL);

    int64_t total_blocks = zone_size * 4;
    int64_t allocated = 0;

    BitAllocator *alloc = new BitAllocator(g_ceph_context, total_blocks,
					   zone_size, CONCURRENT);
    int64_t alloc_size = 2;
    for (int64_t iter = 0; iter < max_iter; iter++) {
      for (int64_t j = 0; alloc_size <= total_blocks; j++) {
        int64_t blk_size = 1024;
        PExtentVector extents;
        AllocatorExtentList block_list(&extents, blk_size, alloc_size);
        for (int64_t i = 0; i < total_blocks; i += alloc_size) {
          bmap_test_assert(alloc->reserve_blocks(alloc_size) == true);
          allocated = alloc->alloc_blocks_dis_res(alloc_size, std::min(alloc_size, zone_size),
                                                  0, &block_list);
          bmap_test_assert(alloc_size == allocated);
          bmap_test_assert(block_list.get_extent_count() == 
                           (alloc_size > zone_size? alloc_size / zone_size: 1));
          bmap_test_assert(extents[0].offset == (uint64_t) i * blk_size);
          bmap_test_assert((int64_t) extents[0].length == 
                           ((alloc_size > zone_size? zone_size: alloc_size) * blk_size));
          block_list.reset();
        }
        for (int64_t i = 0; i < total_blocks; i += alloc_size) {
          alloc->free_blocks(i, alloc_size);
        }
        alloc_size = 2 << j; 
      }
    }

    int64_t blk_size = 1024;
    PExtentVector extents;

    AllocatorExtentList block_list(&extents, blk_size);
  
    ASSERT_EQ(alloc->reserve_blocks(alloc->size() / 2), true);
    allocated = alloc->alloc_blocks_dis_res(alloc->size()/2, 1, 0, &block_list);
    ASSERT_EQ(alloc->size()/2, allocated);

    block_list.reset();
    ASSERT_EQ(alloc->reserve_blocks(1), true);
    allocated = alloc->alloc_blocks_dis_res(1, 1, 0, &block_list);
    bmap_test_assert(allocated == 1);

    alloc->free_blocks(alloc->size()/2, 1);

    block_list.reset();
    ASSERT_EQ(alloc->reserve_blocks(1), true);
    allocated = alloc->alloc_blocks_dis_res(1, 1, 0, &block_list);
    bmap_test_assert(allocated == 1);

    bmap_test_assert((int64_t) extents[0].offset == alloc->size()/2 * blk_size);

    delete alloc;

  }

  // restore to typical value
  g_conf->set_val("bluestore_bitmapallocator_blocks_per_zone", "1024");
  g_conf->set_val("bluestore_bitmapallocator_span_size", "1024");
  g_ceph_context->_conf->apply_changes(NULL);
}

bool alloc_extents_max_block(BitAllocator *alloc,
           int64_t max_alloc,
           int64_t total_alloc)
{
  int64_t blk_size = 1;
  int64_t allocated = 0;
  int64_t verified = 0;
  int64_t count = 0;
  PExtentVector extents;

  AllocatorExtentList block_list(&extents, blk_size, max_alloc);

  EXPECT_EQ(alloc->reserve_blocks(total_alloc), true);
  allocated = alloc->alloc_blocks_dis_res(total_alloc, blk_size, 0, &block_list);
  EXPECT_EQ(allocated, total_alloc);

  max_alloc = total_alloc > max_alloc? max_alloc: total_alloc;

  for (auto &p: extents) {
    count++;
    EXPECT_EQ(p.length,  max_alloc);
    verified += p.length;
    if (verified >= total_alloc) {
      break;
    }
  }

  EXPECT_EQ(total_alloc / max_alloc, count);
  return true;
}

TEST(BitAllocator, test_bmap_alloc2)
{
  int64_t total_blocks = 1024 * 4;
  int64_t zone_size = 1024;
  std::unique_ptr<BitAllocator> alloc = std::make_unique<BitAllocator>(g_ceph_context, total_blocks,
					 zone_size, CONCURRENT);

  alloc_extents_max_block(alloc.get(), 1, 16);
  alloc_extents_max_block(alloc.get(), 4, 16);
  alloc_extents_max_block(alloc.get(), 16, 16);
  alloc_extents_max_block(alloc.get(), 32, 16);
}

__thread int my_tid;

void
do_work_dis(BitAllocator *alloc)
{
  int num_iters = 10;
  int64_t alloced = 0;
  int64_t num_blocks = alloc->size() / NUM_THREADS;

  PExtentVector extents;
  AllocatorExtentList block_list(&extents, 4096);

  while (num_iters--) {
    alloc_assert(alloc->reserve_blocks(num_blocks));
    alloced = alloc->alloc_blocks_dis_res(num_blocks, 1, 0, &block_list);
    alloc_assert(alloced == num_blocks);

    alloc_assert(alloc->is_allocated_dis(&block_list, num_blocks));
    alloc->free_blocks_dis(num_blocks, &block_list);
    block_list.reset();
  }
}

int tid = 0;
static bool cont = true;

void *
worker(void *args)
{
  my_tid = __sync_fetch_and_add(&tid, 1);
  BitAllocator *alloc = (BitAllocator *) args;
  printf("Starting thread %d", my_tid);
  do_work_dis(alloc);

  return NULL;
}

TEST(BitAllocator, test_bmap_alloc_concurrent)
{
  int64_t total_blocks = MAX_BLOCKS;
  int64_t zone_size = 1024;
  pthread_t pthreads[NUM_THREADS] = {0};

  bmap_test_assert(total_blocks <= MAX_BLOCKS);

  std::unique_ptr<BitAllocator> alloc = std::make_unique<BitAllocator>(g_ceph_context, total_blocks,
					 zone_size, CONCURRENT);

  for (int k = 0; k < 2; k++) {
    cont = k;
    printf("Spawning %d threads for parallel test. Mode Cont = %d.....\n", NUM_THREADS, cont);
    for (int j = 0; j < NUM_THREADS; j++) {
      if (pthread_create(&pthreads[j], NULL, worker, alloc.get())) {
        printf("Unable to create worker thread.\n");
        exit(0);
      }
    }

    for (int j = 0; j < NUM_THREADS; j++) {
      pthread_join(pthreads[j], NULL);
    }
  }
}
