// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Bitmap based in-memory allocator unit test cases.
 * Author: Ramesh Chander, Ramesh.Chander@sandisk.com
 */

#include "os/bluestore/BitAllocator.h"
#include <stdio.h>
#include <assert.h>
#include <math.h>
#include <gtest/gtest.h>

#define bmap_test_assert(x) EXPECT_EQ(true, (x))
#define NUM_THREADS 16
#define MAX_BLOCKS (1024 * 1024 * 1)

TEST(BitAllocator, test_bmap_iter)
{
  int num_items = 5;
  int off = 2;

  class BmapEntityTmp {
      int64_t m_num;
      int64_t m_len;
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
  std::vector<BmapEntityTmp> *arr = new std::vector<BmapEntityTmp>(num_items);
  for (i = 0; i < num_items; i++) {
    (*arr)[i].init(i);
  }
  //BitMapList<BmapEntityTmp> *list = new BitMapList<BmapEntityTmp>(arr, num_items, 0);
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
  //delete list;

  num_items = 4;
  off = num_items - 1;

  arr = new std::vector<BmapEntityTmp>(num_items);
  for (i = 0; i < num_items; i++) {
    (*arr)[i].init(i);
  }
//  list = new BitMapList<BmapEntityTmp>(arr, num_items, 0);
  iter = BitMapEntityIter<BmapEntityTmp>(arr, off, true);
  i = off;
  last_idx = off;
  count = 0;
  while ((obj = (BmapEntityTmp*) iter.next())) {
    bmap_test_assert(obj->get_index() == last_idx);
    bmap_test_assert(obj->get_index() == i);
    bmap_test_assert(obj == &(*arr)[i]);
    last_idx = iter.index();
    i = (i + 1) % num_items;
    count++;
  }
  bmap_test_assert(i == (off + 1)%num_items);
  bmap_test_assert(count == num_items + 1);

  /*
   * BitMapArea Iter tests.
   */
  BitMapArea *area = NULL;
  BitMapArea **children = new BitMapArea*[num_items];
  for (i = 0; i < num_items; i++) {
      children[i] = new BitMapAreaLeaf(BitMapArea::get_span_size(), i, false);
  }

  off = 0;
  BitMapAreaList *area_list = new BitMapAreaList(children, num_items);
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

  // offset 0
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
}

TEST(BitAllocator, test_bmap_entry)
{
  int i = 0;
  int start = 0;
  int64_t scanned = 0;
  int64_t allocated = 0;
  int size = BmapEntry::size();

  BmapEntry *bmap = new BmapEntry(true);

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
  bmap = new BmapEntry(false);
  bmap->set_bits(4, BmapEntry::size() - 4);
  bmap_test_assert(bmap->is_allocated(4, BmapEntry::size() - 4));
  bmap_test_assert(!bmap->is_allocated(0, 4));
  bmap->set_bits(0, 4);
  bmap_test_assert(bmap->is_allocated(0, BmapEntry::size()));
}

TEST(BitAllocator, test_zone_alloc)
{
  int total_blocks = 1024;
  int64_t blks = 1;
  int64_t last_blk = -1;
  int64_t start_blk = 0;
  int64_t allocated = 0;

  BitMapZone *zone = new BitMapZone(total_blocks, 0);

  // Allocate all blocks and see that it is allocating in order.
  bool lock = zone->lock_excl_try();
  bmap_test_assert(lock);

  for (int i = 0; i < total_blocks; i++) {
    allocated = zone->alloc_blocks(blks, &start_blk);
    bmap_test_assert(last_blk + 1 == start_blk);
    bmap_test_assert(allocated == blks);
    last_blk = start_blk;
  }
  bmap_test_assert(zone->get_used_blocks() == total_blocks);

  for (int i = 0; i < total_blocks; i++) {
    bmap_test_assert(zone->get_used_blocks() == total_blocks - i);
    zone->free_blocks(i, blks);
  }

  blks = 2;
  last_blk = -2;
  for (int i = 0; i < total_blocks/2; i++) {
    allocated = zone->alloc_blocks(blks, &start_blk);
    bmap_test_assert(last_blk + 2 == start_blk);
    last_blk = start_blk;
  }

  // Free different boundaries and allocate those
  blks = 3;
  bmap_test_assert(zone->is_exhausted());
  zone->free_blocks(BmapEntry::size() - blks, blks);
  zone->free_blocks(BmapEntry::size(), blks);

  allocated = zone->alloc_blocks(blks * 2, &start_blk);
  bmap_test_assert(BmapEntry::size() - blks == start_blk);
  bmap_test_assert(allocated == blks * 2);

  blks = 4;
  zone->free_blocks(BmapEntry::size() * 2 - blks, 2 * blks);
  allocated = zone->alloc_blocks(2 * blks, &start_blk);
  bmap_test_assert(BmapEntry::size() * 2 - blks == start_blk);
  bmap_test_assert(allocated == blks * 2);

  blks = BmapEntry::size() * 2;
  zone->free_blocks(BmapEntry::size() * 6 - blks, blks);
  allocated = zone->alloc_blocks(blks, &start_blk);
  bmap_test_assert(BmapEntry::size() * 6 - blks == start_blk);

  // free blocks at distance 1, 2 up to 63 and allocate all of them
  // together using disc alloc.
  blks = BmapEntry::size() * 2;
  int num_bits = 1;
  for (int i = 0; i < zone->size() / BmapEntry::size() -1; i++) {
    zone->free_blocks(i * BmapEntry::size(), num_bits);
    num_bits++;
  }

  num_bits = 1;
  int64_t start_block = 0;
  for (int i = 0; i < zone->size() / BmapEntry::size() -1; i++) {
    allocated = zone->alloc_blocks(num_bits, &start_block);
    bmap_test_assert(num_bits == allocated);
    bmap_test_assert(start_block == i * BmapEntry::size());
    num_bits++;
  }

  start_block = 0;
  num_bits = 1;
  for (int i = 0; i < zone->size() / BmapEntry::size() -1; i++) {
    zone->free_blocks(i * BmapEntry::size(), num_bits);
    num_bits++;
  }

  delete zone;
  // non-conti allocations test
  zone = new BitMapZone(total_blocks, 0);
  lock = zone->lock_excl_try();
  bmap_test_assert(lock);
  int64_t blocks[1024] = {0};
  for (int i = 0; i < zone->size(); i++) {
    allocated = zone->alloc_blocks(1, &start_block);
    bmap_test_assert(allocated == 1);
  }
  for (int i = 0; i < zone->size(); i += 2) {
    zone->free_blocks(i, 1);
  }

  allocated = zone->alloc_blocks_dis(zone->size() / 2, 0, blocks);
  bmap_test_assert(allocated == zone->size() / 2);
}

TEST(BitAllocator, test_bmap_alloc)
{
  int64_t total_blocks = 1024 * 4;
  int64_t zone_size = 1024;
  int64_t allocated = 0;
  int64_t start_block = 0;

  BitAllocator *alloc = new BitAllocator(total_blocks, zone_size, CONCURRENT);

  for (int64_t iter = 0; iter < 4; iter++) {
    for (int64_t i = 0; i < total_blocks; i++) {
      allocated = alloc->alloc_blocks(1, &start_block);
      bmap_test_assert(allocated == 1);
      bmap_test_assert(start_block == i);
    }

    for (int64_t i = 0; i < total_blocks; i++) {
      alloc->free_blocks(i, 1);
    }
  }

  for (int64_t iter = 0; iter < 4; iter++) {
    for (int64_t i = 0; i < total_blocks / zone_size; i++) {
      allocated = alloc->alloc_blocks(zone_size, &start_block);
      bmap_test_assert(allocated == zone_size);
      bmap_test_assert(start_block == i * zone_size);
    }

    for (int64_t i = 0; i < total_blocks / zone_size; i++) {
      alloc->free_blocks(i * zone_size, zone_size);
    }
  }

  allocated = alloc->alloc_blocks(1, &start_block);
  bmap_test_assert(allocated == 1);

  allocated = alloc->alloc_blocks(zone_size - 1, &start_block);
  bmap_test_assert(allocated == zone_size - 1);
  bmap_test_assert(start_block == 1);

  allocated = alloc->alloc_blocks(1, &start_block);
  bmap_test_assert(allocated == 1);

  allocated = alloc->alloc_blocks(zone_size, &start_block);
  bmap_test_assert(allocated == zone_size);
  bmap_test_assert(start_block == zone_size * 2);

  // Dis contiguous blocks allocations
  delete alloc;
  alloc = new BitAllocator(total_blocks, zone_size, CONCURRENT);

  int64_t blocks[2048] = {0};
  for (int64_t i = 0; i < alloc->size(); i++) {
    allocated = alloc->alloc_blocks(1, &start_block);
    bmap_test_assert(allocated == 1);
  }
  for (int i = 0; i < alloc->size(); i += 2) {
    alloc->free_blocks(i, 1);
  }

  allocated = alloc->alloc_blocks_dis(alloc->size()/2, blocks);
  bmap_test_assert(allocated == alloc->size() / 2);

  allocated = alloc->alloc_blocks_dis(1, blocks);
  bmap_test_assert(allocated == 0);

  alloc->free_blocks(alloc->size()/2, 1);
  allocated = alloc->alloc_blocks_dis(1, blocks);

  bmap_test_assert(allocated == 1);
  bmap_test_assert(blocks[0] == alloc->size()/2);

  alloc->free_blocks(0, alloc->size());
  delete alloc;

  // unaligned zones
  total_blocks = 1024 * 2 + 11;
  alloc = new BitAllocator(total_blocks, zone_size, CONCURRENT);

  for (int64_t iter = 0; iter < 4; iter++) {
    for (int64_t i = 0; i < total_blocks; i++) {
      allocated = alloc->alloc_blocks(1, &start_block);
      bmap_test_assert(allocated == 1);
      bmap_test_assert(start_block == i);
    }

    for (int64_t i = 0; i < total_blocks; i++) {
      alloc->free_blocks(i, 1);
    }
  }

  // Make three > 3 levels tree and check allocations and dealloc
  // in a loop
  int64_t alloc_size = 16;
  total_blocks = pow(BITMAP_SPAN_SIZE, 2) * 4;
  alloc = new BitAllocator(total_blocks, zone_size, CONCURRENT, false);
  for (int64_t iter = 0; iter < 3; iter++) {
    for (int64_t i = 0; i < total_blocks / alloc_size; i++) {
      allocated = alloc->alloc_blocks(alloc_size, &start_block);
      bmap_test_assert(allocated == alloc_size);
      bmap_test_assert(start_block == i * alloc_size);
    }

    for (int64_t i = 0; i < total_blocks / alloc_size; i++) {
      alloc->free_blocks(i * alloc_size, alloc_size);
    }
  }

  delete alloc;
  alloc = new BitAllocator(1024, zone_size, CONCURRENT, true);

  alloc->free_blocks(1, 1023);
  alloc->alloc_blocks(16, &start_block);
  delete alloc;

  total_blocks = pow(BITMAP_SPAN_SIZE, 2) * 4;
  alloc_size = 16;
  alloc = new BitAllocator(total_blocks, zone_size, CONCURRENT, false);
  for (int64_t iter = 0; iter < 3; iter++) {
    for (int64_t i = 0; i < total_blocks / alloc_size; i++) {
      bmap_test_assert(alloc->reserve_blocks(alloc_size));
      allocated = alloc->alloc_blocks_res(alloc_size, &start_block);
      bmap_test_assert(allocated == alloc_size);
      bmap_test_assert(start_block == i * alloc_size);
    }

    for (int64_t i = 0; i < total_blocks / alloc_size; i++) {
      alloc->free_blocks(i * alloc_size, alloc_size);
    }
  }

  delete alloc;
}

void
verify_blocks(int64_t num_blocks, int64_t *blocks)
{
  int64_t i = 0;
  int wraps = 0;
  for (i = 0; i < num_blocks - 1; i++) {
    if (blocks[i] > blocks[i + 1]) {
      wraps++;
      bmap_test_assert(wraps <= 1);
    }
  }
}

__thread int my_tid;

void
do_work(BitAllocator *alloc)
{
  int num_iters = 3;
  int64_t alloced = 0;
  int64_t start_block = -1;
  int64_t num_blocks = alloc->size() / NUM_THREADS;
  int total_alloced = 0;
  int64_t *allocated_blocks = (int64_t *) new int64_t [MAX_BLOCKS];

  while (num_iters--) {
    printf("Allocating in tid %d.\n", my_tid);
    for (int i = 0; i < num_blocks; i++) {
      alloced = alloc->alloc_blocks(1, &start_block);
      bmap_test_assert(alloced == 1);
      total_alloced++;
      allocated_blocks[i] = start_block;
    }

    printf("Freeing in tid %d %d blocks.\n", my_tid, total_alloced);
    for (int i = 0; i < num_blocks; i++) {
      alloc->free_blocks(allocated_blocks[i], 1);
    }

    total_alloced = 0;
    printf("Done tid %d iter %d.\n", my_tid, num_iters);
  }
}

int tid = 0;
void *
worker(void *args)
{
  my_tid = __sync_fetch_and_add(&tid, 1);
  BitAllocator *alloc = (BitAllocator *) args;
  printf("Starting thread %d", my_tid);
  do_work(alloc);
  return NULL;
}

TEST(BitAllocator, test_bmap_alloc_concurrent)
{
  int64_t total_blocks = MAX_BLOCKS;
  int64_t zone_size = 1024;
  pthread_t pthreads[NUM_THREADS] = {0};

  bmap_test_assert(total_blocks <= MAX_BLOCKS);

  BitAllocator *alloc = new BitAllocator(total_blocks, zone_size, CONCURRENT);
  printf("Spawning %d threads for parallel test.....\n", NUM_THREADS);

  for (int j = 0; j < NUM_THREADS; j++) {
    if (pthread_create(&pthreads[j], NULL, worker, alloc)) {
      printf("Unable to create worker thread.\n");
      exit(0);
    }
  }

  for (int j = 0; j < NUM_THREADS; j++) {
    pthread_join(pthreads[j], NULL);
  }

  // max_blks / num threads and free those. Make sure threads
  // always gets blocks
  // Do this with dis-contiguous and contiguous allocations
  // do multithreaded allocation and check allocations are unique
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
