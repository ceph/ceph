// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Bitmap based in-memory allocator unit test cases.
 * Author: Ramesh Chander, Ramesh.Chander@sandisk.com
 */

#include "os/bluestore/BitAllocator.h"
#include <stdio.h>
#include <assert.h>
#define debug_assert assert
#define NUM_THREADS  16
#define MAX_BLOCKS (1024 * 1024 * 4)

void test_bmap_iter()
{
  int num_items = 5;
  int off = 2;
  class BmapEntityTmp : public BmapEntity {
      int64_t m_num;
      int64_t m_len;
    public:
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

  class BmapEntityListTmp: public BmapEntityList {
    private:
      int64_t m_marker;
    public:
    BmapEntityListTmp(BmapEntity **bmaps, int len, int64_t marker):
      BmapEntityList(bmaps, len) {
      m_marker = marker;
    }

    int64_t incr_marker(int64_t add)
    {
      return m_marker++;
    }
    void set_marker(int64_t val)
    {
      m_marker = val;
    }
    int64_t get_marker()
    {
      return m_marker;
    }

  };

  int i = 0;
  BmapEntityTmp *obj = NULL;

  BmapEntity **bmap = new BmapEntity*[num_items];
  for (i = 0; i < num_items; i++) {
    bmap[i] = new BmapEntityTmp(i);
  }

  BmapEntityList *list = new BmapEntityListTmp(bmap, num_items, 0);
  BmapEntityListIter *iter = new BmapEntityListIter(list, off, false);

  i = off;
  int count = 0;
  int64_t last_idx = off;
  while ((obj = (BmapEntityTmp*) iter->next())) {
    debug_assert(obj->get_index() == last_idx);
    debug_assert(obj->get_index() == i);
    debug_assert(obj == bmap[i]);
    last_idx = iter->index();
    i++;
    count++;
  }
  debug_assert(i == num_items);
  debug_assert(count == num_items - off);

  delete iter;

  iter = new BmapEntityListIter(list, off, true);

  i = off;
  last_idx = off;
  count = 0;
  while ((obj = (BmapEntityTmp*) iter->next())) {
    debug_assert(obj->get_index() == last_idx);
    debug_assert(obj->get_index() == i);
    debug_assert(obj == bmap[i]);
    last_idx = iter->index();


    i = (i + 1) % num_items;
    count++;
  }
  debug_assert(i == off);
  debug_assert(count == num_items);


  num_items = 4;
  off = num_items - 1;

  //list = new BmapEntityList(bmap, num_items);
  list = new BmapEntityListTmp(bmap, num_items, 0);

  iter = new BmapEntityListIter(list, off, true);
  i = off;
  last_idx = off;
  count = 0;
  while ((obj = (BmapEntityTmp*) iter->next())) {
    debug_assert(obj->get_index() == last_idx);
    debug_assert(obj->get_index() == i);
    debug_assert(obj == bmap[i]);
    last_idx = iter->index();
    i = (i + 1) % num_items;
    count++;
  }
  debug_assert(i == off);
  debug_assert(count == num_items);
}

void test_bmap_entry()
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
    debug_assert(!bmap->check_bit(i));
  }

  // Set all bits again using set_bits
  bmap->set_bits(0, size);

  // clear 4 bits at a time and then check allocated
  for (i = 0; i < size/4; i++) {
    bmap->clear_bits(i * 4, 4);
    debug_assert(!bmap->is_allocated(i * 4, 4));
  }

  // set all bits again
  bmap->set_bits(0, size);

  // clear alternate bits, check and set those bits
  for (i = 0; i < size/2; i++) {
    bmap->clear_bit(i * 2 + 1);
    debug_assert(!bmap->check_bit(i * 2 + 1));
    debug_assert(bmap->check_n_set_bit(i * 2 + 1));
  }

  // free 1, 2 and size bits at a time and try to find n cont bits
  for (i = 0; i < size / 4; i++) {
    bmap->clear_bits(i * 2 + 1, i + 1);
    debug_assert(!bmap->check_bit(i * 2 + 1));
    debug_assert(bmap->find_n_cont_bits(i * 2 + 1, i + 1) ==
        i + 1);
  }

  // free 1, 2 and size bits at a time and try to find any cont bits
  for (i = 0; i < size / 4; i++) {
    bmap->clear_bits(i * 2 + 1, i + 1);
    debug_assert(!bmap->is_allocated(i * 2 + 1, i + 1));
  }

  for (i = 0; i < size / 4; i++) {
    bmap->clear_bits(i * 2 + 1, i + 1);
    allocated = bmap->find_first_set_bits(i + 1, 0, &start, &scanned);

    debug_assert(allocated == i + 1);
    debug_assert(scanned == ((i * 2 + 1) + (i + 1)));
    debug_assert(start == i * 2 + 1);
    bmap->set_bits(0, BmapEntry::size());

  }

  // Find few bits at end of bitmap and find those
  bmap->clear_bits(0, 4);
  bmap->clear_bits(BmapEntry::size() - 12, 5);
  bmap->clear_bits(BmapEntry::size() - 6, 6);
  allocated = bmap->find_first_set_bits(6, 0, &start, &scanned);

  debug_assert(allocated == 6);
  debug_assert(scanned == BmapEntry::size() - 6 + 6);
  debug_assert(start == BmapEntry::size() - 6);
  debug_assert(bmap->is_allocated(start, 6));

  delete bmap;
  bmap = new BmapEntry(false);
  bmap->set_bits(4, BmapEntry::size() - 4);
  debug_assert(bmap->is_allocated(4, BmapEntry::size() - 4));
  debug_assert(!bmap->is_allocated(0, 4));
  bmap->set_bits(0, 4);
  debug_assert(bmap->is_allocated(0, BmapEntry::size()));


}

void test_zone_alloc()
{
  int total_blocks = 1024;
  int64_t blks = 1;
  int64_t last_blk = -1;
  int64_t start_blk = 0;
  int64_t allocated = 0;

  BitMapZone *zone = new BitMapZone(0, total_blocks);

  // Allocate all blocks and see that it is allocating in order.
  for (int i = 0; i < total_blocks; i++) {
    allocated = zone->alloc_blocks(blks, &start_blk);
    debug_assert(last_blk + 1 == start_blk);
    debug_assert(allocated == blks);
    last_blk = start_blk;
  }
  debug_assert(zone->get_used_blocks() == total_blocks);

  for (int i = 0; i < total_blocks; i++) {
    debug_assert(zone->get_used_blocks() == total_blocks - i);
    zone->free_blocks(i, blks);
  }

  blks = 2;
  last_blk = -2;
  debug_assert(zone->is_exhausted());
  for (int i = 0; i < total_blocks/2; i++) {
    allocated = zone->alloc_blocks(blks, &start_blk);
    debug_assert(last_blk + 2 == start_blk);
    last_blk = start_blk;
  }

  // Free different boundaries and allocate those
  blks = 3;
  debug_assert(zone->is_exhausted());
  zone->free_blocks(BmapEntry::size() - blks, blks);
  zone->free_blocks(BmapEntry::size(), blks);

  allocated = zone->alloc_blocks(blks * 2, &start_blk);
  debug_assert(BmapEntry::size() - blks == start_blk);
  debug_assert(allocated == blks * 2);

  blks = 4;
  zone->free_blocks(BmapEntry::size() * 2 - blks, 2 * blks);
  allocated = zone->alloc_blocks(2 * blks, &start_blk);
  debug_assert(BmapEntry::size() * 2 - blks == start_blk);
  debug_assert(allocated == blks * 2);

  zone->reset_marker();
  blks = BmapEntry::size() * 2;
  zone->free_blocks(BmapEntry::size() * 6 - blks, blks);
  allocated = zone->alloc_blocks(blks, &start_blk);
  debug_assert(BmapEntry::size() * 6 - blks == start_blk);

  // free blocks at distance 1, 2 up to 63 and allocate all of them
  // together using disc alloc.
  zone->reset_marker();
  blks = BmapEntry::size() * 2;
  int num_bits = 1;
  for (int i = 0; i < zone->size() / BmapEntry::size() -1; i++) {

    zone->free_blocks(i * BmapEntry::size(), num_bits);
    num_bits++;
  }

  zone->reset_marker();
  num_bits = 1;
  int64_t start_block = 0;
  for (int i = 0; i < zone->size() / BmapEntry::size() -1; i++) {
    allocated = zone->alloc_blocks(num_bits, &start_block);
    debug_assert(num_bits == allocated);
    debug_assert(start_block == i * BmapEntry::size());
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
  zone = new BitMapZone(0, total_blocks);
  int64_t blocks[1024] = {0};
  for (int i = 0; i < zone->size(); i++) {
    allocated = zone->alloc_blocks(1, &start_block);
    debug_assert(allocated == 1);
  }
  for (int i = 0; i < zone->size(); i += 2) {
    zone->free_blocks(i, 1);
  }

  zone->reset_marker();
  allocated = zone->alloc_blocks_dis(zone->size() / 2, blocks);
  debug_assert(allocated == zone->size() / 2);
}

void test_bitmap_alloc() {
  int64_t total_blocks = 1024 * 4;
  int64_t zone_size = 1024;
  int64_t allocated = 0;
  int64_t start_block = 0;

  BitAllocator *alloc = new BitAllocator(total_blocks, zone_size, CONCURRENT);

  for (int64_t iter = 0; iter < 4; iter++) {
    for (int64_t i = 0; i < total_blocks; i++) {
      allocated = alloc->alloc_blocks(1, &start_block);
      debug_assert(allocated == 1);
      debug_assert(start_block == i);
    }

    for (int64_t i = 0; i < total_blocks; i++) {
      alloc->free_blocks(i, 1);
    }
  }

  for (int64_t iter = 0; iter < 4; iter++) {
    for (int64_t i = 0; i < total_blocks / zone_size; i++) {
      allocated = alloc->alloc_blocks(zone_size, &start_block);
      debug_assert(allocated == zone_size);
      debug_assert(start_block == i * zone_size);
    }

    for (int64_t i = 0; i < total_blocks / zone_size; i++) {
      alloc->free_blocks(i * zone_size, zone_size);
    }
  }

  allocated = alloc->alloc_blocks(1, &start_block);
  debug_assert(allocated == 1);

  allocated = alloc->alloc_blocks(zone_size - 1, &start_block);
  debug_assert(allocated == zone_size - 1);
  debug_assert(start_block == 1);

  allocated = alloc->alloc_blocks(1, &start_block);
  debug_assert(allocated == 1);

  allocated = alloc->alloc_blocks(zone_size, &start_block);
  debug_assert(allocated == zone_size);
  debug_assert(start_block == zone_size * 2);

  // Dis contiguous blocks allocations
  delete alloc;
  alloc = new BitAllocator(total_blocks, zone_size, CONCURRENT);

  int64_t blocks[2048] = {0};
  for (int64_t i = 0; i < alloc->size(); i++) {
    allocated = alloc->alloc_blocks(1, &start_block);
    debug_assert(allocated == 1);
  }
  for (int i = 0; i < alloc->size(); i += 2) {
    alloc->free_blocks(i, 1);
  }

  allocated = alloc->alloc_blocks_dis(alloc->size()/2, blocks);
  debug_assert(allocated == alloc->size() / 2);

  allocated = alloc->alloc_blocks_dis(1, blocks);
  debug_assert(allocated == 0);

  alloc->free_blocks(alloc->size() / 2, 1);
  allocated = alloc->alloc_blocks_dis(1, blocks);

  debug_assert(allocated == 1);
  debug_assert(blocks[0] == alloc->size()/2);

  delete alloc;
  alloc = new BitAllocator(1024, zone_size, CONCURRENT, true);

  alloc->free_blocks(1, 1023);
  alloc->alloc_blocks(16, &start_block);
}

void
verify_blocks(int64_t num_blocks, int64_t *blocks)
{
  int64_t i = 0;
  int wraps = 0;
  for (i = 0; i < num_blocks - 1; i++) {
    if (blocks[i] > blocks[i + 1]) {
      wraps++;
      debug_assert(wraps <= 1);
    }
  }
}

__thread int my_tid;
__thread int64_t allocated_blocks[MAX_BLOCKS];

void
do_work(BitAllocator *alloc)
{
  int num_iters = 10;
  int64_t alloced = 0;
  int64_t start_block = -1;
  uint64_t alloc_unit = 1;
  int64_t num_blocks = alloc->size() / NUM_THREADS;
  int total_alloced = 0;

  while (num_iters--) {
    printf("Allocating in tid %d.\n", my_tid);
    for (int i = 0; i < num_blocks; i++) {
      alloced = alloc->alloc_blocks(1, &start_block);
      debug_assert(alloced == 1);
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

void test_bmap_alloc_concurrent()
{

  // Create an allocator
  int64_t total_blocks = MAX_BLOCKS;
  int64_t zone_size = 1024;
  int64_t allocated = 0;
  int64_t start_block = 0;
  pthread_t pthreads[NUM_THREADS] = {0};

  debug_assert(total_blocks <= MAX_BLOCKS);

  BitAllocator *alloc = new BitAllocator(total_blocks, zone_size, CONCURRENT);

  // Create N threads and each thread allocates at max
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

int main()
{
  test_bmap_entry();
  test_bmap_iter();
  test_zone_alloc();
  test_bitmap_alloc();
  test_bmap_alloc_concurrent();


  printf("All tests done : SUCCESS.\n");
}
