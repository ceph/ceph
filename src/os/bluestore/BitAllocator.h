// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Bitmap based in memory allocator.
 * Author: Ramesh Chander, Ramesh.Chander@sandisk.com
 */

#ifndef  CEPH_OS_BLUESTORE_BITALLOCATOR_H
#define CEPH_OS_BLUESTORE_BITALLOCATOR_H

#define debug_assert assert
#define BITMAP_SPAN_SIZE (1024)

#include <assert.h>
#include <stdint.h>
#include <pthread.h>
#include <mutex>
#include <atomic>
#include <vector>
#include "include/intarith.h"

class BitAllocatorStats {
public:
  std::atomic<int64_t> m_total_alloc_calls;
  std::atomic<int64_t> m_total_free_calls;
  std::atomic<int64_t> m_total_allocated;
  std::atomic<int64_t> m_total_freed;
  std::atomic<int64_t> m_total_serial_scans;
  std::atomic<int64_t> m_total_concurrent_scans;
  std::atomic<int64_t> m_total_node_scanned;

  BitAllocatorStats() {
    m_total_alloc_calls = 0;
    m_total_free_calls = 0;
    m_total_allocated = 0;
    m_total_freed = 0;
    m_total_serial_scans = 0;
    m_total_concurrent_scans = 0;
    m_total_node_scanned = 0;
  }

  void add_alloc_calls(int64_t val) {
    std::atomic_fetch_add(&m_total_alloc_calls, val);
  }
  void add_free_calls(int64_t val) {
    std::atomic_fetch_add(&m_total_free_calls, val);
  }
  void add_allocated(int64_t val) {
    std::atomic_fetch_add(&m_total_allocated, val);
  }
  void add_freed(int64_t val) {
    std::atomic_fetch_add(&m_total_freed, val);
  }
  void add_serial_scans(int64_t val) {
    std::atomic_fetch_add(&m_total_serial_scans, val);
  }
  void add_concurrent_scans(int64_t val) {
    std::atomic_fetch_add(&m_total_concurrent_scans, val);
  }
  void add_node_scanned(int64_t val) {
    std::atomic_fetch_add(&m_total_node_scanned, val);
  }
};

template <class BitMapEntity>
class BitMapEntityIter {
  std::vector<BitMapEntity> *m_list;
  int64_t m_start_idx;
  int64_t m_cur_idx;
  bool m_wrap;
  bool m_wrapped;
  bool m_end;
public:

  void init(std::vector<BitMapEntity> *list, bool wrap, int64_t start_idx) {
    m_list = list;
    m_wrap = wrap;
    m_start_idx = start_idx;
    m_cur_idx = m_start_idx;
    m_wrapped = false;
    m_end = false;
  }

  BitMapEntityIter(std::vector<BitMapEntity> *list, int64_t start_idx) {
    init(list, false, start_idx);
  }
  BitMapEntityIter(std::vector<BitMapEntity> *list, int64_t start_idx, bool wrap) {
    init(list, wrap, start_idx);
  }

  BitMapEntity *next() {
    int64_t cur_idx = m_cur_idx;

    if (m_wrapped &&
      cur_idx == m_start_idx) {
      /*
       * End of wrap cycle + 1
       */
      if (!m_end) {
        m_end = true;
        return &(*m_list)[cur_idx];
      }
      return NULL;
    }
    m_cur_idx++;

    if (m_cur_idx == (int64_t)m_list->size() &&
        m_wrap) {
      m_cur_idx = 0;
      m_wrapped = true;
    }

    if (cur_idx == (int64_t)m_list->size()) {
      /*
       * End of list
       */
      return NULL;
    }

    debug_assert(cur_idx < (int64_t)m_list->size());
    return &(*m_list)[cur_idx];
  }

  int64_t index() {
    return m_cur_idx;
  }
  void decr_idx() {
    m_cur_idx--;
    debug_assert(m_cur_idx >= 0);
  }
};

typedef unsigned long bmap_t;

class BmapEntry {

private:
  bmap_t m_bits;

public:
  static bmap_t full_bmask();
  static int64_t size();
  static bmap_t empty_bmask();
  static bmap_t align_mask(int x);
  bmap_t bit_mask(int bit_num);
  bmap_t atomic_fetch();
  BmapEntry(bool val);
  BmapEntry() {
    m_bits = 0;
  }
  BmapEntry(const BmapEntry& bmap) {
    bmap_t i = bmap.m_bits;
    m_bits = i;
  }

  void clear_bit(int bit);
  void clear_bits(int offset, int num_bits);
  void set_bits(int offset, int num_bits);
  bool check_n_set_bit(int bit);
  bool check_bit(int bit);
  bool is_allocated(int64_t start_bit, int64_t num_bits);

  int find_n_cont_bits(int start_offset, int64_t num_bits);
  int find_n_free_bits(int start_idx, int64_t max_bits,
           int *free_bit, int *end_idx);
  int find_first_set_bits(int64_t required_blocks, int bit_offset,
          int *start_offset, int64_t *scanned);

  int find_any_free_bits(int start_offset, int64_t num_blocks,
        int64_t *alloc_list, int64_t block_offset,
        int64_t *scanned);

  ~BmapEntry();

};

typedef enum bmap_area_type {
  ZONE = 1,
  LEAF = 2,
  NON_LEAF = 3
} bmap_area_type_t;

class BitMapArea {

protected:
  int16_t m_area_index;
  bmap_area_type_t m_type;

public:
  static int64_t get_span_size();
  bmap_area_type_t level_to_type(int level);
  static int get_level(int64_t total_blocks);
  virtual bool is_allocated(int64_t start_block, int64_t num_blocks) = 0;
  virtual bool is_allocated(int64_t *blocks, int64_t num_blocks, int blk_off) {
    debug_assert(0);
    return true;
  }
  virtual bool is_exhausted() = 0;
  virtual bool child_check_n_lock(BitMapArea *child, int64_t required) {
      debug_assert(0);
      return true;
  }
  virtual bool child_check_n_lock(BitMapArea *child, int64_t required, bool lock) {
      debug_assert(0);
      return true;
  }
  virtual void child_unlock(BitMapArea *child) {
    debug_assert(0);
  }

  virtual void lock_excl() = 0;
  virtual bool lock_excl_try() {
    debug_assert(0);
    return false;
  }
  virtual void lock_shared() {
    debug_assert(0);
    return;
  }
  virtual void unlock() = 0;

  virtual int64_t sub_used_blocks(int64_t num_blocks) = 0;
  virtual int64_t add_used_blocks(int64_t num_blocks) = 0;
  virtual bool reserve_blocks(int64_t num_blocks) = 0;
  virtual void unreserve(int64_t num_blocks, int64_t allocated) = 0;
  virtual int64_t get_reserved_blocks() = 0;
  virtual int64_t get_used_blocks() = 0;

  virtual void shutdown() = 0;
  virtual int64_t alloc_blocks(bool wait, int64_t num_blocks, int64_t *start_block) {
    debug_assert(0);
    return 0;
  }
  virtual int64_t alloc_blocks(int64_t num_blocks, int64_t *start_block) {
    debug_assert(0);
    return 0;
  }

  virtual int64_t alloc_blocks_dis(bool wait, int64_t num_blocks,
             int64_t blk_off, int64_t *block_list) {
    debug_assert(0);
    return 0;
  }
  virtual int64_t alloc_blocks_dis(int64_t num_blocks,
                         int64_t blk_offset, int64_t *block_list) {
    debug_assert(0);
    return 0;
  }
  virtual void set_blocks_used(int64_t start_block, int64_t num_blocks) = 0;
  virtual void free_blocks(int64_t start_block, int64_t num_blocks) = 0;
  virtual int64_t size() = 0;

  int64_t child_count();
  int64_t get_index();
  int64_t get_level();
  bmap_area_type_t get_type();
  virtual ~BitMapArea() { }
};

class BitMapAreaList {

private:
  BitMapArea **m_items;
  int64_t m_num_items;
  std::mutex m_marker_mutex;

public:
  BitMapArea *get_nth_item(int64_t idx) {
    return m_items[idx];
  }

   BitMapArea ** get_item_list() {
    return m_items;
  }

  int64_t size() {
    return m_num_items;
  }
  BitMapAreaList(BitMapArea **list, int64_t len);
  BitMapAreaList(BitMapArea **list, int64_t len, int64_t marker);

  BitMapArea **get_list() {
    return m_items;
  }
};

class BmapEntityListIter {
  BitMapAreaList *m_list;
  int64_t m_start_idx;
  int64_t m_cur_idx;
  bool m_wrap;
  bool m_wrapped;
  bool m_end;
public:

  void init(BitMapAreaList *list, int64_t start_idx, bool wrap);
  BmapEntityListIter(BitMapAreaList *list);

  BmapEntityListIter(BitMapAreaList *list, bool wrap);

  BmapEntityListIter(BitMapAreaList *list, int64_t start_idx);

  BmapEntityListIter(BitMapAreaList *list, int64_t start_idx, bool wrap);

  BitMapArea *next();
  int64_t index();
  void decr_idx();
};

class BitMapZone: public BitMapArea{

private:
  std::atomic<int32_t> m_used_blocks;
  std::vector <BmapEntry> *m_bmap_list;
  std::mutex m_lock;

public:
  static int64_t count;
  static int64_t total_blocks;
  static void incr_count() { count++;}
  static int64_t get_total_blocks() {return total_blocks;}
  bool is_allocated(int64_t start_block, int64_t num_blocks);
  bool is_exhausted();
  void reset_marker();

  int64_t sub_used_blocks(int64_t num_blocks);
  int64_t add_used_blocks(int64_t num_blocks);
  bool reserve_blocks(int64_t num_blocks);
  void unreserve(int64_t num_blocks, int64_t allocated);
  int64_t get_reserved_blocks();
  int64_t get_used_blocks();
  int64_t size() {
    return get_total_blocks();
  }

  void lock_excl();
  bool lock_excl_try();
  void unlock();
  bool check_locked();

  int64_t alloc_cont_bits(int64_t num_blocks,
       BitMapEntityIter<BmapEntry> *iter, int64_t *bmap_out_idx);
  void free_blocks_int(int64_t start_block, int64_t num_blocks);
  void init(int64_t zone_num, int64_t total_blocks, bool def);

  BitMapZone(int64_t total_blocks, int64_t zone_num);
  BitMapZone(int64_t total_blocks, int64_t zone_num, bool def);

  ~BitMapZone();
  void shutdown();

  int64_t alloc_blocks(int64_t num_blocks, int64_t *start_block);
  int64_t alloc_blocks_dis(int64_t num_blocks, int64_t blk_off, int64_t *block_list);
  void set_blocks_used(int64_t start_block, int64_t num_blocks);

  void free_blocks(int64_t start_block, int64_t num_blocks);
};

class BitMapAreaIN: public BitMapArea{

protected:
  int64_t m_child_size_blocks;
  int64_t m_total_blocks;
  int16_t m_level;
  int16_t m_num_child;

  int64_t m_used_blocks;
  int64_t m_reserved_blocks;
  std::mutex m_blocks_lock;
  BitMapAreaList *m_child_list;

  bool is_allocated(int64_t start_block, int64_t num_blocks);
  bool is_allocated(int64_t *blocks, int64_t num_blocks, int64_t blk_off);
  virtual bool is_exhausted();
  virtual bool child_check_n_lock(BitMapArea *child, int64_t required);
  virtual void child_unlock(BitMapArea *child);

  virtual void lock_excl() {
    return;
  }
  virtual void lock_shared() {
    return;
  }
  virtual void unlock() {
    return;
  }

  void init(int64_t total_blocks, int64_t zone_size_block, bool def);
  void init_common(int64_t total_blocks, int64_t zone_size_block, bool def);

public:
  BitMapAreaIN();
  BitMapAreaIN(int64_t zone_num, int64_t total_blocks);
  BitMapAreaIN(int64_t zone_num, int64_t total_blocks, bool def);

  virtual ~BitMapAreaIN();
  void shutdown();
  virtual int64_t sub_used_blocks(int64_t num_blocks);
  virtual int64_t add_used_blocks(int64_t num_blocks);
  virtual bool reserve_blocks(int64_t num_blocks);
  virtual void unreserve(int64_t num_blocks, int64_t allocated);
  virtual int64_t get_reserved_blocks();
  virtual int64_t get_used_blocks();
  virtual int64_t size() {
    return m_total_blocks;
  }

  virtual int64_t alloc_blocks_int(bool wait, bool wrap,
                     int64_t num_blocks, int64_t *start_block);
  virtual int64_t alloc_blocks(bool wait, int64_t num_blocks, int64_t *start_block);
  virtual int64_t alloc_blocks_dis_int(bool wait, int64_t num_blocks,
               int64_t blk_off, int64_t *block_list);
  virtual int64_t alloc_blocks_dis(bool wait, int64_t num_blocks,
             int64_t blk_off, int64_t *block_list);
  virtual void set_blocks_used_int(int64_t start_block, int64_t num_blocks);
  virtual void set_blocks_used(int64_t start_block, int64_t num_blocks);

  virtual void free_blocks_int(int64_t start_block, int64_t num_blocks);
  virtual void free_blocks(int64_t start_block, int64_t num_blocks);
};

class BitMapAreaLeaf: public BitMapAreaIN{

private:
  void init(int64_t total_blocks, int64_t zone_size_block,
            bool def);

public:
  static int64_t count;
  static void incr_count() { count++;}
  BitMapAreaLeaf() { }
  BitMapAreaLeaf(int64_t zone_num, int64_t total_blocks);
  BitMapAreaLeaf(int64_t zone_num, int64_t total_blocks, bool def);
  bool child_check_n_lock(BitMapArea *child, int64_t required, bool lock);
  void child_unlock(BitMapArea *child);

  int64_t alloc_blocks_int(bool wait, bool wrap,
                         int64_t num_blocks, int64_t *start_block);
  int64_t alloc_blocks_dis_int(bool wait, int64_t num_blocks,
                               int64_t blk_off, int64_t *block_list);
  void free_blocks_int(int64_t start_block, int64_t num_blocks);

  virtual ~BitMapAreaLeaf();
};


typedef enum bmap_alloc_mode {
  SERIAL = 1,
  CONCURRENT = 2,
} bmap_alloc_mode_t;

class BitAllocator:public BitMapAreaIN{
private:
  bmap_alloc_mode_t m_alloc_mode;
  std::mutex m_serial_mutex;
  pthread_rwlock_t m_rw_lock;
  BitAllocatorStats *m_stats;
  bool m_is_stats_on;

  int64_t truncated_blocks; //see init_check

  bool is_stats_on() {
    return m_is_stats_on;
  }

  bool child_check_n_lock(BitMapArea *child, int64_t required);
  virtual void child_unlock(BitMapArea *child);

  void serial_lock();
  void serial_unlock();
  void lock_excl();
  void lock_shared();
  void unlock();

  bool check_input(int64_t num_blocks);
  bool check_input_dis(int64_t num_blocks);
  void init_check(int64_t total_blocks, int64_t zone_size_block,
                 bmap_alloc_mode_t mode, bool def, bool stats_on);

public:

  BitAllocator(int64_t total_blocks, int64_t zone_size_block, bmap_alloc_mode_t mode);
  BitAllocator(int64_t total_blocks, int64_t zone_size_block, bmap_alloc_mode_t mode, bool def);
  BitAllocator(int64_t total_blocks, int64_t zone_size_block,
               bmap_alloc_mode_t mode, bool def, bool stats_on);
  ~BitAllocator();
  void shutdown();
  int64_t alloc_blocks(int64_t num_blocks, int64_t *start_block);
  int64_t alloc_blocks_res(int64_t num_blocks, int64_t *start_block);
  void free_blocks(int64_t start_block, int64_t num_blocks);
  void set_blocks_used(int64_t start_block, int64_t num_blocks);
  void unreserve_blocks(int64_t blocks);

  int64_t alloc_blocks_dis(int64_t num_blocks, int64_t *block_list);
  void free_blocks_dis(int64_t num_blocks, int64_t *block_list);

  int64_t get_truncated_blocks() { return truncated_blocks; }
  BitAllocatorStats *get_stats() {
      return m_stats;
  }
};

#endif //End of file
