// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Bitmap based in memory allocator.
 * Author: Ramesh Chander, Ramesh.Chander@sandisk.com
 */

#ifndef  CEPH_OS_BLUESTORE_BITALLOCATOR_H
#define CEPH_OS_BLUESTORE_BITALLOCATOR_H

#include <stdint.h>
#include <pthread.h>
#include <mutex>
#include <atomic>

class BmapEntity {
public:
  static int64_t size();
  virtual int64_t get_index() { return -1; }
  virtual bool is_allocated(int64_t start, int64_t num) = 0;

  virtual ~BmapEntity() { }
};


class BmapEntityList {
  BmapEntity **m_items;
  int64_t m_num_items;

public:

  BmapEntityList(BmapEntity **list, int64_t len) {
    m_items = list;
    m_num_items = len;
  }

  virtual ~BmapEntityList() { }

  BmapEntity *get_nth_item(int64_t idx) {
    return m_items[idx];
  }

  BmapEntity** get_item_list() {
    return m_items;
  }

  virtual int64_t incr_marker(int64_t add) = 0;
  virtual int64_t get_marker() = 0;
  virtual void set_marker(int64_t val) = 0;

  int64_t size() {
    return m_num_items;
  }
};


class BmapEntityListIter {
  BmapEntityList *m_list;
  int64_t m_start_idx;
  int64_t m_cur_idx;
  bool m_wrap;
  bool m_wrapped;
public:

  BmapEntityListIter(BmapEntityList *list);

  BmapEntityListIter(BmapEntityList *list, bool wrap);

  BmapEntityListIter(BmapEntityList *list, int64_t start_idx);

  BmapEntityListIter(BmapEntityList *list, int64_t start_idx, bool wrap);

  BmapEntity *next();
  int64_t index();
  void decr_idx();
};

typedef unsigned long bmap_t;

class BmapEntry: public BmapEntity {

private:
 std::atomic<bmap_t> m_bits;
public:
  static bmap_t full_bmask();
  static int64_t size();
  static bmap_t empty_bmask();
  static bmap_t align_mask(int x);
  bmap_t bit_mask(int bit_num);
  bmap_t atomic_fetch();
  BmapEntry(bool val);

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

  virtual ~BmapEntry();

};

class BmapList: public BmapEntityList {

  std::atomic<int64_t> m_marker;
public:
  BmapList(BmapEntity **bmaps, int len, int64_t marker):
    BmapEntityList(bmaps, len) {
    m_marker = marker;
  }

  int64_t incr_marker(int64_t add);
  void set_marker(int64_t val);
  int64_t get_marker();
};


class BitMapZone: public BmapEntity {

private:
  int64_t m_total_blocks;
  int64_t m_zone_num;
  std::atomic<int64_t> m_used_blocks;

  BmapEntityList *m_bmap_list;

  enum {ZONE_FREE = 0, ZONE_ACTIVE = 1} m_state;
  std::mutex m_lock;

  int64_t alloc_cont_bits(int64_t num_blocks, BmapEntityListIter *iter, int64_t *bmap_out_idx);
  void free_blocks_int(int64_t start_block, int64_t num_blocks);
  void init(int64_t zone_num, int64_t total_blocks, bool def);

public:
  BitMapZone(int64_t zone_num, int64_t total_blocks);
  BitMapZone(int64_t zone_num, int64_t total_blocks, bool def);
  bool lock_zone(bool wait);
  void unlock_zone();

  int64_t get_used_blocks();

  int64_t add_used_blocks(int64_t blks);
  int64_t sub_used_blocks(int64_t blks);

  int64_t alloc_blocks(int64_t num_blocks, int64_t *start_block);
  void set_blocks_used(int64_t start_block, int64_t num_blocks);
  void free_blocks(int64_t start_block, int64_t num_blocks);

  int64_t alloc_blocks_dis(int64_t num_blocks, int64_t *allocated_blocks);

  bool is_exhausted();
  bool is_allocated(int64_t stat_block, int64_t num_blocks);
  int64_t get_index();
  int64_t size();
  void reset_marker();

  virtual ~BitMapZone();
};

typedef enum bmap_alloc_mode {
  SERIAL = 1,
  CONCURRENT = 2,
} bmap_alloc_mode_t;


class ZoneList : public BmapEntityList {

private:
  int64_t m_marker;
  std::mutex m_marker_mutex;

public:

  ZoneList(BmapEntity **list, int64_t len);
  ZoneList(BmapEntity **list, int64_t len, int64_t marker);
  int64_t incr_marker(int64_t add);
  int64_t get_marker();
  void set_marker(int64_t val);

};

class BitAllocator {
private:
  bmap_alloc_mode_t m_alloc_mode;
  int64_t m_zone_size_blocks;
  int64_t m_total_zones;

  int64_t m_total_blocks;
  std::atomic<int64_t> m_allocated_blocks;
  int64_t m_reserved_blocks;
  std::mutex m_res_blocks_lock;
  BmapEntityList *m_zone_list;

  enum {ALLOC_DESTROY = 0, ALLOC_ACTIVE = 1} m_state;

  std::mutex m_serial_mutex;
  pthread_rwlock_t m_alloc_slow_lock;

  bool is_allocated(int64_t start_block, int64_t num_blocks);
  bool is_allocated(int64_t *blocks, int64_t num_blocks);

  int64_t alloc_blocks_int(int64_t num_blocks, int64_t *start_block, bool lock);
  int64_t alloc_blocks_dis_int(int64_t num_blocks, int64_t *block_list, bool lock);
  void free_blocks_int(int64_t start_block, int64_t num_blocks);
  bool zone_free_to_alloc(BmapEntity *zone, int64_t required, bool lock);

  void serial_lock();
  void serial_unlock();

  void alloc_lock(bool write);
  void alloc_unlock();
  bool check_input(int64_t num_blocks);
  bool check_input_dis(int64_t num_blocks);
  void unreserve(int64_t needed, int64_t allocated);
  void init(int64_t total_blocks, int64_t zone_size_block, bmap_alloc_mode_t mode, bool def);

public:

  BitAllocator(int64_t total_blocks, int64_t zone_size_block, bmap_alloc_mode_t mode);
  BitAllocator(int64_t total_blocks, int64_t zone_size_block, bmap_alloc_mode_t mode, bool def);
  ~BitAllocator();
  void shutdown();
  int64_t alloc_blocks(int64_t num_blocks, int64_t *start_block);
  int64_t alloc_blocks_res(int64_t num_blocks, int64_t *start_block);
  void free_blocks(int64_t start_block, int64_t num_blocks);
  void set_blocks_used(int64_t start_block, int64_t num_blocks);
  int64_t sub_used_blocks(int64_t num_blocks);
  int64_t add_used_blocks(int64_t num_blocks);
  int64_t get_used_blocks();
  int64_t get_reserved_blocks();
  int64_t size();
  bool reserve_blocks(int64_t blocks);
  void unreserve_blocks(int64_t blocks);

  int64_t alloc_blocks_dis(int64_t num_blocks, int64_t *block_list);
  void free_blocks_dis(int64_t num_blocks, int64_t *block_list);
};

#endif //End of file
