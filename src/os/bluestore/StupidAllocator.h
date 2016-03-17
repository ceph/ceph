// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_STUPIDALLOCATOR_H
#define CEPH_OS_BLUESTORE_STUPIDALLOCATOR_H

#include <mutex>

#include "Allocator.h"
#include "include/btree_interval_set.h"

class StupidAllocator : public Allocator {
  std::mutex lock;

  int64_t num_free;     ///< total bytes in freelist
  int64_t num_uncommitted;
  int64_t num_committing;
  int64_t num_reserved; ///< reserved bytes

  std::vector<btree_interval_set<uint64_t> > free;        ///< leading-edge copy
  btree_interval_set<uint64_t> uncommitted; ///< released but not yet usable
  btree_interval_set<uint64_t> committing;  ///< released but not yet usable

  uint64_t last_alloc;

  unsigned _choose_bin(uint64_t len);
  void _insert_free(uint64_t offset, uint64_t len);

public:
  StupidAllocator();
  ~StupidAllocator();

  int reserve(uint64_t need);
  void unreserve(uint64_t unused);

  int allocate(
    uint64_t want_size, uint64_t alloc_unit, int64_t hint,
    uint64_t *offset, uint32_t *length);

  int release(
    uint64_t offset, uint64_t length);

  void commit_start();
  void commit_finish();

  uint64_t get_free();

  void dump(std::ostream& out);

  void init_add_free(uint64_t offset, uint64_t length);
  void init_rm_free(uint64_t offset, uint64_t length);

  void shutdown();
};

#endif
