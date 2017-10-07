// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_AVLALLOCATOR_H
#define CEPH_OS_BLUESTORE_AVLALLOCATOR_H

#include <mutex>
#include "Allocator.h"
#include "avl.h"
#include "os/bluestore/bluestore_types.h"
#include "include/mempool.h"

typedef struct range_seg {
  MEMPOOL_CLASS_HELPERS();  ///< memory monitoring
  avl_node_t rs_node;	    ///< AVL node
  avl_node_t rs_pp_node;    ///< AVL picker-private node
  uint64_t   rs_start;	    ///< starting offset of this segment
  uint64_t   rs_end;	    ///< ending offset (non-inclusive)
} range_seg_t;

class AvlAllocator : public Allocator {
  CephContext* cct;
  std::mutex lock;

  avl_tree_t range_tree;    ///< main range tree
  /*
   * The range_size_tree should always contain the
   * same number of segments as the range_tree.
   * The only difference is that the range_size_tree
   * is ordered by segment sizes.
   */
  avl_tree_t range_size_tree;

  int64_t num_total = 0;    ///< device size
  int64_t num_free = 0;     ///< total bytes in freelist
  int64_t num_reserved = 0; ///< reserved bytes

  /*
   * This value defines the number of elements in the ms_lbas array.
   * The value of 64 was chosen as it covers all power of 2 buckets
   * up to UINT64_MAX.
   * This is the equivalent of highest-bit of UINT64_MAX.
   */
  static constexpr unsigned MAX_LBAS = 64;
  uint64_t lbas[MAX_LBAS] = {0};

  /*
   * Minimum size which forces the dynamic allocator to change
   * it's allocation strategy.  Once the allocator cannot satisfy
   * an allocation of this size then it switches to using more
   * aggressive strategy (i.e search by size rather than offset).
   */
  uint64_t range_size_alloc_threshold = 1ULL << 17; ///< 128K
  /*
   * The minimum free space, in percent, which must be available
   * in allocator to continue allocations in a first-fit fashion.
   * Once the allocator's free space drops below this level we dynamically
   * switch to using best-fit allocations.
   */
  int range_size_alloc_free_pct = 4;

  range_seg_t* _find_block(avl_tree_t *t, uint64_t start, uint64_t size);
  uint64_t _block_picker(avl_tree_t *t, uint64_t *cursor, uint64_t size,
    uint64_t align);
  void _add_to_tree(uint64_t start, uint64_t size);
  void _remove_from_tree(uint64_t start, uint64_t size);
  int _allocate(
    uint64_t size,
    uint64_t unit,
    uint64_t *offset,
    uint64_t *length);

public:
  AvlAllocator(CephContext* cct, int64_t device_size);
  int reserve(uint64_t need) override;
  void unreserve(uint64_t unused) override;
  int64_t allocate(
    uint64_t want,
    uint64_t unit,
    uint64_t max_alloc_size,
    int64_t  hint,
    mempool::bluestore_alloc::vector<AllocExtent> *extents) override;
  void release(const interval_set<uint64_t>& release_set) override;
  uint64_t get_free() override;
  void dump() override;
  void init_add_free(uint64_t offset, uint64_t length) override;
  void init_rm_free(uint64_t offset, uint64_t length) override;
  void shutdown() override;
};

#endif
