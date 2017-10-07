// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <mutex>
#include <boost/intrusive/avl_set.hpp>

#include "Allocator.h"
#include "os/bluestore/bluestore_types.h"
#include "include/mempool.h"

struct range_seg_t {
  MEMPOOL_CLASS_HELPERS();  ///< memory monitoring
  uint64_t start;   ///< starting offset of this segment
  uint64_t end;	    ///< ending offset (non-inclusive)

  range_seg_t(uint64_t start, uint64_t end)
    : start{start},
      end{end}
  {}
  // Tree is sorted by offset, greater offsets at the end of the tree.
  struct before_t {
    template<typename KeyLeft, typename KeyRight>
    bool operator()(const KeyLeft& lhs, const KeyRight& rhs) const {
      return lhs.end <= rhs.start;
    }
  };
  boost::intrusive::avl_set_member_hook<> offset_hook;

  // Tree is sorted by size, larger sizes at the end of the tree.
  struct shorter_t {
    template<typename KeyType>
    bool operator()(const range_seg_t& lhs, const KeyType& rhs) const {
      auto lhs_size = lhs.end - lhs.start;
      auto rhs_size = rhs.end - rhs.start;
      if (lhs_size < rhs_size) {
	return true;
      } else if (lhs_size > rhs_size) {
	return false;
      } else {
	return lhs.start < rhs.start;
      }
    }
  };
  boost::intrusive::avl_set_member_hook<> size_hook;
};

class AvlAllocator final : public Allocator {
public:
  AvlAllocator(CephContext* cct, int64_t device_size, int64_t block_size,
	       const std::string& name);
  int64_t allocate(
    uint64_t want,
    uint64_t unit,
    uint64_t max_alloc_size,
    int64_t  hint,
    PExtentVector *extents) final;
  void release(const interval_set<uint64_t>& release_set) final;
  uint64_t get_free() final;
  double get_fragmentation() final;

  void dump() final;
  void dump(std::function<void(uint64_t offset, uint64_t length)> notify) final;
  void init_add_free(uint64_t offset, uint64_t length) final;
  void init_rm_free(uint64_t offset, uint64_t length) final;
  void shutdown() final;

private:
  template<class Tree>
  uint64_t _block_picker(const Tree& t, uint64_t *cursor, uint64_t size,
    uint64_t align);
  void _add_to_tree(uint64_t start, uint64_t size);
  void _remove_from_tree(uint64_t start, uint64_t size);
  int _allocate(
    uint64_t size,
    uint64_t unit,
    uint64_t *offset,
    uint64_t *length);

  using range_tree_t = 
    boost::intrusive::avl_set<
      range_seg_t,
      boost::intrusive::compare<range_seg_t::before_t>,
      boost::intrusive::member_hook<
	range_seg_t,
	boost::intrusive::avl_set_member_hook<>,
	&range_seg_t::offset_hook>>;
  range_tree_t range_tree;    ///< main range tree
  /*
   * The range_size_tree should always contain the
   * same number of segments as the range_tree.
   * The only difference is that the range_size_tree
   * is ordered by segment sizes.
   */
  using range_size_tree_t =
    boost::intrusive::avl_multiset<
      range_seg_t,
      boost::intrusive::compare<range_seg_t::shorter_t>,
      boost::intrusive::member_hook<
	range_seg_t,
	boost::intrusive::avl_set_member_hook<>,
	&range_seg_t::size_hook>>;
  range_size_tree_t range_size_tree;

  const int64_t num_total;   ///< device size
  const uint64_t block_size; ///< block size
  uint64_t num_free = 0;     ///< total bytes in freelist

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
  uint64_t range_size_alloc_threshold = 0;
  /*
   * The minimum free space, in percent, which must be available
   * in allocator to continue allocations in a first-fit fashion.
   * Once the allocator's free space drops below this level we dynamically
   * switch to using best-fit allocations.
   */
  int range_size_alloc_free_pct = 0;

  CephContext* cct;
  std::mutex lock;
};
