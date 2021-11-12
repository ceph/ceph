// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <mutex>
#include "include/cpp-btree/btree_map.h"
#include "include/cpp-btree/btree_set.h"
#include "Allocator.h"
#include "os/bluestore/bluestore_types.h"
#include "include/mempool.h"

class BtreeAllocator : public Allocator {
  struct range_seg_t {
    uint64_t start;   ///< starting offset of this segment
    uint64_t end;     ///< ending offset (non-inclusive)

    range_seg_t(uint64_t start, uint64_t end)
      : start{start},
        end{end}
    {}
    inline uint64_t length() const {
      return end - start;
    }
  };

  struct range_value_t {
    uint64_t size;
    uint64_t start;
    range_value_t(uint64_t start, uint64_t end)
      : size{end - start},
        start{start}
    {}
    range_value_t(const range_seg_t& rs)
      : size{rs.length()},
        start{rs.start}
    {}
  };
  // do the radix sort
  struct compare_range_value_t {
    int operator()(const range_value_t& lhs,
                   const range_value_t& rhs) const noexcept {
      if (lhs.size < rhs.size) {
        return -1;
      } else if (lhs.size > rhs.size) {
        return 1;
      }
      if (lhs.start < rhs.start) {
        return -1;
      } else if (lhs.start > rhs.start) {
        return 1;
      } else {
        return 0;
      }
    }
  };
protected:
  /*
  * ctor intended for the usage from descendant class(es) which
  * provides handling for spilled over entries
  * (when entry count >= max_entries)
  */
  BtreeAllocator(CephContext* cct, int64_t device_size, int64_t block_size,
    uint64_t max_mem,
    std::string_view name);

public:
  BtreeAllocator(CephContext* cct, int64_t device_size, int64_t block_size,
                 std::string_view name);
  ~BtreeAllocator();
  const char* get_type() const override
  {
    return "btree";
  }
  int64_t allocate(
    uint64_t want,
    uint64_t unit,
    uint64_t max_alloc_size,
    int64_t  hint,
    PExtentVector *extents) override;
  void release(const interval_set<uint64_t>& release_set) override;
  uint64_t get_free() override;
  double get_fragmentation() override;

  void dump() override;
  void dump(std::function<void(uint64_t offset, uint64_t length)> notify) override;
  void init_add_free(uint64_t offset, uint64_t length) override;
  void init_rm_free(uint64_t offset, uint64_t length) override;
  void shutdown() override;

private:
  // pick a range by search from cursor forward
  uint64_t _pick_block_after(
    uint64_t *cursor,
    uint64_t size,
    uint64_t align);
  // pick a range with exactly the same size or larger
  uint64_t _pick_block_fits(
    uint64_t size,
    uint64_t align);
  int _allocate(
    uint64_t size,
    uint64_t unit,
    uint64_t *offset,
    uint64_t *length);

  template<class T>
  using pool_allocator = mempool::bluestore_alloc::pool_allocator<T>;
  using range_tree_t =
    btree::btree_map<
      uint64_t /* start */,
      uint64_t /* end */,
      std::less<uint64_t>,
      pool_allocator<std::pair<uint64_t, uint64_t>>>;
  range_tree_t range_tree;    ///< main range tree
  /*
   * The range_size_tree should always contain the
   * same number of segments as the range_tree.
   * The only difference is that the range_size_tree
   * is ordered by segment sizes.
   */
  using range_size_tree_t =
    btree::btree_set<
      range_value_t /* size, start */,
      compare_range_value_t,
      pool_allocator<range_value_t>>;
  range_size_tree_t range_size_tree;

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

  /*
  * Max amount of range entries allowed. 0 - unlimited
  */
  int64_t range_count_cap = 0;

private:
  CephContext* cct;
  std::mutex lock;

  double _get_fragmentation() const {
    auto free_blocks = p2align(num_free, (uint64_t)block_size) / block_size;
    if (free_blocks <= 1) {
      return .0;
    }
    return (static_cast<double>(range_tree.size() - 1) / (free_blocks - 1));
  }
  void _dump() const;

  uint64_t _lowest_size_available() const {
    auto rs = range_size_tree.begin();
    return rs != range_size_tree.end() ? rs->size : 0;
  }

  int64_t _allocate(
    uint64_t want,
    uint64_t unit,
    uint64_t max_alloc_size,
    int64_t  hint,
    PExtentVector *extents);

  void _release(const interval_set<uint64_t>& release_set);
  void _release(const PExtentVector&  release_set);
  void _shutdown();

  // called when extent to be released/marked free
  void _add_to_tree(uint64_t start, uint64_t size);
  void _process_range_removal(uint64_t start, uint64_t end, range_tree_t::iterator& rs);
  void _remove_from_tree(uint64_t start, uint64_t size);
  void _try_remove_from_tree(uint64_t start, uint64_t size,
    std::function<void(uint64_t offset, uint64_t length, bool found)> cb);

  uint64_t _get_free() const {
    return num_free;
  }
};
