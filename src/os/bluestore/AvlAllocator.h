// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <mutex>
#include <boost/intrusive/avl_set.hpp>

#include "Allocator.h"
#include "AllocatorBase.h"
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
  inline uint64_t length() const {
    return end - start;
  }
  boost::intrusive::avl_set_member_hook<> size_hook;
};

class AvlAllocator : public AllocatorBase {
  struct dispose_rs {
    void operator()(range_seg_t* p)
    {
      delete p;
    }
  };

protected:
  /*
  * ctor intended for the usage from descendant class(es) which
  * provides handling for spilled over entries
  * (when entry count >= max_entries)
  */
  AvlAllocator(CephContext* cct, int64_t device_size, int64_t block_size,
    uint64_t max_mem,
    std::string_view name);

public:
  AvlAllocator(CephContext* cct, int64_t device_size, int64_t block_size,
	       std::string_view name);
  ~AvlAllocator();
  const char* get_type() const override
  {
    return "avl";
  }
  int64_t allocate(
    uint64_t want,
    uint64_t unit,
    uint64_t max_alloc_size,
    int64_t  hint,
    PExtentVector *extents) override;
  void release(const release_set_t& release_set) override;
  uint64_t get_free() override;
  double get_fragmentation() override;

  void dump() override;
  void foreach(
    std::function<void(uint64_t offset, uint64_t length)> notify) override;
  void init_add_free(uint64_t offset, uint64_t length) override;
  void init_rm_free(uint64_t offset, uint64_t length) override;
  void shutdown() override;

private:
  CephContext* cct;
  std::mutex lock;

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
	&range_seg_t::size_hook>,
      boost::intrusive::constant_time_size<true>>;
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
   * Maximum number of segments to check in the first-fit mode, without this
   * limit, fragmented device can see lots of iterations and _block_picker()
   * becomes the performance limiting factor on high-performance storage.
   */
  const uint32_t max_search_count;
  /*
   * Maximum distance to search forward from the last offset, without this
   * limit, fragmented device can see lots of iterations and _block_picker()
   * becomes the performance limiting factor on high-performance storage.
   */
  const uint32_t max_search_bytes;
  /*
  * Max amount of range entries allowed. 0 - unlimited
  */
  uint64_t range_count_cap = 0;

  void _range_size_tree_rm(range_seg_t& r) {
    ceph_assert(num_free >= r.length());
    num_free -= r.length();
    range_size_tree.erase(r);

  }
  void _range_size_tree_try_insert(range_seg_t& r) {
    if (_try_insert_range(r.start, r.end)) {
      range_size_tree.insert(r);
      num_free += r.length();
    } else {
      range_tree.erase_and_dispose(r, dispose_rs{});
    }
  }
  bool _try_insert_range(uint64_t start,
                         uint64_t end,
                        range_tree_t::iterator* insert_pos = nullptr) {
    bool res = !range_count_cap || range_size_tree.size() < range_count_cap;
    bool remove_lowest = false;
    if (!res) {
      if (end - start > _lowest_size_available()) {
        remove_lowest = true;
        res = true;
      }
    }
    if (!res) {
      _spillover_range(start, end);
    } else {
      // NB:  we should do insertion before the following removal
      // to avoid potential iterator disposal insertion might depend on.
      if (insert_pos) {
        auto new_rs = new range_seg_t{ start, end };
        range_tree.insert_before(*insert_pos, *new_rs);
        range_size_tree.insert(*new_rs);
        num_free += new_rs->length();
      }
      if (remove_lowest) {
        auto r = range_size_tree.begin();
        _range_size_tree_rm(*r);
        _spillover_range(r->start, r->end);
        range_tree.erase_and_dispose(*r, dispose_rs{});
      }
    }
    return res;
  }
  virtual void _spillover_range(uint64_t start, uint64_t end) {
    // this should be overriden when range count cap is present,
    // i.e. (range_count_cap > 0)
    ceph_assert(false);
  }
  // to be overriden by Hybrid wrapper
  virtual uint64_t _get_spilled_over() const {
    return 0;
  }
  virtual uint64_t _spillover_allocate(uint64_t want,
                                      uint64_t unit,
                                      uint64_t max_alloc_size,
                                      int64_t  hint,
                                      PExtentVector* extents) {
    // this should be overriden when range count cap is present,
    // i.e. (range_count_cap > 0)
    ceph_assert(false);
    return 0;
  }

protected:
  // called when extent to be released/marked free
  virtual void _add_to_tree(uint64_t start, uint64_t size);

  CephContext* get_context() { return cct; }

  std::mutex& get_lock() {
    return lock;
  }

  double _get_fragmentation() const {
    auto free_blocks = p2align(num_free, (uint64_t)block_size) / block_size;
    if (free_blocks <= 1) {
      return .0;
    }
    return (static_cast<double>(range_tree.size() - 1) / (free_blocks - 1));
  }
  void _dump() const;
  void _foreach(std::function<void(uint64_t offset, uint64_t length)>) const;

  uint64_t _lowest_size_available() {
    auto rs = range_size_tree.begin();
    return rs != range_size_tree.end() ?
      rs->length() :
      std::numeric_limits<uint64_t>::max();
  }

  int64_t _allocate(
    uint64_t want,
    uint64_t unit,
    uint64_t max_alloc_size,
    int64_t  hint,
    PExtentVector *extents);

  void _release(const release_set_t& release_set);
  void _release(const PExtentVector&  release_set);
  void _shutdown();

  void _process_range_removal(uint64_t start, uint64_t end, range_tree_t::iterator& rs);
  void _remove_from_tree(uint64_t start, uint64_t size);
  void _try_remove_from_tree(uint64_t start, uint64_t size,
    std::function<void(uint64_t offset, uint64_t length, bool found)> cb);

  uint64_t _get_free() const {
    return num_free;
  }
};
