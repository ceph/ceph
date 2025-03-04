// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <mutex>

#include "include/cpp-btree/btree_map.h"
#include "include/cpp-btree/btree_set.h"

#include "Allocator.h"
#include "AllocatorBase.h"

#include "os/bluestore/bluestore_types.h"
#include "include/mempool.h"
#include "common/ceph_mutex.h"

/*
 * class Btree2Allocator
 *
 *
 */
class Btree2Allocator : public AllocatorBase {
  enum {
    RANGE_SIZE_BUCKET_COUNT = 14,
  };
  const LenPartitionedSetTraitsPow2 myTraits;

public:
  // Making public to share with mempools
  struct range_seg_t {
    MEMPOOL_CLASS_HELPERS();  ///< memory monitoring
    uint64_t start;           ///< starting offset of this segment
    uint64_t end;	      ///< ending offset (non-inclusive)

    // Tree is sorted by offset, greater offsets at the end of the tree.
    struct before_t {
      template<typename KeyLeft, typename KeyRight>
      bool operator()(const KeyLeft& lhs, const KeyRight& rhs) const {
        return lhs.end <= rhs.start;
      }
    };

    // Tree is sorted by size, larger chunks are at the end of the tree.
    struct shorter_t {
      template<typename KeyType1, typename KeyType2>
      int compare(const KeyType1& lhs, const KeyType2& rhs) const {
        int64_t delta =
          int64_t(lhs.end) - int64_t(lhs.start) - int64_t(rhs.end) + int64_t(rhs.start);
        if (delta == 0) {
          delta = (int64_t)lhs.start - (int64_t)rhs.start;
        }
        return delta ? delta / std::abs(delta) : 0;
      }
      template<typename KeyType>
      int operator()(const range_seg_t& lhs, const KeyType& rhs) const {
        return compare(lhs, rhs);
      }
    };

    range_seg_t(uint64_t start = 0, uint64_t end = 0)
      : start{ start }, end{ end }
    {}
    inline uint64_t length() const {
      return end - start;
    }
  };

public:
  Btree2Allocator(CephContext* cct, int64_t device_size, int64_t block_size,
    uint64_t max_mem,
    double _rweight_factor,
    bool with_cache,
    std::string_view name);

  //
  //ctor intended for the usage from descendant (aka hybrid) class(es)
  // which provide handling for spilled over entries
  //
  Btree2Allocator(CephContext* cct, int64_t device_size, int64_t block_size,
    uint64_t max_mem,
    std::string_view name) :
    Btree2Allocator(cct, device_size, block_size, max_mem, 1.0, true, name) {
  }

  ~Btree2Allocator() override {
    shutdown();
  }

  const char* get_type() const override
  {
    return "btree_v2";
  }
  void init_add_free(uint64_t offset, uint64_t length) override;
  void init_rm_free(uint64_t offset, uint64_t length) override;

  int64_t allocate(
    uint64_t want,
    uint64_t unit,
    uint64_t max_alloc_size,
    int64_t  hint,
    PExtentVector* extents) override;

  void release(const release_set_t& release_set) override;

  uint64_t get_free() override {
    return num_free;
  }
  double get_fragmentation() override {
    std::lock_guard l(lock);
    return _get_fragmentation();
  }
  size_t get_cache_hit_count() const {
    return cache ? cache->get_hit_count() : 0;
  }

  void dump() override {
    std::lock_guard l(lock);
    _dump();
  }
  void foreach(
      std::function<void(uint64_t offset, uint64_t length)> notify) override {
    std::lock_guard l(lock);
    _foreach(notify);
  }
  void shutdown() override {
    std::lock_guard l(lock);
    _shutdown();
  }

private:
  CephContext* cct = nullptr;
  AllocatorBase::OpportunisticExtentCache* cache = nullptr;
  std::mutex lock;

  template<class T>
  using pool_allocator = mempool::bluestore_alloc::pool_allocator<T>;
  using range_tree_t =
    btree::btree_map<
      uint64_t, // start
      uint64_t, // end
      std::less<uint64_t>,
      pool_allocator<std::pair<uint64_t, uint64_t>>>;
  using range_tree_iterator = range_tree_t::iterator;
  range_tree_t range_tree;    ///< main range tree

  //
  // The range_size_tree should always contain the
  // same number of segments as the range_tree.
  // The only difference is that the range_size_tree
  // is ordered by segment sizes.
  //
  using range_size_tree_t =
    btree::btree_set<
      range_seg_t,
      range_seg_t::shorter_t,
      pool_allocator<range_seg_t>>;
  std::vector<range_size_tree_t> range_size_set;

  std::atomic<uint64_t> num_free = 0;     ///< total bytes in freelist

  //
  // Max amount of range entries allowed. 0 - unlimited
  //
  uint64_t range_count_cap = 0;

  const uint64_t weight_center = 1ull << 20; // 1M
  uint64_t lsum = 0;
  uint64_t rsum = 0;
  double rweight_factor = 0;
  uint64_t left_weight() const {
    return lsum + _get_spilled_over();
  }
  uint64_t right_weight() const {
    return rsum * rweight_factor;
  }
protected:
  static const uint64_t pextent_array_size = 64;
  typedef std::array <const release_set_t::value_type*, pextent_array_size> PExtentArray;

  void set_weight_factor(double _rweight_factor) {
    rweight_factor = _rweight_factor;
  }

  CephContext* get_context() {
    return cct;
  }
  std::mutex& get_lock() {
    return lock;
  }
  range_size_tree_t* _get_lowest(range_size_tree_t::iterator* rs_p) {
    for (auto& t : range_size_set) {
      if (t.begin() != t.end()) {
        *rs_p = t.begin();
        return &t;
      }
    }
    return nullptr;
  }
  uint64_t _lowest_size_available() {
    range_size_tree_t::iterator rs_p;
    if (_get_lowest(&rs_p) != nullptr) {
      return rs_p->length();
    }
    return std::numeric_limits<uint64_t>::max();
  }
  uint64_t _get_free() const {
    return num_free;
  }
  double _get_fragmentation() const {
    auto free_blocks = p2align(num_free.load(), (uint64_t)block_size) / block_size;
    if (free_blocks <= 1) {
      return .0;
    }
    return (static_cast<double>(range_tree.size() - 1) / (free_blocks - 1));
  }

  void _shutdown();

  void _dump(bool full = true) const;
  void _foreach(std::function<void(uint64_t offset, uint64_t length)>);

  int64_t _allocate(
    uint64_t want,
    uint64_t unit,
    uint64_t max_alloc_size,
    int64_t  hint,
    PExtentVector* extents);

  void _release(const release_set_t& release_set);
  void _release(const PExtentVector& release_set);
  void _release(size_t count, const release_set_entry_t** to_release);

  /*
   * overridables for HybridAllocator
   */
   // called when extent to be released/marked free
  virtual void _add_to_tree(uint64_t start, uint64_t size);
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

  /*
   * Used exclusively from HybridAllocator
   */
  void _try_remove_from_tree(uint64_t start, uint64_t size,
    std::function<void(uint64_t offset, uint64_t length, bool found)> cb);
  bool has_cache() const {
    return !!cache;
  }
  bool try_put_cache(uint64_t start, uint64_t len) {
    bool ret = cache && cache->try_put(start, len);
    if (ret) {
      num_free += len;
    }
    return ret;
  }
  bool try_get_from_cache(uint64_t* res_offset, uint64_t want) {
    bool ret = cache && cache->try_get(res_offset, want);
    if (ret) {
      num_free -= want;
    }
    return ret;
  }

private:
  int64_t __allocate(size_t bucket0,
    uint64_t size,
    uint64_t unit,
    PExtentVector* extents);

  inline range_size_tree_t::iterator _pick_block(int distance,
    range_size_tree_t* tree, uint64_t size);

  inline void _remove_from_tree(uint64_t start, uint64_t size);
  inline range_tree_iterator _remove_from_tree(range_tree_iterator rt_p,
    uint64_t start, uint64_t end);
  inline range_tree_iterator _remove_from_tree(range_size_tree_t* rs_tree,
    range_size_tree_t::iterator rs,
    range_tree_iterator rt_p,
    uint64_t start, uint64_t end);

  inline void _try_insert_range(const range_seg_t& rs);
  inline bool __try_insert_range(const range_seg_t& rs,
    range_tree_iterator* insert_pos);

  inline void _range_size_tree_add(const range_seg_t& r);
  inline void _range_size_tree_rm(const range_seg_t& rs);
  inline void _range_size_tree_rm(range_size_tree_t* rs_tree,
    range_size_tree_t::iterator rs);
};
