// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include "extent_allocator.h"
#include "include/ceph_assert.h"
#include "include/buffer_fwd.h"
#include "crimson/osd/exceptions.h"

#include "crimson/os/seastore/transaction.h"
#include <string.h>
#include "include/buffer.h"

#include <boost/intrusive/avl_set.hpp>
#include <optional>
#include <vector>

namespace crimson::os::seastore {

struct extent_range_t {
  rbm_abs_addr start;
  rbm_abs_addr end;

  extent_range_t(rbm_abs_addr start, rbm_abs_addr end) :
    start(start), end(end)
  {}

  struct before_t {
    template<typename KeyLeft, typename KeyRight>
    bool operator()(const KeyLeft& lhs, const KeyRight& rhs) const {
      return lhs.end <= rhs.start;
    }
  };
  boost::intrusive::avl_set_member_hook<> offset_hook;

  struct shorter_t {
    template<typename KeyType>
    bool operator()(const extent_range_t& lhs, const KeyType& rhs) const {
      auto lhs_size = lhs.length(); 
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

  size_t length() const {
    return end - start;
  }
  boost::intrusive::avl_set_member_hook<> size_hook;
};

/*
 * This is the simplest version of avlallocator from bluestore's avlallocator
 */
class AvlAllocator : public ExtentAllocator {
public:
  AvlAllocator(bool detailed) :
    detailed(detailed) {}
  std::optional<interval_set<rbm_abs_addr>> alloc_extent(
    size_t size) final;
  std::optional<interval_set<rbm_abs_addr>> alloc_extents(
    size_t size) final;

  void free_extent(rbm_abs_addr addr, size_t size) final;
  void mark_extent_used(rbm_abs_addr addr, size_t size) final;
  void init(rbm_abs_addr addr, size_t size, size_t b_size);

  struct dispose_rs {
    void operator()(extent_range_t* p)
    {
      delete p;
    }
  };

  ~AvlAllocator() {
    close();
  }

  void close() {
    if (!detailed) {
      assert(reserved_extent_tracker.size() == 0);
    }
    extent_size_tree.clear();
    extent_tree.clear_and_dispose(dispose_rs{});
    total_size = 0;
    block_size = 0;
    available_size = 0;
    base_addr = 0;
  }

  uint64_t get_available_size() const final {
    return available_size;
  }

  uint64_t get_max_alloc_size() const final {
    return max_alloc_size;
  }

  bool is_free_extent(rbm_abs_addr start, size_t size);

  void complete_allocation(rbm_abs_addr start, size_t size) final {
    if (detailed) {
      assert(reserved_extent_tracker.contains(start, size));
      reserved_extent_tracker.erase(start, size);
    }
  }

  bool is_reserved_extent(rbm_abs_addr start, size_t size) {
    if (detailed) {
      return reserved_extent_tracker.contains(start, size);
    } 
    return false;
  }

  rbm_extent_state_t get_extent_state(rbm_abs_addr addr, size_t size) final {
    if (is_reserved_extent(addr, size)) {
      return rbm_extent_state_t::RESERVED;
    } else if (is_free_extent(addr, size)) {
      return rbm_extent_state_t::FREE;
    }
    return rbm_extent_state_t::ALLOCATED;
  }

private:
  void _add_to_tree(rbm_abs_addr start, size_t size);

  void _extent_size_tree_rm(extent_range_t& r) {
    ceph_assert(available_size >= r.length());
    available_size -= r.length();
    extent_size_tree.erase(r);
  }

  void _extent_size_tree_try_insert(extent_range_t& r) {
    extent_size_tree.insert(r);
    available_size += r.length();
  }

  void _remove_from_tree(rbm_abs_addr start, rbm_abs_addr size);
  rbm_abs_addr find_block(size_t size);
  extent_len_t find_block(size_t size, rbm_abs_addr &start);

  using extent_tree_t = 
    boost::intrusive::avl_set<
      extent_range_t,
      boost::intrusive::compare<extent_range_t::before_t>,
      boost::intrusive::member_hook<
	extent_range_t,
	boost::intrusive::avl_set_member_hook<>,
	&extent_range_t::offset_hook>>;
  extent_tree_t extent_tree;    

  using extent_size_tree_t = 
    boost::intrusive::avl_set<
      extent_range_t,
      boost::intrusive::compare<extent_range_t::shorter_t>,
      boost::intrusive::member_hook<
	extent_range_t,
	boost::intrusive::avl_set_member_hook<>,
	&extent_range_t::size_hook>>;
  extent_size_tree_t extent_size_tree;

  uint64_t block_size = 0;
  uint64_t available_size = 0;
  uint64_t total_size = 0;
  uint64_t base_addr = 0;
  uint64_t max_alloc_size = 4 << 20;
  bool detailed;
  interval_set<rbm_abs_addr> reserved_extent_tracker; 
};

}
