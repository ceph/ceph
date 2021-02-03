// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#pragma once

#include <boost/iterator/counting_iterator.hpp>

#include "crimson/common/log.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/extentmap_manager.h"

namespace crimson::os::seastore::extentmap_manager{

struct ext_context_t {
  TransactionManager &tm;
  Transaction &t;
};

struct extmap_node_meta_t {
  depth_t depth = 0;

  std::pair<extmap_node_meta_t, extmap_node_meta_t> split_into(objaddr_t pivot) const {
    return std::make_pair(
           extmap_node_meta_t{depth},
           extmap_node_meta_t{depth});
  }

  static extmap_node_meta_t merge_from(
    const extmap_node_meta_t &lhs, const extmap_node_meta_t &rhs) {
    assert(lhs.depth == rhs.depth);
    return extmap_node_meta_t{lhs.depth};
  }

  static std::pair<extmap_node_meta_t, extmap_node_meta_t>
  rebalance(const extmap_node_meta_t &lhs, const extmap_node_meta_t &rhs, laddr_t pivot) {
    assert(lhs.depth == rhs.depth);
    return std::make_pair(
           extmap_node_meta_t{lhs.depth},
           extmap_node_meta_t{lhs.depth});
  }
};

struct ExtMapNode : LogicalCachedExtent {
  using ExtMapNodeRef = TCachedExtentRef<ExtMapNode>;

  ExtMapNode(ceph::bufferptr &&ptr) : LogicalCachedExtent(std::move(ptr)) {}
  ExtMapNode(const ExtMapNode &other)
  : LogicalCachedExtent(other) {}

  using find_lextent_ertr = ExtentMapManager::find_lextent_ertr;
  using find_lextent_ret = ExtentMapManager::find_lextent_ret;
  virtual find_lextent_ret find_lextent(ext_context_t ec,
		                        objaddr_t lo, extent_len_t len) = 0;

  using insert_ertr = TransactionManager::read_extent_ertr;
  using insert_ret = insert_ertr::future<extent_mapping_t>;
  virtual insert_ret insert(ext_context_t ec, objaddr_t lo, lext_map_val_t val) = 0;

  using rm_lextent_ertr = TransactionManager::read_extent_ertr;
  using rm_lextent_ret = rm_lextent_ertr::future<bool>;
  virtual rm_lextent_ret rm_lextent(ext_context_t ec, objaddr_t lo, lext_map_val_t val) = 0;

  using split_children_ertr = TransactionManager::alloc_extent_ertr;
  using split_children_ret = split_children_ertr::future
 	                           <std::tuple<ExtMapNodeRef, ExtMapNodeRef, uint32_t>>;
  virtual split_children_ret make_split_children(ext_context_t ec) = 0;

  using full_merge_ertr = TransactionManager::alloc_extent_ertr;
  using full_merge_ret = full_merge_ertr::future<ExtMapNodeRef>;
  virtual full_merge_ret make_full_merge(ext_context_t ec, ExtMapNodeRef right) = 0;

  using make_balanced_ertr = TransactionManager::alloc_extent_ertr;
  using make_balanced_ret = make_balanced_ertr::future
	                           <std::tuple<ExtMapNodeRef, ExtMapNodeRef, uint32_t>>;
  virtual make_balanced_ret
    make_balanced(ext_context_t ec, ExtMapNodeRef right, bool prefer_left) = 0;

  virtual extmap_node_meta_t get_node_meta() const = 0;

  virtual bool at_max_capacity() const = 0;
  virtual bool at_min_capacity() const = 0;
  virtual unsigned get_node_size() const = 0;
  virtual ~ExtMapNode() = default;

  using alloc_ertr = TransactionManager::alloc_extent_ertr;
  template<class T>
  alloc_ertr::future<TCachedExtentRef<T>>
  extmap_alloc_extent(ext_context_t ec, extent_len_t len) {
    return ec.tm.alloc_extent<T>(ec.t, L_ADDR_MIN, len).safe_then(
      [](auto&& extent) {
      return alloc_ertr::make_ready_future<TCachedExtentRef<T>>(std::move(extent));
    });
  }

  template<class T>
  alloc_ertr::future<std::pair<TCachedExtentRef<T>, TCachedExtentRef<T>>>
  extmap_alloc_2extents(ext_context_t ec, extent_len_t len) {
    return seastar::do_with(std::pair<TCachedExtentRef<T>, TCachedExtentRef<T>>(),
      [ec, len] (auto &extents) {
      return crimson::do_for_each(boost::make_counting_iterator(0),
                                  boost::make_counting_iterator(2),
                                  [ec, len, &extents] (auto i) {
        return ec.tm.alloc_extent<T>(ec.t, L_ADDR_MIN, len).safe_then(
          [i, &extents](auto &&node) {
	         if (i == 0)
	           extents.first = node;
	         if (i == 1)
	           extents.second = node;
	       });
      }).safe_then([&extents] {
        return alloc_ertr::make_ready_future
	         <std::pair<TCachedExtentRef<T>, TCachedExtentRef<T>>>(std::move(extents));
      });
    });
  }

  using retire_ertr = TransactionManager::ref_ertr;
  using retire_ret = retire_ertr::future<std::list<unsigned>>;
  retire_ret
  extmap_retire_node(ext_context_t ec, std::list<laddr_t> dec_laddrs) {
    return seastar::do_with(std::move(dec_laddrs), std::list<unsigned>(),
      [ec] (auto &&dec_laddrs, auto &refcnt) {
      return crimson::do_for_each(dec_laddrs.begin(), dec_laddrs.end(),
        [ec, &refcnt] (auto &laddr) {
        return ec.tm.dec_ref(ec.t, laddr).safe_then([&refcnt] (auto ref) {
          refcnt.push_back(ref);
        });
      }).safe_then([&refcnt] {
        return retire_ertr::make_ready_future<std::list<unsigned>>(std::move(refcnt));
      });
    });
  }

};

using ExtMapNodeRef = ExtMapNode::ExtMapNodeRef;

TransactionManager::read_extent_ertr::future<ExtMapNodeRef>
extmap_load_extent(ext_context_t ec, laddr_t laddr, depth_t depth);

}
