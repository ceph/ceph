// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/future.hh>

#include "common/Formatter.h"
#include "include/ceph_assert.h"
#include "include/buffer_fwd.h"

#include "crimson/osd/exceptions.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"

#define PAGE_SIZE 4096
#define EXTMAP_BLOCK_SIZE 4096

namespace crimson::os::seastore {

struct lext_map_val_t {
  laddr_t laddr;
  extent_len_t lextent_offset = 0;
  extent_len_t length = 0;
  // other stuff: checksum,  refcount
  lext_map_val_t(
    laddr_t laddr,
    extent_len_t lextent_offset,
    extent_len_t length)
    : laddr(laddr), lextent_offset(lextent_offset), length(length) {}

};
class Extent
{
public:
  objaddr_t logical_offset = 0;  //offset in object
  laddr_t laddr;     // lextent start address aligned with block size.
  extent_len_t lextent_offset = 0;
  extent_len_t length = 0;
  /// ctor for lookup only
  explicit Extent(objaddr_t lo) : logical_offset(lo) { }
  /// ctor for delayed initialization (see decode_some())

  Extent(
    objaddr_t lo,
    laddr_t laddr,
    extent_len_t offset,
    extent_len_t length)
    : logical_offset(lo), laddr(laddr), lextent_offset(offset), length(length) {}

  void dump(ceph::Formatter* f) const;
    // comparators for intrusive_set
    friend bool operator<(const Extent &a, const Extent &b) {
      return a.logical_offset < b.logical_offset;
    }
    friend bool operator>(const Extent &a, const Extent &b) {
      return a.logical_offset > b.logical_offset;
    }
    friend bool operator==(const Extent &a, const Extent &b) {
      return a.logical_offset == b.logical_offset;
    }

  ~Extent() {}
};

using ExtentRef = std::unique_ptr<Extent>;
using extent_map_list_t = std::list<ExtentRef>;

struct extmap_root_t {
  depth_t depth = 0;
  laddr_t extmap_root_laddr;
  extmap_root_t(depth_t dep, laddr_t laddr)
  : depth(dep),
    extmap_root_laddr(laddr) {}
};

using extmap_root_ref = std::shared_ptr<extmap_root_t>;

/**
 * Abstract interface for managing the object inner offset to logical addr mapping
 * each onode has an extentmap tree.
 */
class ExtentMapManager {
public:
  using alloc_extmap_root_ertr = TransactionManager::alloc_extent_ertr;
  using alloc_extmap_root_ret = alloc_extmap_root_ertr::future<>;
  virtual alloc_extmap_root_ret alloc_extmap_root(Transaction &t) = 0;

  using seek_lextent_ertr = TransactionManager::read_extent_ertr;
  using seek_lextent_ret = seek_lextent_ertr::future<extent_map_list_t>;
  //seek to the first lextent including or after offset
  virtual seek_lextent_ret
    seek_lextent(Transaction &t, objaddr_t lo, extent_len_t len) = 0;

  //add a new Extent
  using add_lextent_ertr = TransactionManager::read_extent_ertr;
  using add_lextent_ret = add_lextent_ertr::future<ExtentRef>;
  virtual add_lextent_ret
    add_lextent(Transaction &t, objaddr_t lo, lext_map_val_t val) = 0;

  // remove (and delete) an Extent
  using rm_lextent_ertr = TransactionManager::read_extent_ertr;
  using rm_lextent_ret = rm_lextent_ertr::future<bool>;
  virtual rm_lextent_ret rm_lextent(Transaction &t, objaddr_t lo, lext_map_val_t val) = 0;

  //punch a logical hole.  add lextents to deref to target list.
  using punch_lextent_ertr = TransactionManager::read_extent_ertr;
  using punch_lextent_ret = punch_lextent_ertr::future<extent_map_list_t>;
  virtual punch_lextent_ret punch_lextent(Transaction &t, objaddr_t lo, extent_len_t len) = 0;

  //put new lextent into lextent_map overwriting existing ones if
  //any and update references accordingly
  using set_lextent_ertr = TransactionManager::read_extent_ertr;
  using set_lextent_ret = set_lextent_ertr::future<ExtentRef>;
  virtual set_lextent_ret set_lextent(Transaction &t, objaddr_t lo, lext_map_val_t val) = 0;

  //find a hole in mapping
  using find_hole_ertr = TransactionManager::read_extent_ertr;
  using find_hole_ret = find_hole_ertr::future<ExtentRef>;
  virtual find_hole_ret find_hole(Transaction &t, objaddr_t lo, extent_len_t len) = 0;

  using has_any_ertr = TransactionManager::read_extent_ertr;
  using has_any_ret = has_any_ertr::future<bool>;
  virtual has_any_ret has_any_lextents(Transaction &t, objaddr_t lo, extent_len_t length) = 0;

  /// consolidate adjacent lextents in extent_map
  using compress_ertr = TransactionManager::read_extent_ertr;;
  using compress_ret =  compress_ertr::future<int>;
  virtual compress_ret compress_extent_map(Transaction &t, objaddr_t lo, extent_len_t length) = 0;

  virtual ~ExtentMapManager() {}
};
using ExtentMapManagerRef = std::unique_ptr<ExtentMapManager>;

namespace extentmap_manager {
ExtentMapManagerRef create_extentmap_manager(
  TransactionManager *trans_manager);

ExtentMapManagerRef create_extentmap_manager(
  TransactionManager *trans_manager, extmap_root_ref exroot);

}

}
