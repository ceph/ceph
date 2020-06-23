// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer_fwd.h"
#include "crimson/osd/exceptions.h"

#include "crimson/os/seastore/extentmap_manager.h"
#include "crimson/os/seastore/extentmap_manager/btree/extentmap_btree_node.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"

namespace crimson::os::seastore::extentmap_manager {
/**
 * BtreeExtentMapManager
 *
 * Uses a btree to track :
 * objaddr_t -> laddr_t mapping for each onode extentmap
 *
 */

class BtreeExtentMapManager : public ExtentMapManager {
  TransactionManager *tm = nullptr;
  extmap_root_ref extmap_root = nullptr;

  using get_root_ertr = TransactionManager::read_extent_ertr;
  using get_root_ret = get_root_ertr::future<ExtMapNodeRef>;
  get_root_ret get_extmap_root(Transaction &t);

  using insert_lextent_ertr = TransactionManager::read_extent_ertr;
  using insert_lextent_ret = insert_lextent_ertr::future<ExtentRef>;
  insert_lextent_ret insert_lextent(Transaction &t,
                                    ExtMapNodeRef extent, uint32_t lo,
				    lext_map_val_t val);

public:
  explicit BtreeExtentMapManager(
    TransactionManager *tm, extmap_root_ref extmap_root);

  explicit BtreeExtentMapManager(TransactionManager *tm);

  alloc_extmap_root_ret alloc_extmap_root(Transaction &t) final;

  //seek to the first lextent including or after offset
  seek_lextent_ret seek_lextent(Transaction &t, uint32_t lo, uint32_t len) final;

  add_lextent_ret add_lextent(Transaction &t, uint32_t lo, lext_map_val_t val) final;

  /// remove (and delete) an Extent
  rm_lextent_ret rm_lextent(Transaction &t, uint32_t lo, lext_map_val_t val) final;

  punch_lextent_ret punch_lextent(Transaction &t, uint32_t lo, uint32_t len) final;

  set_lextent_ret
    set_lextent(Transaction &t, uint32_t lo, lext_map_val_t val) final;

  find_hole_ret find_hole(Transaction &t, uint32_t lo, uint32_t len) final;

  has_any_ret
    has_any_lextents(Transaction &t, uint32_t offset, uint32_t length)final;

  /// consolidate adjacent lextents in extent_map
  compress_ret
    compress_extent_map(Transaction &t, uint32_t offset, uint32_t length) final
    {//TODO
      return compress_ertr::make_ready_future<int>(0);
    }

};
using BtreeExtentMapManagerRef = std::unique_ptr<BtreeExtentMapManager>;

}


