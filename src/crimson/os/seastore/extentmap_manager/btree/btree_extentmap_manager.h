// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
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
 */

class BtreeExtentMapManager : public ExtentMapManager {
  TransactionManager &tm;

  ext_context_t get_ext_context(Transaction &t) {
    return ext_context_t{tm,t};
  }

  /* get_extmap_root
   *
   * load extent map tree root node
   */
  using get_root_ertr = TransactionManager::read_extent_ertr;
  using get_root_ret = get_root_ertr::future<ExtMapNodeRef>;
  get_root_ret get_extmap_root(const extmap_root_t &extmap_root, Transaction &t);

  using insert_lextent_ertr = TransactionManager::read_extent_ertr;
  using insert_lextent_ret = insert_lextent_ertr::future<extent_mapping_t >;
  insert_lextent_ret insert_lextent(extmap_root_t &extmap_root, Transaction &t,
                                    ExtMapNodeRef extent, objaddr_t lo,
                                    lext_map_val_t val);

public:
  explicit BtreeExtentMapManager(TransactionManager &tm);

  initialize_extmap_ret initialize_extmap(Transaction &t) final;

  find_lextent_ret find_lextent(const extmap_root_t &extmap_root, Transaction &t, objaddr_t lo, extent_len_t len) final;

  add_lextent_ret add_lextent(extmap_root_t &extmap_root, Transaction &t, objaddr_t lo, lext_map_val_t val) final;

  rm_lextent_ret rm_lextent(extmap_root_t &extmap_root, Transaction &t, objaddr_t lo, lext_map_val_t val) final;


};
using BtreeExtentMapManagerRef = std::unique_ptr<BtreeExtentMapManager>;

}
