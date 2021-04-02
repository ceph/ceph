// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "include/buffer_fwd.h"
#include "include/ceph_assert.h"
#include "common/hobject.h"

#include "crimson/os/seastore/onode.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/osd/exceptions.h"

namespace crimson::os::seastore {

class OnodeManager {
  using base_ertr = TransactionManager::base_ertr;
public:
  using mkfs_ertr = TransactionManager::mkfs_ertr;
  using mkfs_ret = mkfs_ertr::future<>;
  virtual mkfs_ret mkfs(Transaction &t) = 0;

  using get_onode_ertr = base_ertr::extend<
    crimson::ct_error::enoent>;
  using get_onode_ret = get_onode_ertr::future<
    OnodeRef>;
  virtual get_onode_ret get_onode(
    Transaction &trans,
    const ghobject_t &hoid) {
    return seastar::make_ready_future<OnodeRef>();
  }

  using get_or_create_onode_ertr = base_ertr;
  using get_or_create_onode_ret = get_or_create_onode_ertr::future<
    OnodeRef>;
  virtual get_or_create_onode_ret get_or_create_onode(
    Transaction &trans,
    const ghobject_t &hoid) {
    return seastar::make_ready_future<OnodeRef>();
  }

  using get_or_create_onodes_ertr = base_ertr;
  using get_or_create_onodes_ret = get_or_create_onodes_ertr::future<
    std::vector<OnodeRef>>;
  virtual get_or_create_onodes_ret get_or_create_onodes(
    Transaction &trans,
    const std::vector<ghobject_t> &hoids) {
    return seastar::make_ready_future<std::vector<OnodeRef>>();
  }

  using write_dirty_ertr = base_ertr;
  using write_dirty_ret = write_dirty_ertr::future<>;
  virtual write_dirty_ret write_dirty(
    Transaction &trans,
    const std::vector<OnodeRef> &onodes) {
    return seastar::now();
  }
  virtual ~OnodeManager() {}
};
using OnodeManagerRef = std::unique_ptr<OnodeManager>;

}
