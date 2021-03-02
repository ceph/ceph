// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>

#include "osd/osd_types.h"

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"

namespace crimson::os::seastore {

struct coll_info_t {
  unsigned split_bits;
  
  coll_info_t(unsigned bits)
    : split_bits(bits) {}
  
  bool operator==(const coll_info_t &rhs) const {
    return split_bits == rhs.split_bits;
  }
};

/// Interface for maintaining set of collections
class CollectionManager {
public:
  using base_ertr = TransactionManager::read_extent_ertr;
    /// Initialize collection manager instance for an empty store
  using mkfs_ertr = TransactionManager::alloc_extent_ertr;
  using mkfs_ret = mkfs_ertr::future<coll_root_t>;
  virtual mkfs_ret mkfs(
    Transaction &t) = 0;

  /// Create collection
  using create_ertr = base_ertr;
  using create_ret = create_ertr::future<>;
  virtual create_ret create(
    coll_root_t &root,
    Transaction &t,
    coll_t cid,
    coll_info_t info
  ) = 0;

  /// List collections with info
  using list_ertr = base_ertr;
  using list_ret_bare = std::vector<std::pair<coll_t, coll_info_t>>;
  using list_ret = list_ertr::future<list_ret_bare>;
  virtual list_ret list(
    const coll_root_t &root,
    Transaction &t) = 0;

  /// Remove cid
  using remove_ertr = base_ertr;
  using remove_ret = remove_ertr::future<>;
  virtual remove_ret remove(
    const coll_root_t &coll_root,
    Transaction &t,
    coll_t cid) = 0;

  /// Update info for cid
  using update_ertr = base_ertr;
  using update_ret = base_ertr::future<>;
  virtual update_ret update(
    const coll_root_t &coll_root,
    Transaction &t,
    coll_t cid,
    coll_info_t info
  ) = 0;

  virtual ~CollectionManager() {}
};
using CollectionManagerRef = std::unique_ptr<CollectionManager>;

namespace collection_manager {
/* creat CollectionMapManager for Collection  */
CollectionManagerRef create_coll_manager(
  TransactionManager &trans_manager);

}

}
