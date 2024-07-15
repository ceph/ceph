// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/ceph_assert.h"

#include "crimson/os/seastore/collection_manager.h"
#include "crimson/os/seastore/collection_manager/collection_flat_node.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"

namespace crimson::os::seastore::collection_manager {

class FlatCollectionManager : public CollectionManager {
  TransactionManager &tm;

  coll_context_t get_coll_context(Transaction &t) {
    return coll_context_t{tm, t};
  }

  using get_root_iertr = base_iertr;
  using get_root_ret = get_root_iertr::future<CollectionNodeRef>;
  get_root_ret get_coll_root(const coll_root_t &coll_root, Transaction &t);

public:
  explicit FlatCollectionManager(TransactionManager &tm);

  mkfs_ret mkfs(Transaction &t) final;

  create_ret create(coll_root_t &coll_root, Transaction &t, coll_t cid,
                    coll_info_t info) final;

  list_ret list(const coll_root_t &coll_root, Transaction &t) final;

  remove_ret remove(const coll_root_t &coll_root, Transaction &t, coll_t cid) final;

  update_ret update(const coll_root_t &coll_root, Transaction &t, coll_t cid, coll_info_t info) final;
};
using FlatCollectionManagerRef = std::unique_ptr<FlatCollectionManager>;
}
