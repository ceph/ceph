// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <seastar/core/future.hh>

#include "osd/osd_types.h"

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/logical_child_node.h"

namespace crimson::os::seastore {

struct coll_info_t {
  unsigned split_bits;
  // root of this collection's onode tree (see FLTreeOnodeManager)
  laddr_t onode_root;

  coll_info_t(unsigned bits, laddr_t onode_root)
    : split_bits(bits), onode_root(onode_root) {}

  bool operator==(const coll_info_t &rhs) const {
    if (split_bits == rhs.split_bits) {
      // two coll_infos with the same split_bits must belong to the same
      // collection and therefore share the same onode tree root
      assert(onode_root == rhs.onode_root);
      return true;
    }
    return false;
  }
};

namespace collection_manager {
struct coll_context_t {
  TransactionManager &tm;
  Transaction &t;
};

struct coll_value_t {
  uint32_t bits = 0;
  laddr_t onode_root = L_ADDR_NULL;

  DENC(coll_value_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.bits, p);
    denc(v.onode_root, p);
    DENC_FINISH(p);
  }
};
}
}
WRITE_CLASS_DENC(crimson::os::seastore::collection_manager::coll_value_t)

namespace crimson::os::seastore {

/// Interface for a collection's persisted onode-tree root node.
struct CollectionNode : LogicalChildNode {
  using CollectionNodeRef = TCachedExtentRef<CollectionNode>;

  explicit CollectionNode(ceph::bufferptr &&ptr)
    : LogicalChildNode(std::move(ptr)) {}
  explicit CollectionNode(extent_len_t length)
    : LogicalChildNode(length) {}

  virtual const collection_manager::coll_value_t &get_value(coll_t cid) const = 0;
  virtual bool contains(coll_t cid) const = 0;
  virtual void update_value(
    collection_manager::coll_context_t cc, coll_t coll,
    collection_manager::coll_value_t value) = 0;
};
using CollectionNodeRef = CollectionNode::CollectionNodeRef;

/// Interface for maintaining set of collections
class CollectionManager {
public:
  using base_iertr = TransactionManager::read_extent_iertr;

    /// Initialize collection manager instance for an empty store
  using mkfs_iertr = TransactionManager::alloc_extent_iertr;
  using mkfs_ret = mkfs_iertr::future<coll_root_t>;
  virtual mkfs_ret mkfs(
    Transaction &t) = 0;

  /// Get the collection's persisted root node
  using get_coll_node_iertr = base_iertr;
  using get_coll_node_ret = get_coll_node_iertr::future<CollectionNode::CollectionNodeRef>;
  virtual get_coll_node_ret get_coll_node(
    const coll_root_t &coll_root,
    Transaction &t) = 0;

  /// Create collection
  using create_iertr = base_iertr;
  using create_ret = create_iertr::future<>;
  virtual create_ret create(
    coll_root_t &root,
    Transaction &t,
    coll_t cid,
    coll_info_t info
  ) = 0;

  /// List collections with info
  using list_iertr = base_iertr;
  using list_ret_bare = std::vector<std::pair<coll_t, coll_info_t>>;
  using list_ret = list_iertr::future<list_ret_bare>;
  virtual list_ret list(
    const coll_root_t &root,
    Transaction &t) = 0;

  /// Remove cid
  using remove_iertr = base_iertr;
  using remove_ret = remove_iertr::future<>;
  virtual remove_ret remove(
    const coll_root_t &coll_root,
    Transaction &t,
    coll_t cid) = 0;

  /// Update info for cid
  using update_iertr = base_iertr;
  using update_ret = base_iertr::future<>;
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
