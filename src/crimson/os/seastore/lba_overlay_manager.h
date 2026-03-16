// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <iostream>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer_fwd.h"
#include "include/interval_set.h"
#include "common/interval_map.h"

#include "crimson/osd/exceptions.h"

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/lba_mapping.h"
#include "crimson/os/seastore/lba_types.h"
#include "crimson/os/seastore/logical_child_node.h"

#include "crimson/os/seastore/lba_manager.h"

namespace crimson::os::seastore {

/**
 * LBAOverlayManager
 * A projection layer over LBAManager for transactions.
 * Users (e.g. TransactionManager) only see overlaid (cached) mapping changes.
 * Cursors returned here are LBAOverlayCursor and reflect uncommitted updates.
 * LBAOverlayManager records ops to let the underlying
 * LBAManager replay then at commit time.
 *
 * Main path:
 * - get_cursor() -     return an LBAOverlayCursor created from the
 *                      overlay (if any) or the base LBAManager view.
 *
 * - mutations    -     store a deferred op via apply_overlay_op()
 *                      and update/return the overlay view via
 *                      apply_transaction_overlay().
 *                      Examples: alloc_extent(s), update_mapping_refcount..
 *
 * - commit_overlay() - traverse over the per-txn deferred ops in a serilzed order
 *                      and call the matching LBAManager ops.
 *                      (alloc_* / update_mapping* / remap_mappings / remove),
 *                      then clear the overlay state.
 *
 * Notes:
 * - Overlay is txn-local, base LBA tree is *unchanged* until commit.
 * - This goal here is to avoid conflicts from shared LBA nodes.
 *
 * See: doc/dev/crimson/seastore_conflict_handling.pdf
 */

class LBAOverlayManager;
using LBAOverlayManagerRef = std::unique_ptr<LBAOverlayManager>;

class LBAOverlayManager { //  : public LBAManager
//todo: inherit later, avoid overridng all methods..

private:
  explicit LBAOverlayManager(LBAManagerRef base);
  LBAManagerRef lba_manager;

  // todo: Move this to transaction
  std::unordered_map<transaction_id_t, overlay_entry> overlaid_ops;

  // Store overlay cursor per txn
  void apply_transaction_overlay(
    LBAOverlayCursor overlay_cursor,
    Transaction &t);

  // Store overlay operation to be applied to lba_manager
  // at commit time
  void apply_overlay_op(
    Transaction &t,
    overlay_entry entry);

public:
  static LBAOverlayManagerRef create_lba_overlay_manager(Cache &cache);

  void commit_overlay(Transaction &t) {
    // traverse over all overlaid_ops and apply to lba_manager
    // For example:
    //  case Transaction::op_type::update_refcount:
    //    lba_manager->update_mapping_refcount();
  }

  /*
  get_cursor returns LBAOverlayCursor even if no overlay exists
  */
  using get_cursor_iertr = base_iertr::extend<
    crimson::ct_error::enoent>;
  using get_cursor_ret = get_cursor_iertr::future<LBAOverlayCursor>;
  get_cursor_ret get_cursor(
    Transaction &t,
    laddr_t offset,
    bool search_containing = false);

  get_cursor_iertr::future<LBAOverlayCursor> update_mapping_refcount(
    Transaction &t,
    LBACursorRef cursor,
    int delta);

  using alloc_extent_iertr = base_iertr;

  using alloc_extents_ret = alloc_extent_iertr::future<
    std::vector<LBAOverlayCursor>>;
  alloc_extents_ret alloc_extents(
    Transaction &t,
    LBACursorRef cursor,
    std::vector<LogicalChildNodeRef> ext);

  using alloc_extent_ret = alloc_extent_iertr::future<LBAOverlayCursor>;
  alloc_extent_ret alloc_extent(
    Transaction &t,
    laddr_t hint,
    LogicalChildNode &nextent,
    extent_ref_count_t refcount);

  using mkfs_iertr = base_iertr;
  using mkfs_ret = mkfs_iertr::future<>;
  mkfs_ret mkfs(
    Transaction &t) {
    co_return co_await lba_manager->mkfs(t);
  }

};

}
