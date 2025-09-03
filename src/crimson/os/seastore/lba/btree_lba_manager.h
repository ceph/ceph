// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/**
 * =============================================================================
 * BtreeLBAManager - the concrete LBA (Logical Block Address) manager backed
 * by a wandering B+tree (FixedKVBtree).
 *
 * This is the primary address-translation layer in Crimson Seastore.  It maps
 * logical addresses (laddr_t) to physical addresses (paddr_t) and manages the
 * lifecycle of those mappings: allocation, lookup, update, cloning (indirect
 * mappings for snapshots), remapping, and removal.
 *
 * The underlying tree is an LBABtree = FixedKVBtree<laddr_t, lba_map_val_t,
 * LBAInternalNode, LBALeafNode, LBACursor, 4096>.
 *
 * All operations are transaction-scoped - they read/modify a Transaction and
 * become visible only after commit.  The tree is copy-on-write ("wandering"):
 * modified nodes are duplicated, never updated in place.
 *
 * Key abstractions:
 *   LBACursor  - a lightweight handle to a single leaf entry (see lba_btree_node.h)
 *   LBAMapping - a higher-level wrapper combining direct + optional indirect cursors
 *                (see lba_mapping.h)
 *   alloc_mapping_info_t - describes one mapping to be inserted (key, value, extent)
 * =============================================================================
 */

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

#include "crimson/os/seastore/btree/fixed_kv_btree.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/cache.h"

#include "crimson/os/seastore/lba/lba_btree_node.h"
#include "crimson/os/seastore/btree/btree_types.h"

namespace crimson::os::seastore {
class LogicalCachedExtent;
}

namespace crimson::os::seastore::lba {
class BtreeLBAManager;

/**
 * The concrete btree type: keys are laddr_t, values are lba_map_val_t
 * (length, pladdr, refcount, checksum, extent-type), nodes are 4096 bytes.
 */
using LBABtree = FixedKVBtree<
  laddr_t, lba_map_val_t, LBAInternalNode,
  LBALeafNode, LBACursor, LBA_BLOCK_SIZE>;

/**
 * BtreeLBAManager
 *
 * Uses a wandering btree to track two things:
 * 1) lba state including laddr_t -> paddr_t mapping
 * 2) reverse paddr_t -> laddr_t mapping for gc (TODO)
 *
 * Generally, any transaction will involve
 * 1) deltas against lba tree nodes
 * 2) new lba tree nodes
 *    - Note, there must necessarily be a delta linking
 *      these new nodes into the tree -- might be a
 *      bootstrap_state_t delta if new root
 *
 * get_mappings, alloc_extent_*, etc populate a Transaction
 * which then gets submitted
 *
 * Some additional implementation notes:
 *
 * Transaction flow:
 *   1. Read operations (get_cursor, get_cursors, scan_mappings) traverse the
 *      tree within the transaction's view and return LBACursorRef handles.
 *   2. Write operations (alloc_extent, reserve_region, clone_mapping, etc.)
 *      produce deltas against tree nodes (CoW) and/or allocate new nodes.
 *   3. On commit, new/modified tree nodes are written out; the root pointer
 *      is atomically updated.
 *
 * Mapping types:
 *   - Direct:   laddr → paddr (normal data extent)
 *   - Indirect: laddr → intermediate_laddr (clone/snapshot, points to another
 *               LBA entry that holds the actual paddr)
 *   - Zero:     laddr → P_ADDR_ZERO (reserved region, no data yet)
 */
class BtreeLBAManager : public LBAManager {
public:
  BtreeLBAManager(Cache &cache, store_index_t store_index)
    : cache(cache)
  {
    register_metrics(store_index);
  }

  // ---------------------------------------------------------------------------
  // Lookup operations - read the LBA tree to find mappings.
  // All return LBACursorRef handles into the tree within the transaction's view.
  // ---------------------------------------------------------------------------

  /**
   * Create the initial empty LBA tree (single empty leaf as root).
   */
  mkfs_ret mkfs(
    Transaction &t) final;

  /**
   * Find all mappings that overlap the range [offset, offset+length).
   * Uses upper_bound_right to find the first overlapping entry, then
   * iterates forward.  Returns a list of cursors.
   */
  get_cursors_ret get_cursors(
    Transaction &t,
    laddr_t offset, extent_len_t length) final;

  /**
   * Find the mapping at exactly 'offset', or (if search_containing) the
   * mapping whose range contains 'offset'.  Returns enoent if not found.
   */
  get_cursor_ret get_cursor(
    Transaction &t,
    laddr_t offset,
    bool search_containing = false) final;

  /**
   * Find the mapping for a known LogicalChildNode by navigating up from
   * the extent to its parent leaf node (avoids a full tree traversal).
   */
  get_cursor_ret get_cursor(
    Transaction &t,
    LogicalChildNode &extent) final;

  /**
   * Standard btree lower_bound - returns cursor to first entry >= laddr.
   */
  lower_bound_ret lower_bound(
    Transaction &t,
    laddr_t laddr) final;

  upper_bound_right_ret upper_bound_right(
    Transaction &t,
    laddr_t laddr) final;

  promote_extent_ret promote_extent(
    Transaction &t,
    LBACursor &cursor,
    std::vector<LogicalChildNodeRef> extents) final;

  demote_extent_ret demote_extent(
    Transaction &t,
    LBACursor &cursor,
    LogicalChildNode &extent) final;

   // ---------------------------------------------------------------------------
   // Allocation operations - insert new mappings into the LBA tree.
   // ---------------------------------------------------------------------------

  /**
   * Reserve a region at a specific laddr (inserts a zero-mapping with
   * P_ADDR_ZERO).  'pos' is a cursor used as an insertion hint.
   */
  alloc_extent_ret reserve_region(
    Transaction &t,
    LBACursorRef pos,
    laddr_t laddr,
    extent_len_t len,
    extent_types_t type) final;

  /**
   * Reserve a region using a hint-based address search.  Delegates to
   * alloc_contiguous_mappings with a single zero-mapping info.
   */
  alloc_extent_ret reserve_region(
    Transaction &t,
    laddr_hint_t hint,
    extent_len_t len,
    extent_types_t type) final
  {
    std::vector<alloc_mapping_info_t> alloc_infos = {
      alloc_mapping_info_t::create_zero(len, type)};
    auto cursors = co_await alloc_contiguous_mappings(
      t, hint, alloc_infos);
    assert(cursors.size() == 1);
    co_return std::move(cursors.front());
  }

  // ---------------------------------------------------------------------------
  // Clone / move operations - support for snapshots and defragmentation.
  // ---------------------------------------------------------------------------

  /**
   * Create an indirect mapping (clone) at 'laddr' that points to 'inter_key'
   * within the direct mapping 'mapping'.  Optionally increments the refcount
   * of the target mapping (updateref=true).
   * 'pos' is used as an insertion hint for the new indirect entry.
   */
  clone_mapping_ret clone_mapping(
    Transaction &t,
    LBACursorRef pos,
    LBACursorRef mapping,
    laddr_t laddr,
    laddr_t inter_key,
    extent_len_t len,
    bool updateref) final;

  /**
   * Move an indirect mapping: copy src to dest_laddr, then remove src.
   */
  move_mapping_ret move_indirect_mapping(
    Transaction &t,
    LBACursorRef src,
    laddr_t dest_laddr,
    LBACursorRef dest) final {
    assert(src->is_indirect());
    assert(!src->has_shadow_paddr());
    return _move_mapping(
      t, std::move(src), dest_laddr, std::move(dest), nullptr);
  }

  /**
   * Move a direct mapping: copy src to dest_laddr (re-linking the data
   * extent), then remove src.
   */
  move_mapping_ret move_direct_mapping(
    Transaction &t,
    LBACursorRef src,
    laddr_t dest_laddr,
    LBACursorRef dest,
    LogicalChildNode &extent) final {
    assert(!src->is_indirect());
    return _move_mapping(
      t, std::move(src), dest_laddr, std::move(dest), &extent);
  }

  /**
   * Move a direct mapping and set up a clone: copies src to dest, then
   * converts the original src mapping into an indirect mapping pointing
   * at the new location.  Used during snapshot operations.
   */
  move_mapping_ret move_and_clone_direct_mapping(
    Transaction &t,
    LBACursorRef src,
    laddr_t dest_laddr,
    LBACursorRef dest,
    LogicalChildNode &extent) final;

#ifdef UNIT_TESTS_BUILT
  get_end_mapping_ret get_end_mapping(Transaction &t) final;
#endif

  /**
   * Insert mappings for a vector of extents at their pre-assigned laddrs,
   * using 'pos' as a btree insertion hint.  Inserts in reverse order for
   * efficiency (each insertion stays near the hint).
   */
  alloc_extents_ret alloc_extents(
    Transaction &t,
    LBACursorRef pos,
    std::vector<LogicalChildNodeRef> ext) final;

  /**
   * Allocate a single extent: finds a free laddr near 'hint', inserts the
   * mapping (laddr -> ext.paddr), and assigns the laddr to the extent.
   * Delegates to alloc_contiguous_mappings with a single direct mapping info.
   */
  alloc_extent_ret alloc_extent(
    Transaction &t,
    laddr_hint_t hint,
    LogicalChildNode &ext,
    extent_ref_count_t refcount) final
  {
    // The real checksum will be updated upon transaction commit
    assert(ext.get_last_committed_crc() == 0);
    assert(!ext.has_laddr());
    std::vector<alloc_mapping_info_t> alloc_infos = {
      alloc_mapping_info_t::create_direct(
	L_ADDR_NULL,
	ext.get_length(),
	ext.get_paddr(),
	refcount,
	ext.get_last_committed_crc(),
	ext)};
    auto cursors = co_await alloc_contiguous_mappings(
      t, hint, alloc_infos
    );
    assert(cursors.size() == 1);
    co_return std::move(cursors.front());
  }

  /**
   * Allocate multiple extents.  If extents already have laddrs assigned
   * (has_laddr), uses alloc_sparse_mappings (each at its own address).
   * Otherwise, uses alloc_contiguous_mappings (packed sequentially from
   * a single hint-based starting point).
   */
  alloc_extents_ret alloc_extents(
    Transaction &t,
    laddr_hint_t hint,
    std::vector<LogicalChildNodeRef> extents,
    extent_ref_count_t refcount) final
  {
    std::vector<alloc_mapping_info_t> alloc_infos;
    assert(!extents.empty());
    auto has_laddr = extents.front()->has_laddr();
    for (auto &extent : extents) {
      assert(extent);
      assert(extent->has_laddr() == has_laddr);
      alloc_infos.emplace_back(
	alloc_mapping_info_t::create_direct(
	  extent->has_laddr() ? extent->get_laddr() : L_ADDR_NULL,
	  extent->get_length(),
	  extent->get_paddr(),
	  refcount,
	  extent->get_last_committed_crc(),
	  *extent));
    }
    std::list<LBACursorRef> cursors;
    if (has_laddr) {
      assert(hint.condition == laddr_conflict_condition_t::all_at_never);
      cursors = co_await alloc_sparse_mappings(
        t, hint, alloc_infos);
      assert(alloc_infos.size() == cursors.size());
#ifndef NDEBUG
      auto info_p = alloc_infos.begin();
      auto cursor_p = cursors.begin();
      for (; info_p != alloc_infos.end(); info_p++, cursor_p++) {
	auto &cursor = *cursor_p;
	assert(cursor->get_laddr() == info_p->key);
      }
#endif
    } else {
      cursors = co_await alloc_contiguous_mappings(t, hint, alloc_infos);
    }
    co_return std::vector<LBACursorRef>(cursors.begin(), cursors.end());
  }

   // ---------------------------------------------------------------------------
   // Update operations - modify existing mappings in the tree.
   // ---------------------------------------------------------------------------

  /**
   * Adjust a mapping's refcount by 'delta'.  If refcount reaches 0,
   * _update_mapping removes the entry entirely.
   */
  base_iertr::future<LBACursorRef> update_mapping_refcount(
    Transaction &t,
    LBACursorRef cursor,
    int delta) final {
    co_return co_await _update_mapping(
      t,
      *cursor,
      [delta](lba_map_val_t ret) {
	ceph_assert((int)ret.refcount + delta >= 0);
	ret.refcount += delta;
	return ret;
      },
      nullptr
    ).handle_error_interruptible(
      base_iertr::pass_further{},
      crimson::ct_error::assert_all("unexpected error")
    );
  }

  /**
   * Split or shrink an existing mapping according to the remap entries.
   * Used by the remap_pin flow to adjust mapping boundaries (e.g. when
   * an extent is partially overwritten).
   */
  remap_ret remap_mappings(
    Transaction &t,
    LBACursorRef mapping,
    std::vector<remap_entry_t> remaps) final;

  // ---------------------------------------------------------------------------
  // Extent lifecycle - cache warm-up, GC, and commit-time updates.
  // ---------------------------------------------------------------------------

  /**
   * init_cached_extent
   *
   * Checks whether e is live (reachable from lba tree) and drops or initializes
   * accordingly.
   *
   * Returns if e is live.
   */
  /**
   * For logical extents: looks up e's laddr and checks if the tree entry
   * points to e's paddr.  If live, links the extent into the parent leaf's
   * child-tracking array.
   * For tree nodes (internal/leaf): delegates to LBABtree::init_cached_extent.
   */
  init_cached_extent_ret init_cached_extent(
    Transaction &t,
    CachedExtentRef e) final;

#ifdef UNIT_TESTS_BUILT
  check_child_trackers_ret check_child_trackers(Transaction &t) final;
#endif

  /**
   * Iterate all direct mappings in [begin, end) and invoke f for each.
   * Skips indirect mappings.
   */
  scan_mappings_ret scan_mappings(
    Transaction &t,
    laddr_t begin,
    laddr_t end,
    scan_mappings_func_t &&f) final;

  /**
   * GC/cleaner entry point: relocate an LBA tree node (internal or leaf)
   * to a new segment.  Delegates to LBABtree::rewrite_extent which
   * allocates a new copy and patches the parent pointer.  Skips non-LBA
   * extents.
   */
  rewrite_extent_ret rewrite_extent(
    Transaction &t,
    CachedExtentRef extent) final;

  /**
   * Update a mapping's paddr, length, and checksum after a data extent
   * has been rewritten (e.g. during commit when extents move from
   * initial-write segment to their final location).  Validates that the
   * old paddr/length match before updating.
   */
  update_mapping_ret update_mapping(
    Transaction& t,
    LBACursorRef cursor,
    extent_len_t prev_len,
    paddr_t prev_addr,
    LogicalChildNode&) final;

  /**
   * Batch version of update_mapping for multiple extents.  For each
   * extent, navigates to its parent leaf (via get_parent_node) and
   * updates paddr + checksum.
   */
  update_mappings_ret update_mappings(
    Transaction& t,
    const std::list<LogicalChildNodeRef>& extents);

  /**
   * GC helper: check if a tree node at (type, paddr, laddr) is still
   * live in the tree.  Returns the extent if live, null otherwise.
   */
  get_physical_extent_if_live_ret get_physical_extent_if_live(
    Transaction &t,
    extent_types_t type,
    paddr_t addr,
    laddr_t laddr,
    extent_len_t len) final;

  /**
   * Full tree scan: iterate every mapping from L_ADDR_MIN to end,
   * invoking f with (laddr, paddr, len, type) for each entry plus
   * (paddr, len) for each internal tree node.  Used for space accounting.
   */
  scan_mapped_space_ret scan_mapped_space(
    Transaction &t,
    scan_mapped_space_func_t &&f) final;

private:
  Cache &cache;

  /**
   * Performance counters registered as Seastar metrics under the "LBA" group.
   * num_alloc_extents:            total bytes allocated via alloc_extent paths
   * num_alloc_extents_iter_nexts: total btree iterator steps during
   *                               search_insert_position (measures conflict
   *                               resolution cost when hints collide)
   */
  struct {
    uint64_t num_alloc_extents = 0;
    uint64_t num_alloc_extents_iter_nexts = 0;
  } stats;

  /**
   * Describes one mapping to be inserted into the tree.  Used by
   * alloc_contiguous_mappings, alloc_sparse_mappings, and insert_mappings.
   *
   * Three factory methods produce the three mapping flavors:
   *   create_zero     - reserved region, paddr = P_ADDR_ZERO, no data extent
   *   create_indirect - clone entry, pladdr holds a local_clone_id pointing to
   *                     the direct mapping that owns the physical data
   *   create_direct   - normal mapping, pladdr holds the paddr, 'extent'
   *                     points to the in-memory data extent for child-tracking
   */
  struct alloc_mapping_info_t {
    laddr_t key = L_ADDR_NULL; // once assigned, the allocation to
			       // key must be exact and successful
    lba_map_val_t value;
    LogicalChildNode* extent = nullptr;

    bool is_zero_mapping() const {
      return value.pladdr.is_paddr() && value.pladdr.get_paddr().is_zero();
    }

    bool is_indirect_mapping() const {
      return value.pladdr.is_laddr();
    }

    static alloc_mapping_info_t create_zero(
      extent_len_t len,
      extent_types_t type) {
      return {
	L_ADDR_NULL,
	{
	  len,
	  pladdr_t(P_ADDR_ZERO),
	  P_ADDR_NULL,
	  EXTENT_DEFAULT_REF_COUNT,
	  0,
	  type
	}};
    }
    static alloc_mapping_info_t create_indirect(
      laddr_t laddr,
      extent_len_t len,
      laddr_t intermediate_key) {
      return {
	laddr,
	{
	  len,
	  pladdr_t(intermediate_key.get_local_clone_id()),
	  P_ADDR_NULL,
	  EXTENT_DEFAULT_REF_COUNT,
	  0,	// crc will only be used and checked with LBA direct mappings
		// also see pin_to_extent(_by_type)
	  // only OBJECT_DATA_BLOCK support indirect mapping for now
	  extent_types_t::OBJECT_DATA_BLOCK
	}};
    }
    static alloc_mapping_info_t create_direct(
      laddr_t laddr,
      extent_len_t len,
      paddr_t paddr,
      extent_ref_count_t refcount,
      checksum_t checksum,
      LogicalChildNode& extent) {
      return {
	laddr,
	{len, pladdr_t(paddr), P_ADDR_NULL, refcount, checksum, extent.get_type()},
	&extent
      };
    }
  };

  /**
   * Bundle cache + transaction into the op_context_t passed throughout
   * the btree operations.
   */
  op_context_t get_context(Transaction &t) {
    return op_context_t{cache, t};
  }

  seastar::metrics::metric_group metrics;
  void register_metrics(store_index_t store_index);

  // -------------------------------------------------------------------------
  // Internal helpers for move/copy operations.
  // -------------------------------------------------------------------------

  /*
   * _move_mapping
   *
   * copy the mapping "src" to "dest" and remove the "src" mapping.
   * If extent != null (direct mapping), re-links the data extent to the
   * new mapping.
   *
   * Return: the mappings next to "src" and the "dest" mapping
   */
  move_mapping_ret _move_mapping(
    Transaction &t,
    LBACursorRef src,
    laddr_t dest_laddr,
    LBACursorRef dest,
    LogicalChildNode *extent);

  /*
   * _copy_mapping
   * The data extent (if any) is linked to the new destination entry.
   *
   * copy the mapping "src" to "dest", the extent attached to
   * "src" will also be attached to the dest. This is the building
   * block for _move_mapping and move_and_clone_direct_mapping
   *
   * Return: the "src" and the new "dest"
   */
  move_mapping_ret _copy_mapping(
    op_context_t c,
    LBABtree &btree,
    LBACursorRef src,
    laddr_t dest_laddr,
    LBACursorRef dest,
    LogicalChildNode *extent);

  /**
   * update_paddr_sync
   *
   * This is basically for updating the paddr of the mapping
   * that has been copied by the transaction t and modified
   * by some background rewrite transaction.
   *
   * Synchronous - requires the leaf to already be cached.
   */
  void update_paddr_sync(
    Transaction &t,
    laddr_t laddr,
    paddr_t paddr);


  /**
   * _update_mapping
   *
   * Updates mapping, removes if f returns nullopt
   *
   * Core update primitive.  Applies f(old_val) -> new_val on the mapping
   * at cursor's position.  If the resulting refcount is 0, removes the
   * entry (btree.remove).  Otherwise, updates in place (btree.update).
   *
   * Used by update_mapping_refcount, update_mapping (paddr change), and
   * remap_mappings.  The LogicalChildNode* parameter, when non-null and
   * not yet tracked, is linked as the leaf's child pointer for the entry.
   */
  using _update_mapping_ret = ref_iertr::future<LBACursorRef>;
  using update_func_t = std::function<
    lba_map_val_t(const lba_map_val_t &v)
    >;
  _update_mapping_ret _update_mapping(
    Transaction &t,
    LBACursor &cursor,
    update_func_t f,
    LogicalChildNode*);

  // -------------------------------------------------------------------------
  // Address allocation - finding free space in the logical address range.
  // -------------------------------------------------------------------------

  /**
   * Result of search_insert_position: the chosen laddr and a btree iterator
   * positioned just past it (suitable as an insertion hint).
   */
  struct insert_position_t {
    laddr_t laddr;
    LBABtree::iterator insert_iter;
  };

  /**
   * Find a free laddr near 'hint' that doesn't conflict with existing
   * mappings.  Uses upper_bound_right + forward scan (linear or random
   * retry depending on hint.policy) to skip past occupied regions.
   * Tracks iteration cost in stats.num_alloc_extents_iter_nexts.
   */
  using search_insert_position_iertr = base_iertr;
  using search_insert_position_ret =
      search_insert_position_iertr::future<insert_position_t>;
  search_insert_position_ret search_insert_position(
    op_context_t c,
    LBABtree &btree,
    laddr_hint_t hint,
    extent_len_t length);

  using alloc_mappings_iertr = base_iertr;
  using alloc_mappings_ret =
      alloc_mappings_iertr::future<std::list<LBACursorRef>>;

  /**
   * alloc_contiguous_mappings
   *
   * Insert a range of contiguous mappings into the LBA btree.
   *
   * hint is a non-null laddr hint for allocation. All alloc_infos' key
   * should be L_ADDR_NULL, the final laddr is relative to the allocated
   * laddr based on preceding mappings' total length.
   */
  alloc_mappings_ret alloc_contiguous_mappings(
    Transaction &t,
    laddr_hint_t hint,
    std::vector<alloc_mapping_info_t> &alloc_infos);

  /**
   * alloc_sparse_mappings
   *
   * Insert a range of sparse mappings into the LBA btree.
   *
   * hint is a non-null laddr hint for allocation. All of alloc_infos' key
   * are non-null laddr hints and must be incremental, each mapping's final
   * laddr maintains same offset to allocated laddr as original to hint.
   */
  alloc_mappings_ret alloc_sparse_mappings(
    Transaction &t,
    laddr_hint_t hint,
    std::vector<alloc_mapping_info_t> &alloc_infos);

  /**
   * insert_mappings
   *
   * Insert all lba mappings built from alloc_infos into LBA btree before
   * iter and return the inserted LBACursors.
   *
   * NOTE: There is no guarantee that the returned cursors are all valid
   * since the successive insertion is possible to invalidate the parent
   * extent of predecessively returned LBACursor.
   */
  alloc_mappings_ret insert_mappings(
    op_context_t c,
    LBABtree &btree,
    LBABtree::iterator iter,
    std::vector<alloc_mapping_info_t> &alloc_infos);

  // -------------------------------------------------------------------------
  // Internal lookup helpers (take op_context + btree, avoid redundant root fetch).
  // -------------------------------------------------------------------------

  /**
   * Exact-match lookup: lower_bound(offset), return enoent if no match.
   */
  get_cursor_ret get_cursor(
    op_context_t c,
    LBABtree& btree,
    laddr_t offset);

  /**
   * Containing-match: upper_bound_right(laddr) to find the mapping whose
   * range [key, key+len) contains laddr.
   */
  get_cursor_ret get_containing_cursor(
    op_context_t c,
    LBABtree &btree,
    laddr_t laddr);

  /**
   * Range lookup: upper_bound_right + iterate while key < offset+length.
   */
  get_cursors_ret get_cursors(
    op_context_t c,
    LBABtree& btree,
    laddr_t offset,
    extent_len_t length);

  /**
   * Resolve an indirect cursor to its target direct cursor.  An indirect
   * mapping stores a local_clone_id; get_intermediate_key() reconstructs
   * the full laddr of the direct mapping.  get_cursors() on that key
   * returns the single direct cursor that owns the physical data.
   */
  using resolve_indirect_cursor_ret = base_iertr::future<LBACursorRef>;
  resolve_indirect_cursor_ret resolve_indirect_cursor(
    op_context_t c,
    LBABtree& btree,
    const LBACursor& indirect_cursor);

  /**
   * Convenience overload that fetches the btree internally.
   */
  resolve_indirect_cursor_ret resolve_indirect_cursor(
    op_context_t c,
    const LBACursor& indirect_cursor) {
    assert(indirect_cursor.is_indirect());
    return with_btree<LBABtree>(
      cache,
      c,
      [c, &indirect_cursor, this](auto &btree) {
      return resolve_indirect_cursor(c, btree, indirect_cursor);
    });
  }
};
using BtreeLBAManagerRef = std::unique_ptr<BtreeLBAManager>;

}
