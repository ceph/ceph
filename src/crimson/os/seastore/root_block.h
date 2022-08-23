// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/btree/btree_child_tracker.h"

namespace crimson::os::seastore {

/**
 * RootBlock
 *
 * Holds the physical addresses of all metadata roots.
 * In-memory values may be
 * - absolute: reference to block which predates the current transaction
 * - record_relative: reference to block updated in this transaction
 *   if !pending()
 *
 * Journal replay only considers deltas and must always discover the most
 * recent value for the RootBlock.  Because the contents of root_t above are
 * very small, it's simplest to stash the entire root_t value into the delta
 * and never actually write the RootBlock to a physical location (safe since
 * nothing references the location of the RootBlock).
 *
 * As a result, Cache treats the root differently in a few ways including:
 * - state will only ever be DIRTY or MUTATION_PENDING
 * - RootBlock's never show up in the transaction fresh or dirty lists --
 *   there's a special Transaction::root member for when the root needs to
 *   be mutated.
 *
 * TODO: Journal trimming will need to be aware of the most recent RootBlock
 * delta location, or, even easier, just always write one out with the
 * mutation which changes the journal trim bound.
 */
struct RootBlock final : CachedExtent {
  constexpr static seastore_off_t SIZE = 4<<10;
  using Ref = TCachedExtentRef<RootBlock>;

  root_t root;

  child_tracker_t* lba_root_node = nullptr;
  CachedExtent::trans_view_set_t lba_root_trans_views;

  child_tracker_t* backref_root_node = nullptr;
  CachedExtent::trans_view_set_t backref_root_trans_views;

  RootBlock() : CachedExtent(0) {
    lba_root_node = new child_tracker_t();
    backref_root_node = new child_tracker_t();
  }

  RootBlock(const RootBlock &rhs)
    : CachedExtent(rhs),
      root(rhs.root),
      lba_root_node(rhs.lba_root_node),
      backref_root_node(rhs.backref_root_node)
  {}

  template <typename T>
  CachedExtentRef get_phy_root_node(Transaction &t);

  template <typename T>
  child_tracker_t* get_phy_root_tracker() {
    static_assert(std::is_same_v<T, laddr_t>
      || std::is_same_v<T, paddr_t>);
    if constexpr (std::is_same_v<T, laddr_t>) {
      return lba_root_node;
    } else {
      return backref_root_node;
    }
  }

  template <typename T>
  void link_root_node(CachedExtent &root_node) {
    static_assert(std::is_same_v<T, laddr_t>
      || std::is_same_v<T, paddr_t>);
    if constexpr (std::is_same_v<T, laddr_t>) {
      ceph_assert(lba_root_node->child.get() != &root_node);
      lba_root_node->child = root_node.weak_from_this();
    } else {
      ceph_assert(backref_root_node->child.get() != &root_node);
      backref_root_node->child = root_node.weak_from_this();
    }
  }

  template <typename T>
  void new_fixedkv_root(CachedExtent &node, Transaction &t);

  template <typename T>
  void add_fixedkv_root_trans_view(CachedExtent &node);

  CachedExtentRef duplicate_for_write(Transaction &t) final;

  static constexpr extent_types_t TYPE = extent_types_t::ROOT;
  extent_types_t get_type() const final {
    return extent_types_t::ROOT;
  }

  /// dumps root as delta
  ceph::bufferlist get_delta() final {
    ceph::bufferlist bl;
    ceph::buffer::ptr bptr(sizeof(root_t));
    *reinterpret_cast<root_t*>(bptr.c_str()) = root;
    bl.append(bptr);
    return bl;
  }

  /// overwrites root
  void apply_delta_and_adjust_crc(paddr_t base, const ceph::bufferlist &_bl) final {
    assert(_bl.length() == sizeof(root_t));
    ceph::bufferlist bl = _bl;
    bl.rebuild();
    root = *reinterpret_cast<const root_t*>(bl.front().c_str());
    root.adjust_addrs_from_base(base);
  }

  /// Patches relative addrs in memory based on record commit addr
  void on_delta_write(paddr_t record_block_offset) final {
    root.adjust_addrs_from_base(record_block_offset);
  }

  complete_load_ertr::future<> complete_load() final {
    ceph_abort_msg("Root is only written via deltas");
  }

  void on_initial_write() final {
    ceph_abort_msg("Root is only written via deltas");
  }

  root_t &get_root() { return root; }

  ~RootBlock() final {
    if (is_valid()) {
      if (lba_root_node) {
	delete lba_root_node;
      }
      if (backref_root_node) {
	delete backref_root_node;
      }
    }
  }
};
using RootBlockRef = RootBlock::Ref;

}
