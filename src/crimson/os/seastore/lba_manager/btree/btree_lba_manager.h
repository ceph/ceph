// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/lba_manager.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/segment_manager.h"

#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree.h"

namespace crimson::os::seastore::lba_manager::btree {

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
 */
class BtreeLBAManager : public LBAManager {
public:
  BtreeLBAManager(
    SegmentManager &segment_manager,
    Cache &cache);

  mkfs_ret mkfs(
    Transaction &t) final;

  get_mappings_ret get_mappings(
    Transaction &t,
    laddr_t offset, extent_len_t length) final;

  get_mappings_ret get_mappings(
    Transaction &t,
    laddr_list_t &&list) final;

  get_mapping_ret get_mapping(
    Transaction &t,
    laddr_t offset) final;

  alloc_extent_ret alloc_extent(
    Transaction &t,
    laddr_t hint,
    extent_len_t len,
    paddr_t addr) final;

  ref_ret decref_extent(
    Transaction &t,
    laddr_t addr) final {
    return update_refcount(t, addr, -1);
  }

  ref_ret incref_extent(
    Transaction &t,
    laddr_t addr) final {
    return update_refcount(t, addr, 1);
  }

  void complete_transaction(
    Transaction &t) final;

  init_cached_extent_ret init_cached_extent(
    Transaction &t,
    CachedExtentRef e) final;

  scan_mappings_ret scan_mappings(
    Transaction &t,
    laddr_t begin,
    laddr_t end,
    scan_mappings_func_t &&f) final;

  scan_mapped_space_ret scan_mapped_space(
    Transaction &t,
    scan_mapped_space_func_t &&f) final;

  rewrite_extent_ret rewrite_extent(
    Transaction &t,
    CachedExtentRef extent) final;

  update_mapping_ret update_mapping(
    Transaction& t,
    laddr_t laddr,
    paddr_t prev_addr,
    paddr_t paddr) final;

  get_physical_extent_if_live_ret get_physical_extent_if_live(
    Transaction &t,
    extent_types_t type,
    paddr_t addr,
    laddr_t laddr,
    seastore_off_t len) final;

  void add_pin(LBAPin &pin) final {
    auto *bpin = reinterpret_cast<BtreeLBAPin*>(&pin);
    pin_set.add_pin(bpin->pin);
    bpin->parent = nullptr;
  }

  ~BtreeLBAManager();
private:
  SegmentManager &segment_manager;
  Cache &cache;

  btree_pin_set_t pin_set;

  struct {
    uint64_t num_alloc_extents = 0;
    uint64_t num_alloc_extents_iter_nexts = 0;
  } stats;

  op_context_t get_context(Transaction &t) {
    return op_context_t{cache, t, &pin_set};
  }

  static btree_range_pin_t &get_pin(CachedExtent &e);

  seastar::metrics::metric_group metrics;
  void register_metrics();
  template <typename F, typename... Args>
  auto with_btree(
    op_context_t c,
    F &&f) {
    return cache.get_root(
      c.trans
    ).si_then([this, c, f=std::forward<F>(f)](RootBlockRef croot) mutable {
      return seastar::do_with(
	LBABtree(croot->get_root().lba_root),
	[this, c, croot, f=std::move(f)](auto &btree) mutable {
	  return f(
	    btree
	  ).si_then([this, c, croot, &btree] {
	    if (btree.is_root_dirty()) {
	      auto mut_croot = cache.duplicate_for_write(
		c.trans, croot
	      )->cast<RootBlock>();
	      mut_croot->get_root().lba_root = btree.get_root_undirty();
	    }
	    return base_iertr::now();
	  });
	});
    });
  }

  template <typename State, typename F>
  auto with_btree_state(
    op_context_t c,
    State &&init,
    F &&f) {
    return seastar::do_with(
      std::forward<State>(init),
      [this, c, f=std::forward<F>(f)](auto &state) mutable {
	(void)this; // silence incorrect clang warning about capture
	return with_btree(c, [&state, f=std::move(f)](auto &btree) mutable {
	  return f(btree, state);
	}).si_then([&state] {
	  return seastar::make_ready_future<State>(std::move(state));
	});
      });
  }

  template <typename State, typename F>
  auto with_btree_state(
    op_context_t c,
    F &&f) {
    return with_btree_state<State, F>(c, State{}, std::forward<F>(f));
  }

  template <typename Ret, typename F>
  auto with_btree_ret(
    op_context_t c,
    F &&f) {
    return with_btree_state<Ret>(
      c,
      [f=std::forward<F>(f)](auto &btree, auto &ret) mutable {
	return f(
	  btree
	).si_then([&ret](auto &&_ret) {
	  ret = std::move(_ret);
	});
      });
  }

  /**
   * update_refcount
   *
   * Updates refcount, returns resulting refcount
   */
  using update_refcount_ret = ref_ret;
  update_refcount_ret update_refcount(
    Transaction &t,
    laddr_t addr,
    int delta);

  /**
   * _update_mapping
   *
   * Updates mapping, removes if f returns nullopt
   */
  using _update_mapping_iertr = ref_iertr;
  using _update_mapping_ret = ref_iertr::future<lba_map_val_t>;
  using update_func_t = std::function<
    lba_map_val_t(const lba_map_val_t &v)
    >;
  _update_mapping_ret _update_mapping(
    Transaction &t,
    laddr_t addr,
    update_func_t &&f);
};
using BtreeLBAManagerRef = std::unique_ptr<BtreeLBAManager>;

}
