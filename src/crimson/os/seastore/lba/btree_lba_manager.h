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
 */
class BtreeLBAManager : public LBAManager {
public:
  BtreeLBAManager(Cache &cache)
    : cache(cache)
  {
    register_metrics();
  }

  mkfs_ret mkfs(
    Transaction &t) final;

  get_mappings_ret get_mappings(
    Transaction &t,
    laddr_t offset, extent_len_t length) final;

  get_mapping_ret get_mapping(
    Transaction &t,
    laddr_t offset,
    bool search_containing = false) final;

  get_mapping_ret get_mapping(
    Transaction &t,
    LogicalChildNode &extent) final;

  alloc_extent_ret reserve_region(
    Transaction &t,
    LBAMapping pos,
    laddr_t laddr,
    extent_len_t len) final;

  alloc_extent_ret reserve_region(
    Transaction &t,
    laddr_t hint,
    extent_len_t len) final
  {
    std::vector<alloc_mapping_info_t> alloc_infos = {
      alloc_mapping_info_t::create_zero(len)};
    return seastar::do_with(
      std::move(alloc_infos),
      [&t, hint, this](auto &alloc_infos) {
      return alloc_contiguous_mappings(
	t, hint, alloc_infos, alloc_policy_t::linear_search
      ).si_then([](auto cursors) {
	assert(cursors.size() == 1);
	return LBAMapping::create_direct(std::move(cursors.front()));
      });
    });
  }

  clone_mapping_ret clone_mapping(
    Transaction &t,
    LBAMapping pos,
    LBAMapping mapping,
    laddr_t laddr,
    extent_len_t offset,
    extent_len_t len,
    bool updateref) final;

#ifdef UNIT_TESTS_BUILT
  get_end_mapping_ret get_end_mapping(Transaction &t) final;
#endif

  alloc_extents_ret alloc_extents(
    Transaction &t,
    LBAMapping pos,
    std::vector<LogicalChildNodeRef> ext) final;

  alloc_extent_ret alloc_extent(
    Transaction &t,
    laddr_t hint,
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
    return seastar::do_with(
      std::move(alloc_infos),
      [this, &t, hint](auto &alloc_infos) {
      return alloc_contiguous_mappings(
	t, hint, alloc_infos, alloc_policy_t::linear_search
      ).si_then([](auto cursors) {
	assert(cursors.size() == 1);
	return LBAMapping::create_direct(std::move(cursors.front()));
      });
    });
  }

  alloc_extents_ret alloc_extents(
    Transaction &t,
    laddr_t hint,
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
    return seastar::do_with(
      std::move(alloc_infos),
      [this, &t, hint, has_laddr](auto &alloc_infos)
    {
      if (has_laddr) {
	return alloc_sparse_mappings(
	  t, hint, alloc_infos, alloc_policy_t::deterministic)
#ifndef NDEBUG
	.si_then([&alloc_infos](std::list<LBACursorRef> cursors) {
	  assert(alloc_infos.size() == cursors.size());
	  auto info_p = alloc_infos.begin();
	  auto cursor_p = cursors.begin();
	  for (; info_p != alloc_infos.end(); info_p++, cursor_p++) {
	    auto &cursor = *cursor_p;
	    assert(cursor->get_laddr() == info_p->key);
	  }
	  return alloc_extent_iertr::make_ready_future<
	    std::list<LBACursorRef>>(std::move(cursors));
	})
#endif
	  ;
      } else {
	return alloc_contiguous_mappings(
	  t, hint, alloc_infos, alloc_policy_t::linear_search);
      }
    }).si_then([](std::list<LBACursorRef> cursors) {
      std::vector<LBAMapping> ret;
      for (auto &cursor : cursors) {
	ret.emplace_back(LBAMapping::create_direct(std::move(cursor)));
      }
      return ret;
    });
  }

  ref_ret remove_mapping(
    Transaction &t,
    laddr_t addr) final {
    return update_refcount(t, addr, -1
    ).si_then([this, &t](auto res) {
      ceph_assert(res.refcount == 0);
      if (res.addr.is_paddr()) {
	return ref_iertr::make_ready_future<
	  ref_update_result_t>(ref_update_result_t{
	    std::move(res), std::nullopt});
      }
      return update_refcount(t, res.key, -1
      ).si_then([indirect_result=std::move(res)](auto direct_result) mutable {
	return indirect_result.mapping.refresh(
	).si_then([direct_result=std::move(direct_result),
		   indirect_result=std::move(indirect_result)](auto) {
	  return ref_iertr::make_ready_future<
	    ref_update_result_t>(ref_update_result_t{
	      std::move(indirect_result),
	      std::move(direct_result)});
	});
      });
    });
  }

  ref_ret remove_indirect_mapping_only(
    Transaction &t,
    LBAMapping mapping) final {
    assert(mapping.is_viewable());
    assert(mapping.is_indirect());
    return seastar::do_with(
      std::move(mapping),
      [&t, this](auto &mapping) {
      return update_refcount(t, mapping.indirect_cursor.get(), -1
      ).si_then([](auto res) {
	return ref_iertr::make_ready_future<
	  ref_update_result_t>(ref_update_result_t{
	    std::move(res), std::nullopt});
      });
    });
  }

  ref_ret remove_mapping(
    Transaction &t,
    LBAMapping mapping) final {
    assert(mapping.is_viewable());
    assert(mapping.is_complete());
    return seastar::do_with(
      std::move(mapping),
      [&t, this](auto &mapping) {
      auto &cursor = mapping.get_effective_cursor();
      return update_refcount(t, &cursor, -1
      ).si_then([this, &t, &mapping](auto res) {
	ceph_assert(res.refcount == 0);
	if (res.addr.is_paddr()) {
	  assert(!mapping.is_indirect());
	  return ref_iertr::make_ready_future<
	    ref_update_result_t>(ref_update_result_t{
	      std::move(res), std::nullopt});
	}
	assert(mapping.is_indirect());
	auto &cursor = *mapping.direct_cursor;
	return cursor.refresh().si_then([this, &t, &cursor] {
	  return update_refcount(t, &cursor, -1);
	}).si_then([indirect_result=std::move(res)]
		   (auto direct_result) mutable {
	  return indirect_result.mapping.refresh(
	  ).si_then([direct_result=std::move(direct_result),
		     indirect_result=std::move(indirect_result)](auto) {
	    return ref_iertr::make_ready_future<
	      ref_update_result_t>(ref_update_result_t{
		std::move(indirect_result),
		std::move(direct_result)});
	  });
	});
      });
    });
  }

  ref_ret incref_extent(
    Transaction &t,
    laddr_t addr) final {
    return update_refcount(t, addr, 1
    ).si_then([](auto res) {
      return ref_update_result_t(std::move(res), std::nullopt);
    });
  }

  ref_ret incref_extent(
    Transaction &t,
    LBAMapping mapping) final {
    assert(mapping.is_viewable());
    return seastar::do_with(
      std::move(mapping),
      [&t, this](auto &mapping) {
      auto &cursor = mapping.get_effective_cursor();
      return update_refcount(t, &cursor, 1
      ).si_then([](auto res) {
	return ref_update_result_t(std::move(res), std::nullopt);
      });
    });
  }

  remap_ret remap_mappings(
    Transaction &t,
    LBAMapping mapping,
    std::vector<remap_entry_t> remaps) final;

  /**
   * init_cached_extent
   *
   * Checks whether e is live (reachable from lba tree) and drops or initializes
   * accordingly.
   *
   * Returns if e is live.
   */
  init_cached_extent_ret init_cached_extent(
    Transaction &t,
    CachedExtentRef e) final;

#ifdef UNIT_TESTS_BUILT
  check_child_trackers_ret check_child_trackers(Transaction &t) final;
#endif

  scan_mappings_ret scan_mappings(
    Transaction &t,
    laddr_t begin,
    laddr_t end,
    scan_mappings_func_t &&f) final;

  rewrite_extent_ret rewrite_extent(
    Transaction &t,
    CachedExtentRef extent) final;

  update_mapping_ret update_mapping(
    Transaction& t,
    LBAMapping mapping,
    extent_len_t prev_len,
    paddr_t prev_addr,
    LogicalChildNode&) final;

  update_mappings_ret update_mappings(
    Transaction& t,
    const std::list<LogicalChildNodeRef>& extents);

  get_physical_extent_if_live_ret get_physical_extent_if_live(
    Transaction &t,
    extent_types_t type,
    paddr_t addr,
    laddr_t laddr,
    extent_len_t len) final;

  complete_lba_mapping_ret complete_indirect_lba_mapping(
    Transaction &t,
    LBAMapping mapping) final;

  scan_mapped_space_ret scan_mapped_space(
    Transaction &t,
    scan_mapped_space_func_t &&f) final;

private:
  Cache &cache;

  struct {
    uint64_t num_alloc_extents = 0;
    uint64_t num_alloc_extents_iter_nexts = 0;
  } stats;

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

    static alloc_mapping_info_t create_zero(extent_len_t len) {
      return {
	L_ADDR_NULL,
	{
	  len,
	  pladdr_t(P_ADDR_ZERO),
	  EXTENT_DEFAULT_REF_COUNT,
	  0,
          extent_types_t::NONE
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
	  pladdr_t(intermediate_key),
	  EXTENT_DEFAULT_REF_COUNT,
	  0,	// crc will only be used and checked with LBA direct mappings
		// also see pin_to_extent(_by_type)
          extent_types_t::NONE
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
        {len,
         pladdr_t(paddr),
         refcount,
         checksum,
         extent.get_type()},
        &extent};
    }
  };

  op_context_t get_context(Transaction &t) {
    return op_context_t{cache, t};
  }

  seastar::metrics::metric_group metrics;
  void register_metrics();

  struct update_mapping_ret_bare_t {
    update_mapping_ret_bare_t()
	: update_mapping_ret_bare_t(LBACursorRef(nullptr)) {}

    update_mapping_ret_bare_t(LBACursorRef cursor)
	: ret(std::move(cursor)) {}

    update_mapping_ret_bare_t(
      laddr_t laddr, lba_map_val_t value, LBACursorRef &&cursor)
	: ret(removed_mapping_t{laddr, value, std::move(cursor)}) {}

    struct removed_mapping_t {
      laddr_t laddr;
      lba_map_val_t map_value;
      LBACursorRef next;
    };
    std::variant<removed_mapping_t, LBACursorRef> ret;

    bool is_removed_mapping() const {
      return ret.index() == 0;
    }

    bool is_alive_mapping() const {
      if (ret.index() == 1) {
	assert(std::get<1>(ret));
	return true;
      } else {
	return false;
      }
    }

    removed_mapping_t &get_removed_mapping() {
      assert(is_removed_mapping());
      return std::get<0>(ret);
    }

    const removed_mapping_t& get_removed_mapping() const {
      assert(is_removed_mapping());
      return std::get<0>(ret);
    }

    const LBACursor& get_cursor() const {
      assert(is_alive_mapping());
      return *std::get<1>(ret);
    }

    LBACursorRef take_cursor() {
      assert(is_alive_mapping());
      return std::move(std::get<1>(ret));
    }
  };

  mapping_update_result_t get_mapping_update_result(
    update_mapping_ret_bare_t &result) {
    if (result.is_removed_mapping()) {
      auto &v = result.get_removed_mapping();
      auto &val = v.map_value;
      return {v.laddr,
	      val.refcount,
	      val.pladdr,
	      val.len,
	      v.next->is_indirect()
		? LBAMapping::create_indirect(nullptr, std::move(v.next))
		: LBAMapping::create_direct(std::move(v.next))};
    } else {
      assert(result.is_alive_mapping());
      auto &c = result.get_cursor();
      assert(c.val);
      ceph_assert(!c.is_indirect());
      return {c.get_laddr(), c.val->refcount, 
	c.val->pladdr, c.val->len,
	LBAMapping::create_direct(result.take_cursor())};
    }
  }

  ref_update_result_t get_ref_update_result(
    update_mapping_ret_bare_t &result,
    std::optional<update_mapping_ret_bare_t> direct_result) {
    mapping_update_result_t primary_r = get_mapping_update_result(result);

    if (direct_result) {
      // only removing indirect mapping can have direct_result
      assert(result.is_removed_mapping());
      assert(result.get_removed_mapping().map_value.pladdr.is_laddr());
      auto direct_r = get_mapping_update_result(*direct_result);
      return ref_update_result_t{std::move(primary_r), std::move(direct_r)};
    }
    return ref_update_result_t{std::move(primary_r), std::nullopt};
  }

  using update_refcount_iertr = ref_iertr;
  using update_refcount_ret = update_refcount_iertr::future<
    mapping_update_result_t>;
  update_refcount_ret update_refcount(
    Transaction &t,
    std::variant<laddr_t, LBACursor*> addr_or_cursor,
    int delta);

  /**
   * _update_mapping
   *
   * Updates mapping, removes if f returns nullopt
   */
  using _update_mapping_iertr = ref_iertr;
  using _update_mapping_ret = ref_iertr::future<
    update_mapping_ret_bare_t>;
  using update_func_t = std::function<
    lba_map_val_t(const lba_map_val_t &v)
    >;
  _update_mapping_ret _update_mapping(
    Transaction &t,
    laddr_t addr,
    update_func_t &&f,
    LogicalChildNode*);
  _update_mapping_ret _update_mapping(
    Transaction &t,
    LBACursor &cursor,
    update_func_t &&f,
    LogicalChildNode*);

  struct insert_position_t {
    laddr_t laddr;
    LBABtree::iterator insert_iter;
  };
  enum class alloc_policy_t {
    deterministic, // no conflict
    linear_search,
  };
  using search_insert_position_iertr = base_iertr;
  using search_insert_position_ret =
      search_insert_position_iertr::future<insert_position_t>;
  search_insert_position_ret search_insert_position(
    op_context_t c,
    LBABtree &btree,
    laddr_t hint,
    extent_len_t length,
    alloc_policy_t policy);

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
    laddr_t hint,
    std::vector<alloc_mapping_info_t> &alloc_infos,
    alloc_policy_t policy);

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
    laddr_t hint,
    std::vector<alloc_mapping_info_t> &alloc_infos,
    alloc_policy_t policy);

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

  ref_ret _incref_extent(
    Transaction &t,
    laddr_t addr,
    int delta) {
    ceph_assert(delta > 0);
    return update_refcount(t, addr, delta
    ).si_then([](auto res) {
      return ref_update_result_t(std::move(res), std::nullopt);
    });
  }

  using _get_cursor_ret = get_mapping_iertr::future<LBACursorRef>;
  _get_cursor_ret get_cursor(
    op_context_t c,
    LBABtree& btree,
    laddr_t offset);

  _get_cursor_ret get_containing_cursor(
    op_context_t c,
    LBABtree &btree,
    laddr_t laddr);

  using _get_cursors_ret = get_mappings_iertr::future<std::list<LBACursorRef>>;
  _get_cursors_ret get_cursors(
    op_context_t c,
    LBABtree& btree,
    laddr_t offset,
    extent_len_t length);

  using resolve_indirect_cursor_ret = get_mappings_iertr::future<LBACursorRef>;
  resolve_indirect_cursor_ret resolve_indirect_cursor(
    op_context_t c,
    LBABtree& btree,
    const LBACursor& indirect_cursor);

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
