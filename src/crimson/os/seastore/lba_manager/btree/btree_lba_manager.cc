// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include <seastar/core/metrics.hh>

#include "include/buffer.h"
#include "crimson/os/seastore/lba_manager/btree/btree_lba_manager.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/logging.h"

SET_SUBSYS(seastore_lba);
/*
 * levels:
 * - INFO:  mkfs
 * - DEBUG: modification operations
 * - TRACE: read operations, DEBUG details
 */

namespace crimson::os::seastore {

enum class shadow_mapping_t : uint16_t {
  COLD_MIRROR = 1,
  INVALID = 2,
  MAX = L_ADDR_ALIGNMENT - 1
};

shadow_mapping_t get_shadow_mapping(laddr_t laddr) {
  assert(is_shadow_laddr(laddr));
  auto diff = laddr - p2align(laddr, L_ADDR_ALIGNMENT);
  assert(diff < static_cast<uint16_t>(shadow_mapping_t::INVALID));
  return static_cast<shadow_mapping_t>(diff);
}

laddr_t map_shadow_laddr(laddr_t laddr, shadow_mapping_t mapping) {
  assert(!is_shadow_laddr(laddr));
  assert(mapping < shadow_mapping_t::INVALID);
  return laddr + static_cast<uint16_t>(mapping);
}

laddr_t reset_shadow_mapping(laddr_t laddr) {
  return p2align(laddr, L_ADDR_ALIGNMENT);
}

template <typename T>
Transaction::tree_stats_t& get_tree_stats(Transaction &t)
{
  return t.get_lba_tree_stats();
}

template Transaction::tree_stats_t&
get_tree_stats<
  crimson::os::seastore::lba_manager::btree::LBABtree>(
  Transaction &t);

template <typename T>
phy_tree_root_t& get_phy_tree_root(root_t &r)
{
  return r.lba_root;
}

template phy_tree_root_t&
get_phy_tree_root<
  crimson::os::seastore::lba_manager::btree::LBABtree>(root_t &r);

template <>
const get_phy_tree_root_node_ret get_phy_tree_root_node<
  crimson::os::seastore::lba_manager::btree::LBABtree>(
  const RootBlockRef &root_block, op_context_t<laddr_t> c)
{
  auto lba_root = root_block->lba_root_node;
  if (lba_root) {
    ceph_assert(lba_root->is_initial_pending()
      == root_block->is_pending());
    return {true,
	    trans_intr::make_interruptible(
	      c.cache.get_extent_viewable_by_trans(c.trans, lba_root))};
  } else if (root_block->is_pending()) {
    auto &prior = static_cast<RootBlock&>(*root_block->get_prior_instance());
    lba_root = prior.lba_root_node;
    if (lba_root) {
      return {true,
	      trans_intr::make_interruptible(
		c.cache.get_extent_viewable_by_trans(c.trans, lba_root))};
    } else {
      return {false,
	      trans_intr::make_interruptible(
		Cache::get_extent_ertr::make_ready_future<
		  CachedExtentRef>())};
    }
  } else {
    return {false,
	    trans_intr::make_interruptible(
	      Cache::get_extent_ertr::make_ready_future<
		CachedExtentRef>())};
  }
}

template <typename ROOT>
void link_phy_tree_root_node(RootBlockRef &root_block, ROOT* lba_root) {
  root_block->lba_root_node = lba_root;
  ceph_assert(lba_root != nullptr);
  lba_root->root_block = root_block;
}

template void link_phy_tree_root_node(
  RootBlockRef &root_block, lba_manager::btree::LBAInternalNode* lba_root);
template void link_phy_tree_root_node(
  RootBlockRef &root_block, lba_manager::btree::LBALeafNode* lba_root);
template void link_phy_tree_root_node(
  RootBlockRef &root_block, lba_manager::btree::LBANode* lba_root);

template <>
void unlink_phy_tree_root_node<laddr_t>(RootBlockRef &root_block) {
  root_block->lba_root_node = nullptr;
}

}

namespace crimson::os::seastore::lba_manager::btree {

BtreeLBAManager::mkfs_ret
BtreeLBAManager::mkfs(
  Transaction &t)
{
  LOG_PREFIX(BtreeLBAManager::mkfs);
  INFOT("start", t);
  return cache.get_root(t).si_then([this, &t](auto croot) {
    assert(croot->is_mutation_pending());
    croot->get_root().lba_root = LBABtree::mkfs(croot, get_context(t));
    return mkfs_iertr::now();
  }).handle_error_interruptible(
    mkfs_iertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in BtreeLBAManager::mkfs"
    }
  );
}

BtreeLBAManager::get_mappings_ret
BtreeLBAManager::get_mappings(
  Transaction &t,
  laddr_t offset, extent_len_t length)
{
  LOG_PREFIX(BtreeLBAManager::get_mappings);
  TRACET("{}~{}", t, offset, length);
  auto c = get_context(t);
  return with_btree_state<LBABtree, lba_pin_list_t>(
    cache,
    c,
    [c, offset, length, FNAME](auto &btree, auto &ret) {
      return LBABtree::iterate_repeat(
	c,
	btree.upper_bound_right(c, offset),
	[&ret, offset, length, c, FNAME](auto &pos) {
	  if (pos.is_end() || pos.get_key() >= (offset + length)) {
	    TRACET("{}~{} done with {} results",
	           c.trans, offset, length, ret.size());
	    return typename LBABtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::yes);
	  }
	  TRACET("{}~{} got {}, {}, repeat ...",
	         c.trans, offset, length, pos.get_key(), pos.get_val());
	  ceph_assert(pos.get_val_end() > offset);
	  ret.push_back(pos.get_pin(c));
	  return typename LBABtree::iterate_repeat_ret_inner(
	    interruptible::ready_future_marker{},
	    seastar::stop_iteration::no);
	});
    });
}

BtreeLBAManager::get_mappings_ret
BtreeLBAManager::get_mappings(
  Transaction &t,
  laddr_list_t &&list)
{
  LOG_PREFIX(BtreeLBAManager::get_mappings);
  TRACET("{}", t, list);
  auto l = std::make_unique<laddr_list_t>(std::move(list));
  auto retptr = std::make_unique<lba_pin_list_t>();
  auto &ret = *retptr;
  return trans_intr::do_for_each(
    l->begin(),
    l->end(),
    [this, &t, &ret](const auto &p) {
      return this->get_mappings(t, p.first, p.second).si_then(
	[&ret](auto res) {
	  ret.splice(ret.end(), res, res.begin(), res.end());
	  return get_mappings_iertr::now();
	});
    }).si_then([l=std::move(l), retptr=std::move(retptr)]() mutable {
      return std::move(*retptr);
    });
}

BtreeLBAManager::get_mapping_ret
BtreeLBAManager::get_mapping(
  Transaction &t,
  laddr_t offset)
{
  LOG_PREFIX(BtreeLBAManager::get_mapping);
  TRACET("{}", t, offset);
  auto c = get_context(t);
  return with_btree_ret<LBABtree, LBAMappingRef>(
    cache,
    c,
    [FNAME, c, offset](auto &btree) {
      return btree.lower_bound(
	c, offset
      ).si_then([FNAME, offset, c](auto iter) -> get_mapping_ret {
	if (iter.is_end() || iter.get_key() != offset) {
	  ERRORT("laddr={} doesn't exist", c.trans, offset);
	  return crimson::ct_error::enoent::make();
	} else {
	  TRACET("{} got {}, {}",
	         c.trans, offset, iter.get_key(), iter.get_val());
	  auto e = iter.get_pin(c);
	  return get_mapping_ret(
	    interruptible::ready_future_marker{},
	    std::move(e));
	}
      });
    });
}

BtreeLBAManager::alloc_extent_ret
BtreeLBAManager::alloc_extent(
  Transaction &t,
  laddr_t hint,
  extent_len_t len,
  paddr_t addr,
  LogicalCachedExtent* nextent)
{
  struct state_t {
    laddr_t last_end;

    std::optional<typename LBABtree::iterator> insert_iter;
    std::optional<typename LBABtree::iterator> ret;

    state_t(laddr_t hint) : last_end(hint) {}
  };

  assert(!is_shadow_laddr(hint));
  assert(!is_shadow_laddr(hint + len));
  LOG_PREFIX(BtreeLBAManager::alloc_extent);
  TRACET("{}~{}, hint={}", t, addr, len, hint);
  auto c = get_context(t);
  ++stats.num_alloc_extents;
  auto lookup_attempts = stats.num_alloc_extents_iter_nexts;
  return crimson::os::seastore::with_btree_state<LBABtree, state_t>(
    cache,
    c,
    hint,
    [this, FNAME, c, hint, len, addr, lookup_attempts,
    &t, nextent](auto &btree, auto &state) {
      return LBABtree::iterate_repeat(
	c,
	btree.upper_bound_right(c, hint),
	[this, &state, len, addr, &t, hint, FNAME, lookup_attempts](auto &pos) {
	  ++stats.num_alloc_extents_iter_nexts;
	  if (pos.is_end()) {
	    DEBUGT("{}~{}, hint={}, state: end, done with {} attempts, insert at {}",
                   t, addr, len, hint,
                   stats.num_alloc_extents_iter_nexts - lookup_attempts,
                   state.last_end);
	    state.insert_iter = pos;
	    return typename LBABtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::yes);
	  } else if (pos.get_key() >= (state.last_end + len)) {
	    DEBUGT("{}~{}, hint={}, state: {}~{}, done with {} attempts, insert at {} -- {}",
                   t, addr, len, hint,
                   pos.get_key(), pos.get_val().len,
                   stats.num_alloc_extents_iter_nexts - lookup_attempts,
                   state.last_end,
                   pos.get_val());
	    state.insert_iter = pos;
	    return typename LBABtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::yes);
	  } else {
	    if (!is_shadow_laddr(pos.get_key())) {
	      state.last_end = pos.get_key() + pos.get_val().len;
	    }
	    TRACET("{}~{}, hint={}, state: {}~{}, repeat ... -- {}",
                   t, addr, len, hint,
                   pos.get_key(), pos.get_val().len,
                   pos.get_val());
	    return typename LBABtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::no);
	  }
	}).si_then([FNAME, c, addr, len, hint, &btree, &state, nextent] {
	  return btree.insert(
	    c,
	    *state.insert_iter,
	    state.last_end,
	    lba_map_val_t{len, addr, 1, 0},
	    nextent
	  ).si_then([&state, FNAME, c, addr, len, hint, nextent](auto &&p) {
	    auto [iter, inserted] = std::move(p);
	    TRACET("{}~{}, hint={}, inserted at {}",
	           c.trans, addr, len, hint, state.last_end);
	    if (nextent) {
	      assert(!is_shadow_laddr(iter.get_key()));
	      nextent->set_laddr(iter.get_key());
	    }
	    ceph_assert(inserted);
	    state.ret = iter;
	  });
	});
    }).si_then([c](auto &&state) {
      return state.ret->get_pin(c);
    });
}

BtreeLBAManager::alloc_extent_ret
BtreeLBAManager::alloc_shadow_extent(
  Transaction &t,
  laddr_t laddr,
  extent_len_t len,
  paddr_t paddr,
  LogicalCachedExtent *nextent)
{
  assert(enable_shadow_entry);
  assert(!is_shadow_laddr(laddr));
  assert(!is_shadow_laddr(laddr + len));
  auto c = get_context(t);
  return with_btree_ret<LBABtree, LBAMappingRef>(
    cache,
    c,
    [c, laddr, len, paddr, nextent](LBABtree &btree) {
      return btree.lower_bound(c, laddr
      ).si_then([c, laddr, len, paddr, nextent, &btree](auto iter) {
        assert(!iter.is_end());
	assert(iter.get_key() == laddr);
        return iter.next(c
        ).si_then([c, laddr, len, paddr, nextent, iter, &btree](auto niter) {
	  LOG_PREFIX(BtreeLBAManager::alloc_shadow_extent);
	  auto shadow_laddr = map_shadow_laddr(laddr, shadow_mapping_t::COLD_MIRROR);
	  if (niter.get_key() == shadow_laddr) {
	    ERRORT("shadow_laddr {} already exist", c.trans, shadow_laddr);
	    ceph_abort();
	  }
          auto val = iter.get_val();
	  assert(len == val.len);
          val.paddr = paddr;
          return btree.insert(
	    c,
	    niter,
	    shadow_laddr,
	    val,
	    nextent
          ).si_then([c, nextent](auto iter) {
	    assert(iter.second);
	    if (nextent) {
	      nextent->set_laddr(iter.first.get_key());
	    }
	    return iter.first.get_pin(c);
          });
        });
      });
    });
}

BtreeLBAManager::demote_region_ret
BtreeLBAManager::demote_region(
  Transaction &t,
  laddr_t laddr,
  extent_len_t length,
  extent_len_t max_demote_size,
  retire_promotion_func_t retire_func,
  update_nextent_func_t update_func)
{
  struct state_t {
    std::optional<LBABtree::iterator> iter;
    laddr_t cur_addr = L_ADDR_NULL;
    LogicalCachedExtent *nextent = nullptr;
    retire_promotion_func_t retire_promotion;
    update_nextent_func_t update_nextent;
    demote_region_res_t res;
  };

  LOG_PREFIX(BtreeLBAManager::demote_region);
  TRACET("demote {}~{} max_demote_size={}",
	 t, laddr, length, max_demote_size);
  auto c = get_context(t);
  return with_btree_state<LBABtree, state_t>(
    cache,
    c,
    [c, laddr, length, max_demote_size,
     FNAME, retire_func=std::move(retire_func),
     update_func=std::move(update_func)](LBABtree &btree, state_t &state) {
      state.retire_promotion = std::move(retire_func);
      state.update_nextent = std::move(update_func);

      return btree.upper_bound_right(
        c, laddr
      ).si_then([c, laddr, length, max_demote_size,
		 &state, &btree, FNAME](auto iter) {
	ceph_assert(!iter.is_end());
	ceph_assert(laddr == iter.get_key());
	state.iter.emplace(iter);

	return trans_intr::repeat([c, laddr, length, max_demote_size, &state, &btree, FNAME] {
	  if (state.iter->is_end() ||
	      (state.iter->get_key() >= laddr + length) ||
	      (state.res.demote_size >= max_demote_size)) {
            state.res.completed = state.iter->is_end() ||
              state.iter->get_key() >= laddr + length;
            return demote_region_iertr::make_ready_future<
	      seastar::stop_iteration>(seastar::stop_iteration::yes);
	  }

	  DEBUGT("handle {}~{} {}",
		 c.trans,
		 state.iter->get_key(),
		 state.iter->get_val().len,
		 state.iter->get_val().paddr);
	  state.cur_addr = state.iter->get_key();
	  ceph_assert(!is_shadow_laddr(state.cur_addr));
	  state.res.proceed_size += state.iter->get_val().len;

	  return state.iter->next(c
	  ).si_then([c, &state, &btree, FNAME](auto shadow_iter) {
	    if (!shadow_iter.is_end() && is_shadow_laddr(shadow_iter.get_key())) {
	      DEBUGT("handle shadow {}~{} {}",
		     c.trans,
		     shadow_iter.get_key(),
		     shadow_iter.get_val().len,
		     shadow_iter.get_val().paddr);
	      ceph_assert(shadow_iter.get_key() ==
			  map_shadow_laddr(state.cur_addr,
					   shadow_mapping_t::COLD_MIRROR));
	      state.res.demote_size += shadow_iter.get_val().len;

	      // remap existing cold extent and remove shadow entry
	      auto fun = [c, shadow_iter, &state, &btree] {
		auto update = [c, shadow_iter, &state, &btree](LogicalCachedExtent *extent) {
		  return state.update_nextent(
		    extent,
		    shadow_iter.get_val().paddr,
		    shadow_iter.get_val().len
		  ).si_then([c, shadow_iter, &btree, &state](auto nextent) {
		    nextent->set_laddr(state.cur_addr);
		    state.nextent = nextent;
		    ceph_assert(nextent->get_paddr() == shadow_iter.get_val().paddr);
		    return btree.remove(c, shadow_iter
		    ).si_then([c](auto iter) {
		      return iter.prev(c);
		    }).si_then([&state](auto iter) {
		      ceph_assert(iter.get_key() == state.cur_addr);
		      ceph_assert(!is_shadow_laddr(state.cur_addr));
		      state.iter.emplace(iter);
		    });
		  });
		};

		auto ext = shadow_iter.get_pin(c)->get_logical_extent(c.trans);
		if (ext.has_child()) {
		  return trans_intr::make_interruptible(
		    std::move(ext.get_child_fut())
		  ).si_then([update=std::move(update)](auto extent) {
		    return update(extent.get());
		  });
		} else {
		  return update(nullptr);
		}
	      };

	      return fun().si_then([c, &btree, &state] {
		ceph_assert(state.iter->get_key() == state.cur_addr);
		ceph_assert(!is_shadow_laddr(state.cur_addr));
		auto val = state.iter->get_val();
		return state.retire_promotion(val.paddr, val.len
		).si_then([c, val, &btree, &state]() mutable {
		  val.paddr = state.nextent->get_paddr();
		  return btree.update(
		    c,
		    *state.iter,
		    val,
		    state.nextent
		  ).si_then([c, &state](auto iter) {
		    return iter.next(c).si_then([&state](auto iter) {
		      state.iter.emplace(iter);
		      return demote_region_iertr::make_ready_future<
			seastar::stop_iteration>(seastar::stop_iteration::no);
		    });
		  });
		});
	      });
	    } else {
	      state.iter.emplace(shadow_iter);
	      return demote_region_iertr::make_ready_future<
		seastar::stop_iteration>(seastar::stop_iteration::no);
	    }
	  });
	});
      });
    }).si_then([](auto &&state) {
      return std::move(state.res);
    });
}

static bool is_lba_node(const CachedExtent &e)
{
  return is_lba_node(e.get_type());
}

BtreeLBAManager::base_iertr::template future<>
_init_cached_extent(
  op_context_t<laddr_t> c,
  const CachedExtentRef &e,
  LBABtree &btree,
  bool &ret)
{
  if (e->is_logical()) {
    auto logn = e->cast<LogicalCachedExtent>();
    return btree.lower_bound(
      c,
      logn->get_laddr()
    ).si_then([e, c, logn, &ret](auto iter) {
      LOG_PREFIX(BtreeLBAManager::init_cached_extent);
      if (!iter.is_end() &&
	  iter.get_key() == logn->get_laddr() &&
	  iter.get_val().paddr == logn->get_paddr()) {
	assert(!iter.get_leaf_node()->is_pending());
	iter.get_leaf_node()->link_child(logn.get(), iter.get_leaf_pos());
	logn->set_laddr(iter.get_pin(c)->get_key());
	ceph_assert(iter.get_val().len == e->get_length());
	DEBUGT("logical extent {} live", c.trans, *logn);
	ret = true;
      } else {
	DEBUGT("logical extent {} not live", c.trans, *logn);
	ret = false;
      }
    });
  } else {
    return btree.init_cached_extent(c, e
    ).si_then([&ret](bool is_alive) {
      ret = is_alive;
    });
  }
}

BtreeLBAManager::init_cached_extent_ret
BtreeLBAManager::init_cached_extent(
  Transaction &t,
  CachedExtentRef e)
{
  LOG_PREFIX(BtreeLBAManager::init_cached_extent);
  TRACET("{}", t, *e);
  return seastar::do_with(bool(), [this, e, &t](bool &ret) {
    auto c = get_context(t);
    return with_btree<LBABtree>(
      cache, c,
      [c, e, &ret](auto &btree) -> base_iertr::future<> {
	LOG_PREFIX(BtreeLBAManager::init_cached_extent);
	DEBUGT("extent {}", c.trans, *e);
	return _init_cached_extent(c, e, btree, ret);
      }
    ).si_then([&ret] { return ret; });
  });
}

BtreeLBAManager::check_child_trackers_ret
BtreeLBAManager::check_child_trackers(
  Transaction &t) {
  auto c = get_context(t);
  return with_btree<LBABtree>(
    cache, c,
    [c](auto &btree) {
    return btree.check_child_trackers(c);
  });
}

BtreeLBAManager::scan_mappings_ret
BtreeLBAManager::scan_mappings(
  Transaction &t,
  laddr_t begin,
  laddr_t end,
  scan_mappings_func_t &&f)
{
  LOG_PREFIX(BtreeLBAManager::scan_mappings);
  DEBUGT("begin: {}, end: {}", t, begin, end);

  auto c = get_context(t);
  return with_btree<LBABtree>(
    cache,
    c,
    [c, f=std::move(f), begin, end](auto &btree) mutable {
      return LBABtree::iterate_repeat(
	c,
	btree.upper_bound_right(c, begin),
	[f=std::move(f), begin, end](auto &pos) {
	  if (pos.is_end() || pos.get_key() >= end) {
	    return typename LBABtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::yes);
	  }
	  ceph_assert(pos.get_val_end() > begin);
	  f(pos.get_key(), pos.get_val().paddr, pos.get_val().len);
	  return typename LBABtree::iterate_repeat_ret_inner(
	    interruptible::ready_future_marker{},
	    seastar::stop_iteration::no);
	});
    });
}

BtreeLBAManager::rewrite_extent_ret
BtreeLBAManager::rewrite_extent(
  Transaction &t,
  CachedExtentRef extent)
{
  LOG_PREFIX(BtreeLBAManager::rewrite_extent);
  if (extent->has_been_invalidated()) {
    ERRORT("extent has been invalidated -- {}", t, *extent);
    ceph_abort();
  }
  assert(!extent->is_logical());

  if (is_lba_node(*extent)) {
    DEBUGT("rewriting lba extent -- {}", t, *extent);
    auto c = get_context(t);
    return with_btree<LBABtree>(
      cache,
      c,
      [c, extent](auto &btree) mutable {
	return btree.rewrite_extent(c, extent);
      });
  } else {
    DEBUGT("skip non lba extent -- {}", t, *extent);
    return rewrite_extent_iertr::now();
  }
}

BtreeLBAManager::update_mapping_ret
BtreeLBAManager::update_mapping(
  Transaction& t,
  laddr_t laddr,
  paddr_t prev_addr,
  paddr_t addr,
  LogicalCachedExtent *nextent)
{
  LOG_PREFIX(BtreeLBAManager::update_mapping);
  TRACET("laddr={}, paddr {} => {}", t, laddr, prev_addr, addr);
  return _update_mapping(
    t,
    laddr,
    [prev_addr, addr](
      const lba_map_val_t &in) {
      assert(!addr.is_null());
      lba_map_val_t ret = in;
      ceph_assert(in.paddr == prev_addr);
      ret.paddr = addr;
      return ret;
    },
    nextent
  ).si_then([&t, laddr, prev_addr, addr, FNAME](auto result) {
      DEBUGT("laddr={}, paddr {} => {} done -- {}",
             t, laddr, prev_addr, addr, result.val);
    },
    update_mapping_iertr::pass_further{},
    /* ENOENT in particular should be impossible */
    crimson::ct_error::assert_all{
      "Invalid error in BtreeLBAManager::update_mapping"
    }
  );
}

BtreeLBAManager::get_physical_extent_if_live_ret
BtreeLBAManager::get_physical_extent_if_live(
  Transaction &t,
  extent_types_t type,
  paddr_t addr,
  laddr_t laddr,
  extent_len_t len)
{
  LOG_PREFIX(BtreeLBAManager::get_physical_extent_if_live);
  DEBUGT("{}, laddr={}, paddr={}, length={}",
         t, type, laddr, addr, len);
  ceph_assert(is_lba_node(type));
  auto c = get_context(t);
  return with_btree_ret<LBABtree, CachedExtentRef>(
    cache,
    c,
    [c, type, addr, laddr, len](auto &btree) {
      if (type == extent_types_t::LADDR_INTERNAL) {
	return btree.get_internal_if_live(c, addr, laddr, len);
      } else {
	assert(type == extent_types_t::LADDR_LEAF ||
	       type == extent_types_t::DINK_LADDR_LEAF);
	return btree.get_leaf_if_live(c, addr, laddr, len);
      }
    });
}

void BtreeLBAManager::register_metrics()
{
  LOG_PREFIX(BtreeLBAManager::register_metrics);
  DEBUG("start");
  stats = {};
  namespace sm = seastar::metrics;
  metrics.add_group(
    "LBA",
    {
      sm::make_counter(
        "alloc_extents",
        stats.num_alloc_extents,
        sm::description("total number of lba alloc_extent operations")
      ),
      sm::make_counter(
        "alloc_extents_iter_nexts",
        stats.num_alloc_extents_iter_nexts,
        sm::description("total number of iterator next operations during extent allocation")
      ),
    }
  );
}

BtreeLBAManager::update_refcount_ret
BtreeLBAManager::update_refcount(
  Transaction &t,
  laddr_t addr,
  int delta)
{
  LOG_PREFIX(BtreeLBAManager::update_refcount);
  TRACET("laddr={}, delta={}", t, addr, delta);
  assert(!is_shadow_laddr(addr));
  auto update_mapping = [this, &t, delta](laddr_t addr,
					  std::optional<LBABtree::iterator> iter) {
    return _update_mapping(
      t,
      addr,
      [delta](const lba_map_val_t &in) {
	lba_map_val_t out = in;
	ceph_assert((int)out.refcount + delta >= 0);
	out.refcount += delta;
	return out;
      },
      nullptr,
      iter);
  };
  return update_mapping(addr, std::nullopt
  ).si_then([&t, addr, delta, FNAME,
	     update_mapping=std::move(update_mapping)](auto result) {
    DEBUGT("laddr={}, delta={} done -- {}", t, addr, delta, result.val);
    auto res = ref_update_result_t{
      result.val.refcount,
      result.val.paddr,
      P_ADDR_NULL,
      result.val.len
    };
    if (result.shadow_iter) {
      res.shadow_addr = result.shadow_iter->get_val().paddr;
      return update_mapping(
        map_shadow_laddr(addr, shadow_mapping_t::COLD_MIRROR),
	result.shadow_iter
      ).si_then([res](auto result) mutable {
	return ref_iertr::make_ready_future<
	  ref_update_result_t>(res);
      });
    }
    return ref_iertr::make_ready_future<
      ref_update_result_t>(res);
  });
}

BtreeLBAManager::_update_mapping_ret
BtreeLBAManager::_update_mapping(
  Transaction &t,
  laddr_t addr,
  update_func_t &&f,
  LogicalCachedExtent* nextent,
  std::optional<LBABtree::iterator> iter)
{
  auto c = get_context(t);
  return with_btree_ret<LBABtree, update_res_t>(
    cache,
    c,
    [c, addr, nextent, iter, f=std::move(f)](auto &btree) mutable {
      using fetch_ret = _update_mapping_iertr::future<LBABtree::iterator>;
      auto fetch_iter = [c, iter, addr, &btree]() -> fetch_ret {
	if (iter == std::nullopt) {
	  return btree.lower_bound(c, addr).si_then([c, addr](auto iter) -> fetch_ret {
	    if (iter.is_end() || iter.get_key() != addr) {
	      LOG_PREFIX(BtreeLBAManager::_update_mapping);
	      ERRORT("laddr={} doesn't exist", c.trans, addr);
	      return crimson::ct_error::enoent::make();
	    } else {
	      return update_mapping_iertr::make_ready_future<
		LBABtree::iterator>(iter);
	    }
	  });
	} else {
	  ceph_assert(iter->get_key() == addr);
	  return update_mapping_iertr::make_ready_future<
	    LBABtree::iterator>(*iter);
	}
      };

      return fetch_iter().si_then([c, addr, nextent, &btree, f=std::move(f)](LBABtree::iterator iter) -> _update_mapping_ret {
	auto ret = f(iter.get_val());
	if (ret.refcount == 0) {
	  return btree.remove(c, iter).si_then([ret, addr](auto iter) {
	    if (!iter.is_end() && !is_shadow_laddr(addr) &&
		iter.get_key() == map_shadow_laddr(addr, shadow_mapping_t::COLD_MIRROR)) {
	      return update_res_t(ret, iter);
	    } else {
	      return update_res_t(ret, std::nullopt);
	    }
	  });
	} else {
	  return btree.update(c, iter, ret, nextent).si_then([c, ret, addr](auto iter) -> _update_mapping_ret {
	    if (is_shadow_laddr(addr)) {
	      return _update_mapping_iertr::make_ready_future<
		update_res_t>(ret, std::nullopt);
	    } else {
	      return iter.next(c).si_then([ret, addr](auto iter) {
		if (!iter.is_end() &&
		    iter.get_key() == map_shadow_laddr(addr, shadow_mapping_t::COLD_MIRROR)) {
		  return update_res_t(ret, iter);
		} else {
		  return update_res_t(ret, std::nullopt);
		}
	      });
	    }
	  });
	}
      });
    });
}

}
