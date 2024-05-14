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
    [c, offset, length, FNAME, this](auto &btree, auto &ret) {
      return seastar::do_with(
	std::list<BtreeLBAMappingRef>(),
	[offset, length, c, FNAME, this, &ret, &btree](auto &pin_list) {
	return LBABtree::iterate_repeat(
	  c,
	  btree.upper_bound_right(c, offset),
	  [&pin_list, offset, length, c, FNAME](auto &pos) {
	    if (pos.is_end() || pos.get_key() >= (offset + length)) {
	      TRACET("{}~{} done with {} results",
		     c.trans, offset, length, pin_list.size());
	      return LBABtree::iterate_repeat_ret_inner(
		interruptible::ready_future_marker{},
		seastar::stop_iteration::yes);
	    }
	    TRACET("{}~{} got {}, {}, repeat ...",
		   c.trans, offset, length, pos.get_key(), pos.get_val());
	    ceph_assert((pos.get_key() + pos.get_val().len) > offset);
	    pin_list.push_back(pos.get_pin(c));
	    return LBABtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::no);
	  }).si_then([this, &ret, c, &pin_list] {
	    return _get_original_mappings(c, pin_list
	    ).si_then([&ret](auto _ret) {
	      ret = std::move(_ret);
	    });
	  });
	});
    });
}

BtreeLBAManager::_get_original_mappings_ret
BtreeLBAManager::_get_original_mappings(
  op_context_t<laddr_t> c,
  std::list<BtreeLBAMappingRef> &pin_list)
{
  return seastar::do_with(
    lba_pin_list_t(),
    [this, c, &pin_list](auto &ret) {
    return trans_intr::do_for_each(
      pin_list,
      [this, c, &ret](auto &pin) {
	LOG_PREFIX(BtreeLBAManager::get_mappings);
	if (pin->get_raw_val().is_paddr()) {
	  ret.emplace_back(std::move(pin));
	  return get_mappings_iertr::now();
	}
	TRACET(
	  "getting original mapping for indirect mapping {}~{}",
	  c.trans, pin->get_key(), pin->get_length());
	auto original_key = pin->get_raw_val().get_non_snap_laddr(pin->get_key());
	return this->get_mappings(
	  c.trans, original_key, pin->get_length()
	).si_then([&pin, &ret, original_key, c](auto new_pin_list) {
	  LOG_PREFIX(BtreeLBAManager::get_mappings);
	  assert(new_pin_list.size() == 1);
	  auto &new_pin = new_pin_list.front();
	  auto intermediate_key = original_key;
	  assert(!new_pin->is_indirect());
	  assert(new_pin->get_key() <= intermediate_key);
	  assert(new_pin->get_key() + new_pin->get_length() >=
	  intermediate_key + pin->get_length());

	  TRACET("Got mapping {}~{} for indirect mapping {}~{}, "
	    "intermediate_key {}",
	    c.trans,
	    new_pin->get_key(), new_pin->get_length(),
	    pin->get_key(), pin->get_length(),
	    original_key);
	  auto &btree_new_pin = static_cast<BtreeLBAMapping&>(*new_pin);
	  btree_new_pin.make_indirect(
	    pin->get_key(),
	    pin->get_length(),
	    original_key);
	  ret.emplace_back(std::move(new_pin));
	  return seastar::now();
	}).handle_error_interruptible(
	  crimson::ct_error::input_output_error::pass_further{},
	  crimson::ct_error::assert_all("unexpected enoent")
	);
      }
    ).si_then([&ret] {
      return std::move(ret);
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

BtreeLBAManager::base_iertr::future<bool>
BtreeLBAManager::prefix_contains_shadow_mapping(
  Transaction &t,
  laddr_t laddr)
{
  auto c = get_context(t);
  return with_btree_state<LBABtree, bool>(
    cache,
    c,
    false,
    [c, laddr](LBABtree &btree, bool &res) {
      return btree.lower_bound(
        c, laddr.with_shadow()
      ).si_then([laddr, &res](LBABtree::iterator iter) {
	res = !iter.is_end() &&
	  laddr.get_object_prefix() == iter.get_key().get_object_prefix();
      });
    });
}

BtreeLBAManager::get_mapping_ret
BtreeLBAManager::get_mapping(
  Transaction &t,
  laddr_t offset)
{
  LOG_PREFIX(BtreeLBAManager::get_mapping);
  TRACET("{}", t, offset);
  return _get_mapping(t, offset
  ).si_then([](auto pin) {
    return get_mapping_iertr::make_ready_future<LBAMappingRef>(std::move(pin));
  });
}

BtreeLBAManager::_get_mapping_ret
BtreeLBAManager::_get_mapping(
  Transaction &t,
  laddr_t offset)
{
  LOG_PREFIX(BtreeLBAManager::_get_mapping);
  TRACET("{}", t, offset);
  auto c = get_context(t);
  return with_btree_ret<LBABtree, BtreeLBAMappingRef>(
    cache,
    c,
    [FNAME, c, offset, this](auto &btree) {
      return btree.lower_bound(
	c, offset
      ).si_then([FNAME, offset, c](auto iter) -> _get_mapping_ret {
	if (iter.is_end() || iter.get_key() != offset) {
	  ERRORT("laddr={} doesn't exist", c.trans, offset);
	  return crimson::ct_error::enoent::make();
	} else {
	  TRACET("{} got {}, {}",
	         c.trans, offset, iter.get_key(), iter.get_val());
	  auto e = iter.get_pin(c);
	  return _get_mapping_ret(
	    interruptible::ready_future_marker{},
	    std::move(e));
	}
      }).si_then([this, c](auto pin) -> _get_mapping_ret {
	if (pin->get_raw_val().is_laddr()) {
	  return seastar::do_with(
	    std::move(pin),
	    [this, c](auto &pin) {
	    return _get_mapping(
	      c.trans, pin->get_raw_val().get_non_snap_laddr(pin->get_key())
	    ).si_then([&pin](auto new_pin) {
	      ceph_assert(pin->get_length() == new_pin->get_length());
	      new_pin->make_indirect(
		pin->get_key(),
		pin->get_length());
	      return new_pin;
	    });
	  });
	} else {
	  return get_mapping_iertr::make_ready_future<BtreeLBAMappingRef>(std::move(pin));
	}
      });
    });
}

BtreeLBAManager::alloc_extents_ret
BtreeLBAManager::_alloc_extents(
  Transaction &t,
  laddr_t hint,
  std::vector<alloc_mapping_info_t> &alloc_infos,
  extent_ref_count_t refcount,
  bool determinsitic)
{
  extent_len_t total_len = 0;
  for (auto &info : alloc_infos) {
    total_len += info.len;
  }
  struct state_t {
    laddr_t last_end;

    std::optional<typename LBABtree::iterator> insert_iter;
    std::optional<typename LBABtree::iterator> ret;

    state_t(laddr_t hint) : last_end(hint) {}
  };

  LOG_PREFIX(BtreeLBAManager::_alloc_extents);
  TRACET("{}~{}, hint={}, num of extents: {}, refcount={}",
    t, alloc_infos.front().val, total_len, hint, alloc_infos.size(), refcount);

  auto c = get_context(t);
  stats.num_alloc_extents += alloc_infos.size();
  return seastar::do_with(
    std::vector<LBAMappingRef>(),
    [this, FNAME, &alloc_infos, hint, &t, total_len, c,
    refcount, determinsitic](auto &rets) {
    return crimson::os::seastore::with_btree_state<LBABtree, state_t>(
      cache,
      c,
      hint,
      [this, c, hint, total_len, addr=alloc_infos.front().val, &rets, refcount,
      determinsitic, &t, &alloc_infos, FNAME](auto &btree, auto &state) {
	return search_insert_pos(t, btree, hint, total_len, determinsitic
	).si_then([c, addr, hint, &btree, &state, &alloc_infos,
		   total_len, &rets, refcount, FNAME](auto pos) {
	state.last_end = pos.laddr;
	state.insert_iter = pos.iter;
	return trans_intr::do_for_each(
	  alloc_infos,
	  [c, addr, hint, &btree, &state, FNAME,
	  total_len, &rets, refcount](auto &alloc_info) {
	  return btree.insert(
	    c,
	    *state.insert_iter,
	    state.last_end,
	    lba_map_val_t{
	      alloc_info.len,
	      pladdr_t(alloc_info.val),
	      refcount,
	      alloc_info.checksum},
	    alloc_info.extent
	  ).si_then([&state, c, addr, total_len, hint, FNAME,
		    &alloc_info, &rets](auto &&p) {
	    auto [iter, inserted] = std::move(p);
	    TRACET("{}~{}, hint={}, inserted at {}",
		   c.trans, addr, total_len, hint, state.last_end);
	    if (alloc_info.extent) {
	      ceph_assert(alloc_info.val.is_paddr());
	      alloc_info.extent->set_laddr(iter.get_key());
	    }
	    ceph_assert(inserted);
	    rets.emplace_back(iter.get_pin(c));
	    return iter.next(c).si_then([&state, &alloc_info](auto it) {
	      state.insert_iter = it;
	      state.last_end += alloc_info.len;
	    });
	  });
	});
      });
    }).si_then([&rets](auto &&state) {
      return alloc_extent_iertr::make_ready_future<
	std::vector<LBAMappingRef>>(std::move(rets));
    });
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
	  iter.get_val().pladdr.is_paddr() &&
	  iter.get_val().pladdr.get_paddr() == logn->get_paddr()) {
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
	  ceph_assert((pos.get_key() + pos.get_val().len) > begin);
	  if (pos.get_val().pladdr.is_paddr()) {
	    f(pos.get_key(), pos.get_val().pladdr.get_paddr(), pos.get_val().len);
	  }
	  return LBABtree::iterate_repeat_ret_inner(
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
  extent_len_t prev_len,
  paddr_t prev_addr,
  extent_len_t len,
  paddr_t addr,
  uint32_t checksum,
  LogicalCachedExtent *nextent,
  std::optional<bool> has_shadow)
{
  LOG_PREFIX(BtreeLBAManager::update_mapping);
  TRACET("laddr={}, paddr {} => {}", t, laddr, prev_addr, addr);
  return _update_mapping(
    t,
    laddr,
    [prev_addr, addr, prev_len, len, checksum, has_shadow](
      const lba_map_val_t &in) {
      assert(!addr.is_null());
      lba_map_val_t ret = in;
      ceph_assert(in.pladdr.is_paddr());
      ceph_assert(in.pladdr.get_paddr() == prev_addr);
      ceph_assert(in.len == prev_len);
      ret.pladdr = addr;
      ret.len = len;
      ret.checksum = checksum;
      if (has_shadow) {
	ret.pladdr.has_shadow = *has_shadow;
      }
      return ret;
    },
    nextent
  ).si_then([&t, laddr, prev_addr, addr, FNAME](auto res) {
      auto &result = res.map_value;
      DEBUGT("laddr={}, paddr {} => {} done -- {}",
             t, laddr, prev_addr, addr, result);
      return update_mapping_iertr::make_ready_future<
	extent_ref_count_t>(result.refcount);
    },
    update_mapping_iertr::pass_further{},
    /* ENOENT in particular should be impossible */
    crimson::ct_error::assert_all{
      "Invalid error in BtreeLBAManager::update_mapping"
    }
  );
}

BtreeLBAManager::demote_region_ret
BtreeLBAManager::demote_region(
  Transaction &t,
  laddr_t laddr,
  extent_len_t max_demote_size,
  retire_promotion_func_t retire_func,
  update_nextent_func_t update_func)
{
  struct mapping_t {
    laddr_t laddr;
    paddr_t paddr;
    LogicalCachedExtent *nextent;
  };
  struct state_t {
    state_t(laddr_t laddr, extent_len_t max_demote_size,
            retire_promotion_func_t &&retire_func,
            update_nextent_func_t &&update_func)
      : prefix(laddr), max_demote_size(max_demote_size), counter(0),
	iter(std::nullopt), retire_promotion(std::move(retire_func)),
	update_nextent(std::move(update_func)), res() {}
    state_t(state_t &&) = default;

    laddr_t prefix;
    extent_len_t max_demote_size;
    std::list<mapping_t> shadow_mappings;
    int counter;
    std::optional<LBABtree::iterator> iter;
    retire_promotion_func_t retire_promotion;
    update_nextent_func_t update_nextent;
    demote_region_result_t res;
  };

  LOG_PREFIX(BtreeLBAManager::demote_region);
  TRACET("demote {} max_demote_size={}",
	 t, laddr, max_demote_size);
  assert(laddr == laddr.get_object_prefix());
  auto c = get_context(t);
  return with_btree_state<LBABtree, state_t>(
    cache,
    c,
    state_t(laddr, max_demote_size, std::move(retire_func), std::move(update_func)),
    [c, FNAME](LBABtree &btree, state_t &state) {
      return btree.upper_bound_right(
        c, state.prefix.with_shadow()
      ).si_then([c, &btree, &state, FNAME](LBABtree::iterator iter) {
	ceph_assert(!iter.is_end());
	state.iter.emplace(iter);
	return trans_intr::repeat([c, &btree, &state, FNAME] {
	  if (state.iter->is_end() ||
	      (state.iter->get_key().get_object_prefix() != state.prefix.get_object_prefix()) ||
	      (state.res.demoted_size >= state.max_demote_size)) {
	    TRACET("finish demote", c.trans);
	    state.res.completed = state.iter->is_end() ||
	      (state.iter->get_key().get_object_prefix() != state.prefix.get_object_prefix());
	    return demote_region_iertr::make_ready_future<
	      seastar::stop_iteration>(seastar::stop_iteration::yes);
	  }
	  ceph_assert(state.iter->get_key().is_shadow());
	  DEBUGT("demoting {}~{} at {}",
		 c.trans,
		 state.iter->get_key(),
		 state.iter->get_val().len,
		 state.iter->get_val().pladdr);
	  state.res.demoted_size += state.iter->get_val().len;
	  auto fun = [c, &btree, &state] {
	    auto update = [c, &btree, &state](LogicalCachedExtent *nextent) {
	      return state.update_nextent(
	        nextent,
	        state.iter->get_val().pladdr.get_paddr(),
	        state.iter->get_val().len
	      ).si_then([c, &btree, &state](LogicalCachedExtent *nextent) {
		auto key = state.iter->get_key();
		auto paddr = state.iter->get_val().pladdr.get_paddr();
		nextent->set_laddr(key.without_shadow());
		state.shadow_mappings.push_back(mapping_t{key, paddr, nextent});
		return btree.remove(
	          c, *state.iter
	        ).si_then([&state](LBABtree::iterator iter) {
		  state.iter.emplace(iter);
		});
	      });
	    };
	    auto ext = state.iter->get_pin(c)->get_logical_extent(c.trans);
	    if (ext.has_child()) {
	      return trans_intr::make_interruptible(
	        std::move(ext.get_child_fut())
	      ).si_then([update=std::move(update)](auto nextent) {
		return update(nextent.get());
	      });
	    } else {
	      return update(nullptr);
	    }
	  };
	  return fun().si_then([] {
	    return demote_region_iertr::make_ready_future<
	      seastar::stop_iteration>(seastar::stop_iteration::no);
	  });
	}).si_then([c, &btree, &state, FNAME] {
	  TRACET("finish demote {} shadow mappings",
		 c.trans, state.shadow_mappings.size());
	  if (!state.shadow_mappings.empty()) {
	    return btree.upper_bound_right(
	      c, state.shadow_mappings.front().laddr.without_shadow()
	    ).si_then([&state](LBABtree::iterator iter) {
	      state.iter.emplace(iter);
	    });
	  } else {
	    return demote_region_iertr::make_ready_future();
	  }
	});
      }).si_then([c, &btree, &state, FNAME] {
	return trans_intr::repeat([c, &btree, &state, FNAME] {
	  if (state.shadow_mappings.empty()) {
	    return demote_region_iertr::make_ready_future<
	      seastar::stop_iteration>(seastar::stop_iteration::yes);
	  } else if (state.iter->get_key().with_shadow() != state.shadow_mappings.front().laddr) {
	    TRACET("skip {}", c.trans, state.iter->get_key());
	    auto cur_shadow_laddr = state.shadow_mappings.front().laddr;
	    ceph_assert(state.iter->get_key().with_shadow() < cur_shadow_laddr);
	    if (state.counter < LEAF_NODE_CAPACITY * 2) {
	      return state.iter->next(c
	      ).si_then([&state](LBABtree::iterator iter) {
		state.counter++;
		state.iter.emplace(iter);
		return demote_region_iertr::make_ready_future<
		  seastar::stop_iteration>(seastar::stop_iteration::no);
	      });
	    } else {
	      DEBUGT("iterate over {} mappings, search it directly",
		     c.trans, state.counter);
	      auto laddr = cur_shadow_laddr.without_shadow();
	      return btree.lower_bound(c, laddr
	      ).si_then([&state, laddr](LBABtree::iterator iter) {
		ceph_assert(!iter.is_end());
		ceph_assert(laddr == iter.get_key());
		state.iter.emplace(iter);
		return demote_region_iertr::make_ready_future<
		  seastar::stop_iteration>(seastar::stop_iteration::no);
	      });
	    }
	  } else {
	    auto &mapping = state.shadow_mappings.front();
	    auto val = state.iter->get_val();
	    return state.retire_promotion(
	      val.pladdr.get_paddr(),
	      val.len
	    ).si_then([c, &btree, &state, &mapping, val]() mutable {
	      val.pladdr.has_shadow = false;
	      val.pladdr.pladdr = mapping.paddr;
	      return btree.update(
	        c,
		*state.iter,
		val,
		mapping.nextent
	      ).si_then([&state, c](LBABtree::iterator iter) {
		return iter.next(c
		).si_then([&state](LBABtree::iterator iter) {
		  state.counter = 0;
		  state.shadow_mappings.pop_front();
		  state.iter.emplace(iter);
		  return demote_region_iertr::make_ready_future<
		    seastar::stop_iteration>(seastar::stop_iteration::no);
		});
	      });
	    });
	  }
	});
      });
    }).si_then([](auto &&state) {
      return std::move(state.res);
    });
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

BtreeLBAManager::_decref_intermediate_ret
BtreeLBAManager::_decref_intermediate(
  Transaction &t,
  laddr_t addr,
  extent_len_t len)
{
  LOG_PREFIX(BtreeLBAManager::_decref_intermediate);
  TRACET("dec_ref: {}~{}", t, addr, len);
  auto c = get_context(t);
  return with_btree<LBABtree>(
    cache,
    c,
    [c, addr, len, &t, this, FNAME](auto &btree) mutable {
    return btree.upper_bound_right(
      c, addr
    ).si_then([&btree, addr, len, c, &t, this, FNAME](auto iter) {
      return seastar::do_with(
	std::move(iter),
	[&btree, addr, len, c, &t, this, FNAME](auto &iter) {
	ceph_assert(!iter.is_end());
	ceph_assert(iter.get_key() <= addr);
	lba_map_val_t val = iter.get_val();
	ceph_assert(iter.get_key() + val.len >= addr + len);
	ceph_assert(val.pladdr.is_paddr());
	ceph_assert(val.refcount >= 1);
	val.refcount -= 1;

	TRACET("decreased refcount of intermediate key {} -- {}",
	  c.trans,
	  iter.get_key(),
	  val);

	laddr_t laddr = iter.get_key();
	bool has_shadow = val.pladdr.has_shadow_mapping();
	if (!val.refcount) {
	  return btree.remove(c, iter
	  ).si_then([val, laddr, has_shadow, &t, this, FNAME](auto) {
	    if (has_shadow) {
	      auto shadow_key = laddr.with_shadow();
	      TRACET("dec ref corresponding shadow mapping: {}", t, shadow_key);
	      return update_refcount(t, shadow_key, -1, false
	      ).si_then([val](auto res) {
		ref_update_result_t &ref_res = res.ref_update_res;
		assert(ref_res.refcount == 0);
		assert(ref_res.shadow_paddr == P_ADDR_NULL);
		auto r = ref_update_result_t{
		  val.refcount,
		  val.pladdr,
		  ref_res.addr.get_paddr(),
		  val.len
		};
		return ref_iertr::make_ready_future<
		  std::optional<ref_update_result_t>>(
	            std::make_optional<ref_update_result_t>(r));
	      });
	    } else {
	      auto res = ref_update_result_t{
		val.refcount,
		val.pladdr,
		P_ADDR_NULL,
		val.len
	      };
	      return ref_iertr::make_ready_future<
		std::optional<ref_update_result_t>>(
	          std::make_optional<ref_update_result_t>(res));
	    }
	  });
	} else {
	  return btree.update(c, iter, val, nullptr
	  ).si_then([](auto) {
	    return ref_iertr::make_ready_future<
	      std::optional<ref_update_result_t>>(std::nullopt);
	  });
	}
      });
    });
  });
}

struct update_refcount_res_t {
  std::optional<BtreeLBAManager::ref_update_result_t> intermediate;
  std::optional<BtreeLBAManager::ref_update_result_t> direct_shadow;
};

BtreeLBAManager::update_refcount_ret
BtreeLBAManager::update_refcount(
  Transaction &t,
  laddr_t addr,
  int delta,
  bool cascade_remove)
{
  LOG_PREFIX(BtreeLBAManager::update_refcount);
  TRACET("laddr={}, delta={}", t, addr, delta);
  return _update_mapping(
    t,
    addr,
    [addr, delta](const lba_map_val_t &in) {
      if (addr.is_shadow()) {
	ceph_assert(in.refcount == 1);
	ceph_assert(delta == -1);
      }
      lba_map_val_t out = in;
      ceph_assert((int)out.refcount + delta >= 0);
      out.refcount += delta;
      return out;
    },
    nullptr
  ).si_then([&t, addr, delta, FNAME, this, cascade_remove]
	    (update_mapping_ret_bare_t res) {
    auto &map_value = res.map_value;
    auto &mapping = res.mapping;
    DEBUGT("laddr={}, delta={} done -- {}", t, addr, delta, map_value);
    auto fut = ref_iertr::make_ready_future<
      std::optional<ref_update_result_t>>();
    if (!map_value.refcount && map_value.pladdr.is_laddr() && cascade_remove) {
      fut = _decref_intermediate(
	t,
	map_value.pladdr.get_non_snap_laddr(addr),
	map_value.len
      );
    }
    return fut.si_then([map_value, this, &t, addr, FNAME]
		       (std::optional<ref_update_result_t> intermediate_res) {
      if (map_value.refcount == 0 && map_value.pladdr.has_shadow_mapping()) {
	assert(!addr.is_shadow());
	DEBUGT("removed primary mapping {}, remove its shadow mapping...",
	       t, addr);
	return update_refcount(t, addr.with_shadow(), -1, false
	).si_then([intermediate_res=std::move(intermediate_res)](update_refcount_ret_bare_t res) {
	  return update_refcount_iertr::make_ready_future<
	    update_refcount_res_t>(update_refcount_res_t{
		intermediate_res,
		std::make_optional<ref_update_result_t>(res.ref_update_res)
	      });
	});
      } else {
	return update_refcount_iertr::make_ready_future<
	  update_refcount_res_t>(update_refcount_res_t{
	      intermediate_res,
	      std::nullopt
	    });
      }
    }).si_then([map_value, mapping=std::move(mapping)]
	       (update_refcount_res_t res) mutable {
      if (map_value.pladdr.is_laddr()
	  && res.intermediate) {
	return update_refcount_ret_bare_t{
	  *res.intermediate,
	  std::move(mapping)
	};
      } else {
	auto shadow_paddr = P_ADDR_NULL;
	if (res.direct_shadow) {
	  shadow_paddr = res.direct_shadow->addr.get_paddr();
	}
	return update_refcount_ret_bare_t{
	  ref_update_result_t{
	    map_value.refcount,
	    map_value.pladdr,
	    shadow_paddr,
	    map_value.len
	  },
	  std::move(mapping)
	};
      }
    });
  });
}

BtreeLBAManager::_update_mapping_ret
BtreeLBAManager::_update_mapping(
  Transaction &t,
  laddr_t addr,
  update_func_t &&f,
  LogicalCachedExtent* nextent)
{
  auto c = get_context(t);
  return with_btree_ret<LBABtree, update_mapping_ret_bare_t>(
    cache,
    c,
    [f=std::move(f), c, addr, nextent](auto &btree) mutable {
      return btree.lower_bound(
	c, addr
      ).si_then([&btree, f=std::move(f), c, addr, nextent](auto iter)
		-> _update_mapping_ret {
	if (iter.is_end() || iter.get_key() != addr) {
	  LOG_PREFIX(BtreeLBAManager::_update_mapping);
	  ERRORT("laddr={} doesn't exist", c.trans, addr);
	  return crimson::ct_error::enoent::make();
	}

	auto ret = f(iter.get_val());
	if (ret.refcount == 0) {
	  return btree.remove(
	    c,
	    iter
	  ).si_then([ret](auto) {
	    return update_mapping_ret_bare_t{
	      std::move(ret),
	      BtreeLBAMappingRef(nullptr)
	    };
	  });
	} else {
	  return btree.update(
	    c,
	    iter,
	    ret,
	    nextent
	  ).si_then([c, ret](auto iter) {
	    return update_mapping_ret_bare_t{
	      std::move(ret),
	      iter.get_pin(c)
	    };
	  });
	}
      });
    });
}

BtreeLBAManager::search_insert_pos_ret
BtreeLBAManager::search_insert_pos(
  Transaction &t,
  LBABtree &btree,
  laddr_t laddr,
  extent_len_t length,
  bool determinsitic)
{
  auto c = get_context(t);
  if (determinsitic) {
    return btree.lower_bound(c, laddr
    ).si_then([c, laddr, length](LBABtree::iterator pos) {
      LOG_PREFIX(BtreeLBAManager::search_insert_pos);
      if (!pos.is_end()) {
	ceph_assert(pos.get_key() != laddr);
	ceph_assert(laddr + length <= pos.get_key());
	DEBUGT("insert {}~{} at {} -- {}", c.trans, laddr, length, pos.get_key(), pos.get_val());
      } else {
	DEBUGT("insert {}~{} at begin", c.trans, laddr, length);
      }

#ifndef NDEBUG
      if (!pos.is_begin()) {
	return pos.prev(c).si_then([laddr, pos](LBABtree::iterator prev) {
	  auto prev_laddr = prev.get_key();
	  auto prev_length = prev.get_val().len;
	  ceph_assert(prev_laddr + prev_length <= laddr);
	  return alloc_extent_iertr::make_ready_future<
	    insert_pos_t>(std::move(pos), laddr);
	});
      } else
#endif
	return alloc_extent_iertr::make_ready_future<
	  insert_pos_t>(std::move(pos), laddr);
    });
  } else {
    struct state_t {
      state_t(laddr_t laddr) : last_end(laddr), iter(std::nullopt) {}
      laddr_t last_end;
      std::optional<LBABtree::iterator> iter;
    };
    auto lookup_attempts = stats.num_alloc_extents;
    return seastar::do_with(
      state_t(laddr),
      [this, c, &btree, laddr, length, lookup_attempts](state_t &state) {
	return trans_intr::repeat([this, c, &btree, &state,
				   laddr, length, lookup_attempts] {
	  return btree.upper_bound_right(c, state.last_end
	  ).si_then([this, &state, c, laddr, length, lookup_attempts](LBABtree::iterator pos) {
	    LOG_PREFIX(BtreeLBAManager::search_insert_pos);
	    ++stats.num_alloc_extents_iter_nexts;
	    if (pos.is_end()) {
	      DEBUGT("{}~{} state end, done with {} attempts, insert at {}",
		     c.trans, laddr, length,
		     stats.num_alloc_extents_iter_nexts - lookup_attempts,
		     state.last_end);
	      state.iter = pos;
	      return typename LBABtree::iterate_repeat_ret_inner(
	        interruptible::ready_future_marker{},
	        seastar::stop_iteration::yes);
	    } else if (pos.get_key() >= (state.last_end + length) &&
		       (!laddr.has_valid_prefix() ||
			pos.get_key().get_object_prefix() !=
			state.last_end.get_object_prefix())) {
	      DEBUGT("{}~{} state {}~{}, done with {} attempts, insert at {}",
		     c.trans, laddr, length, pos.get_key(), pos.get_val().len,
		     stats.num_alloc_extents_iter_nexts - lookup_attempts,
		     state.last_end);
	      state.iter = pos;
	      return typename LBABtree::iterate_repeat_ret_inner(
	        interruptible::ready_future_marker{},
	        seastar::stop_iteration::yes);
	    } else {
	      state.last_end = laddr_t::get_next_hint(state.last_end);
	      DEBUGT("{}~{} state {}~{}, retry with {}",
		     c.trans, laddr, length, pos.get_key(),
		     pos.get_val().len, state.last_end);
	      return typename LBABtree::iterate_repeat_ret_inner(
	        interruptible::ready_future_marker{},
	        seastar::stop_iteration::no);
	    }
	  });
	}).si_then([&state] {
	  return alloc_extent_iertr::make_ready_future<
	    insert_pos_t>(*state.iter, state.last_end);
	});
      });
  }
}

}
