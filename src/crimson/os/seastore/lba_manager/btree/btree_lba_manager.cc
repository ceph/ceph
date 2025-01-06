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

template <>
struct fmt::formatter<crimson::os::seastore::lba_manager::btree::LBABtree::iterator>
    : public fmt::formatter<std::string_view> {
  using Iter = crimson::os::seastore::lba_manager::btree::LBABtree::iterator;
  template<typename FormatContext>
  auto format(const Iter &iter, FormatContext &ctx) const
      -> decltype(ctx.out()) {
    if (iter.is_leaf_end()) {
      return fmt::format_to(ctx.out(), "end");
    } else {
      return fmt::format_to(ctx.out(), "{} {}", iter.get_key(), iter.get_val());
    }
  }
};

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
            c.cache.get_extent_viewable_by_trans(c.trans, lba_root)};
  } else if (root_block->is_pending()) {
    auto &prior = static_cast<RootBlock&>(*root_block->get_prior_instance());
    lba_root = prior.lba_root_node;
    if (lba_root) {
      return {true,
              c.cache.get_extent_viewable_by_trans(c.trans, lba_root)};
    } else {
      c.cache.account_absent_access(c.trans.get_src());
      return {false,
              Cache::get_extent_iertr::make_ready_future<CachedExtentRef>()};
    }
  } else {
    c.cache.account_absent_access(c.trans.get_src());
    return {false,
            Cache::get_extent_iertr::make_ready_future<CachedExtentRef>()};
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
	    if (pos.is_tree_end(c.trans) || pos.get_key() >= (offset + length)) {
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
	return this->get_mappings(
	  c.trans, pin->get_raw_val().get_laddr(), pin->get_length()
	).si_then([&pin, &ret, c](auto new_pin_list) {
	  LOG_PREFIX(BtreeLBAManager::get_mappings);
	  assert(new_pin_list.size() == 1);
	  auto &new_pin = new_pin_list.front();
	  auto intermediate_key = pin->get_raw_val().get_laddr();
	  assert(!new_pin->is_indirect());
	  assert(new_pin->get_key() <= intermediate_key);
	  assert(new_pin->get_key() + new_pin->get_length() >=
	  intermediate_key + pin->get_length());

	  TRACET("Got mapping {}~{} for indirect mapping {}~{}, "
	    "intermediate_key {}",
	    c.trans,
	    new_pin->get_key(), new_pin->get_length(),
	    pin->get_key(), pin->get_length(),
	    pin->get_raw_val().get_laddr());
	  auto &btree_new_pin = static_cast<BtreeLBAMapping&>(*new_pin);
	  btree_new_pin.make_indirect(
	    pin->get_key(),
	    pin->get_length(),
	    pin->get_raw_val().get_laddr());
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
	if (iter.is_tree_end(c.trans) || iter.get_key() != offset) {
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
	      c.trans, pin->get_raw_val().get_laddr()
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

BtreeLBAManager::search_insert_pos_ret
BtreeLBAManager::search_insert_pos(
  Transaction &t,
  LBABtree &btree,
  laddr_t hint,
  extent_len_t length)
{
  LOG_PREFIX(BtreeLBAManager::search_insert_pos);
  struct state_t {
    laddr_t last_end;
    std::optional<LBABtree::iterator> iter;
  };
  return seastar::do_with(
    state_t{hint, std::nullopt},
    [this, &t, &btree, hint, length, FNAME](state_t &state) {
      auto lookup_attempts = stats.num_alloc_extents_iter_nexts;
      auto c = get_context(t);
      return LBABtree::iterate_repeat(
	c,
	btree.upper_bound_right(c, state.last_end),
	[this, &t, &state, hint, length,
	 lookup_attempts, FNAME](LBABtree::iterator &iter) {
	  ++stats.num_alloc_extents_iter_nexts;
	  if (iter.is_tree_end(t)) {
	    DEBUGT("hint={}, len={} state: end, done with {} attempts, insert at {}",
		   t, hint, length,
		   stats.num_alloc_extents_iter_nexts - lookup_attempts,
		   state.last_end);
	    state.iter = iter;
	    return typename LBABtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::yes);
	  } else if (iter.get_key() >= (state.last_end + length)) {
	    DEBUGT("hint={}, len={} state: {}~{}, done with {} attempts, insert at {} -- {}",
		   t, hint, length,
		   iter.get_key(), iter.get_val().len,
		   stats.num_alloc_extents_iter_nexts - lookup_attempts,
		   state.last_end,
		   iter.get_val());
	    state.iter = iter;
	    return typename LBABtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::yes);
	  } else {
	    state.last_end = (iter.get_key() + iter.get_val().len).checked_to_laddr();
	    TRACET("hint={}, len={}, state: {}~{}, repeat ... -- {}",
		   t, hint, length,
		   iter.get_key(), iter.get_val().len,
		   iter.get_val());
	    return typename LBABtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::no);
	  }
	}).si_then([&state] {
	  assert(state.iter);
	  return search_insert_pos_iertr::make_ready_future<
	    search_insert_pos_result_t>(
	      state.last_end, std::move(*state.iter));
	});
    });
}

BtreeLBAManager::alloc_extents_ret
BtreeLBAManager::_alloc_extents(
  Transaction &t,
  laddr_t hint,
  std::vector<alloc_mapping_info_t> &alloc_infos,
  extent_ref_count_t refcount)
{
  ceph_assert(hint != L_ADDR_NULL);
  extent_len_t total_len = 0;
#ifndef NDEBUG
  bool laddr_null = (alloc_infos.front().key == L_ADDR_NULL);
  laddr_t last_end = hint;
  for (auto &info : alloc_infos) {
    assert((info.key == L_ADDR_NULL) == (laddr_null));
    if (!laddr_null) {
      assert(info.key >= last_end);
      last_end = (info.key + info.len).checked_to_laddr();
    }
  }
#endif
  if (alloc_infos.front().key == L_ADDR_NULL) {
    for (auto &info : alloc_infos) {
      total_len += info.len;
    }
  } else {
    auto end = alloc_infos.back().key + alloc_infos.back().len;
    total_len = end.get_byte_distance<extent_len_t>(hint);
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
    [this, FNAME, &alloc_infos, hint, total_len, c, refcount](auto &rets) {
    return crimson::os::seastore::with_btree_state<LBABtree, state_t>(
      cache,
      c,
      hint,
      [this, c, hint, total_len, addr=alloc_infos.front().val, &rets, refcount,
       &alloc_infos, FNAME](auto &btree, auto &state) {
      DEBUGT("alloc {}~{} hint={}", c.trans, addr, total_len, hint);
      return search_insert_pos(c.trans, btree, hint, total_len
      ).si_then([c, addr, hint, &btree, &state, &alloc_infos,
		 total_len, &rets, refcount, FNAME](auto res) {
	state.last_end = res.laddr;
	state.insert_iter = std::move(res.iter);
	return trans_intr::do_for_each(
	  alloc_infos,
	  [c, addr, hint, &btree, &state, FNAME,
	  total_len, &rets, refcount](auto &alloc_info) {
	  if (alloc_info.key != L_ADDR_NULL) {
	    state.last_end = alloc_info.key;
	  }
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
	      assert(alloc_info.val == iter.get_val().pladdr);
	      assert(alloc_info.len == iter.get_val().len);
	      if (alloc_info.extent->has_laddr()) {
		assert(alloc_info.key == alloc_info.extent->get_laddr());
		assert(alloc_info.key == iter.get_key());
	      } else {
		alloc_info.extent->set_laddr(iter.get_key());
	      }
	      alloc_info.extent->set_laddr(iter.get_key());
	    }
	    ceph_assert(inserted);
	    rets.emplace_back(iter.get_pin(c));
	    return iter.next(c).si_then([&state, &alloc_info](auto it) {
	      state.insert_iter = it;
	      if (alloc_info.key == L_ADDR_NULL) {
		state.last_end = (state.last_end + alloc_info.len).checked_to_laddr();
	      }
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
      if (!iter.is_tree_end(c.trans) &&
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
	[c, f=std::move(f), begin, end](auto &pos) {
	  if (pos.is_tree_end(c.trans) || pos.get_key() >= end) {
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
  LogicalCachedExtent *nextent)
{
  LOG_PREFIX(BtreeLBAManager::update_mapping);
  TRACET("laddr={}, paddr {} => {}", t, laddr, prev_addr, addr);
  return _update_mapping(
    t,
    laddr,
    [prev_addr, addr, prev_len, len, checksum](
      const lba_map_val_t &in) {
      assert(!addr.is_null());
      lba_map_val_t ret = in;
      ceph_assert(in.pladdr.is_paddr());
      ceph_assert(in.pladdr.get_paddr() == prev_addr);
      ceph_assert(in.len == prev_len);
      ret.pladdr = addr;
      ret.len = len;
      ret.checksum = checksum;
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

namespace {
LBAIter make_erased_iter(
  Transaction &t,
  LBABtree::iterator& iter)
{
  auto key = iter.is_tree_end(t) ? L_ADDR_NULL : iter.get_key();
  auto val = iter.is_tree_end(t) ? lba_map_val_t{} : iter.get_val();
  return LBAIter{
    iter.get_leaf_node().get(),
    iter.get_leaf_pos(),
    key,
    val,
    t.get_lba_iter_ver()
  };
}
}

BtreeLBAManager::make_btree_iter_ret
BtreeLBAManager::make_btree_iter(
  op_context_t<laddr_t> c,
  LBABtree &btree,
  CachedExtentRef parent,
  laddr_t laddr,
  std::optional<btree_iter_version_t> ver,
  std::optional<uint16_t> pos)
{
  LOG_PREFIX(BtreeLBAManager::make_btree_iter);
  TRACET("build LBABtree::iterator from extent: {} laddr: {}",
	 c.trans, (void*)parent.get(), laddr);
  if (!parent->is_valid()) {
    // XXX: how to eliminate this lookup?
    TRACET("leaf node {} is invalid, search btree from root...",
	   c.trans, (void*)parent.get());
    return btree.lower_bound(c, laddr
    ).si_then([c, laddr](LBABtree::iterator iter) {
      if (iter.is_tree_end(c.trans)) {
	ceph_assert(laddr == L_ADDR_NULL);
      } else {
	ceph_assert(laddr == iter.get_key());
      }
      return make_iterator_iertr::make_ready_future<
	LBABtree::iterator>(std::move(iter));
    });
  }

  auto leaf = parent->cast<LBALeafNode>();
  auto [viewable, state] = leaf->is_viewable_by_trans(c.trans.get_trans_id());
  if (!viewable) {
    leaf = leaf->find_pending_version(c.trans, laddr)->cast<LBALeafNode>();
    TRACET("found viewable extent {} for {}",
	   c.trans, (void*)leaf.get(), (void*)parent.get());
    return make_iterator_iertr::make_ready_future<
      LBABtree::iterator>(btree.make_partial_iter(c, leaf, laddr));
  } else if (ver && c.trans.get_lba_iter_ver() == *ver) {
    return make_iterator_iertr::make_ready_future<
      LBABtree::iterator>(btree.make_partial_iter(c, leaf, laddr, pos));
  } else {
    return make_iterator_iertr::make_ready_future<
      LBABtree::iterator>(btree.make_partial_iter(c, leaf, laddr));
  }
}

BtreeLBAManager::base_iertr::future<LBAIter>
BtreeLBAManager::begin(Transaction &t)
{
  auto c = get_context(t);
  return with_btree_ret<LBABtree, LBAIter>(
    cache, c,
    [c](LBABtree &btree) {
      return btree.begin(c
      ).si_then([c](LBABtree::iterator iter) {
	return make_erased_iter(c.trans, iter);
      });
    });
}

BtreeLBAManager::base_iertr::future<LBAIter>
BtreeLBAManager::end(Transaction &t)
{
  auto c = get_context(t);
  return with_btree_ret<LBABtree, LBAIter>(
    cache, c,
    [c](LBABtree &btree) {
      return btree.end(c
      ).si_then([c](LBABtree::iterator iter) {
	return make_erased_iter(c.trans, iter);
      });
    });
}

BtreeLBAManager::base_iertr::future<bool>
BtreeLBAManager::is_begin(Transaction &t, const LBAIter &iter)
{
  auto c = get_context(t);
  return with_btree_ret<LBABtree, bool>(
    cache, c,
    [this, &iter, c](LBABtree &btree) {
      return make_btree_iter(
	c,
	btree,
	iter.parent,
	iter.key,
	iter.ver,
	iter.pos
      ).si_then([c](LBABtree::iterator iter) {
	return base_iertr::make_ready_future<
	  bool>(iter.is_tree_begin(c.trans));
      });
    });
}

BtreeLBAManager::base_iertr::future<bool>
BtreeLBAManager::is_end(Transaction &t, const LBAIter &iter)
{
  auto c = get_context(t);
  return with_btree_ret<LBABtree, bool>(
    cache, c,
    [this, &iter, c](LBABtree &btree) {
      return make_btree_iter(
	c,
	btree,
	iter.parent,
	iter.key,
	iter.ver,
	iter.pos
      ).si_then([c](LBABtree::iterator iter) {
	return base_iertr::make_ready_future<
	  bool>(iter.is_tree_end(c.trans));
      });
    });
}

BtreeLBAManager::make_mapping_ret
BtreeLBAManager::make_mapping(
  Transaction &t,
  const LBAIter &iter)
{
  LOG_PREFIX(BtreeLBAManager::make_mapping);
  DEBUGT("make LBAMapping for {}", t, iter);
  auto c = get_context(t);
  return with_btree_ret<LBABtree, LBAMappingRef>(
    cache, c,
    [this, &iter, c, FNAME](LBABtree &btree) {
      return make_btree_iter(
	c,
	btree,
	iter.parent,
	iter.key,
	iter.ver,
	iter.pos
      ).si_then([this, c, FNAME](auto iter) {
	TRACET("LBABtree::iterator: {}", c.trans, iter);
	auto pin = iter.get_pin(c);
	if (pin->get_raw_val().is_paddr()) {
	  return make_iterator_iertr::make_ready_future<
	    LBAMappingRef>(std::move(pin));
	}
	std::list<BtreeLBAMappingRef> list;
	list.emplace_back(std::move(pin));
	return seastar::do_with(
	  std::move(list),
	  [this, c](auto &pins) {
	    return _get_original_mappings(c, pins);
	  }).si_then([](auto pins) {
	    assert(pins.size() == 1);
	    return make_iterator_iertr::make_ready_future<
	      LBAMappingRef>(std::move(pins.front()));
	  });
	});
    });
}

BtreeLBAManager::make_iterator_ret
BtreeLBAManager::make_iterator(
  Transaction &t,
  LogicalCachedExtentRef exent)
{
  LOG_PREFIX(BtreeLBAManager::make_iterator);
  DEBUGT("from extent: {}", t, *exent);
  assert(exent->is_valid());
  auto c = get_context(t);
  return with_btree_ret<LBABtree, LBAIter>(
    cache, c,
    [this, exent, c, FNAME](LBABtree &btree) {
      assert(exent->has_parent_tracker());
      auto parent = exent->get_parent_node();
      assert(parent->is_valid());
      assert(parent->is_viewable_by_trans(c.trans.get_trans_id()).first);
      return make_btree_iter(
	c, btree, parent, exent->get_laddr()
      ).si_then([c, FNAME](auto iter) {
	TRACET("returned LBABtree::iterator: {}",
	       c.trans, iter);
	return make_iterator_iertr::make_ready_future<
	  LBAIter>(make_erased_iter(c.trans, iter));
      });
    })
#ifndef NDEBUG
      .si_then([&t, exent, this](auto iter) {
	return get_mapping(t, exent->get_laddr()
	).si_then([exent, iter=std::move(iter)](auto pin) mutable {
	  assert(exent->get_parent_node().get() == pin->get_parent().get());
	  assert(exent->get_laddr() == pin->get_key());
	  return make_iterator_iertr::make_ready_future<
	    LBAIter>(std::move(iter));
	});
      }).handle_error_interruptible(
	make_iterator_iertr::pass_further{},
	crimson::ct_error::assert_all{
	  "Trying to make a non-existent iter in BtreeLBAManager::make_iterator"
	})
#endif
      ;
}

BtreeLBAManager::make_iterator_ret
BtreeLBAManager::make_direct_iterator(
  Transaction &t,
  const LBAMapping &mapping)
{
  LOG_PREFIX(BtreeLBAManager::make_direct_iterator);
  DEBUGT("from LBAMapping: {}", t, mapping);
  auto c = get_context(t);
  auto laddr = mapping.is_indirect()
      ? mapping.get_intermediate_base()
      : mapping.get_key();
  return with_btree_ret<LBABtree, LBAIter>(
    cache, c,
    [this, &mapping, laddr, c, FNAME](auto &btree) {
      return make_btree_iter(
	c, btree, mapping.get_parent(), laddr,
	mapping.get_iter_ver(), mapping.get_pos()
      ).si_then([c, FNAME](auto iter) {
	TRACET("returned LBABtree::iterator: {}",
	       c.trans, iter);
	return make_iterator_iertr::make_ready_future<
	  LBAIter>(make_erased_iter(c.trans, iter));
      });
    })
#ifndef NDEBUG
      .si_then([&t, laddr, this](auto iter) {
	return get_mapping(t, laddr
	).si_then([laddr, iter=std::move(iter)](auto pin) mutable {
	  assert(pin->get_key() == laddr);
	  return make_iterator_iertr::make_ready_future<
	    LBAIter>(std::move(iter));
	});
      }).handle_error_interruptible(
	make_iterator_iertr::pass_further{},
	crimson::ct_error::assert_all{
	  "Trying to make a non-existent iter in BtreeLBAManager::make_iterator"
	})
#endif
      ;
}

BtreeLBAManager::make_iterator_ret
BtreeLBAManager::refresh_iterator(
  Transaction &t,
  const LBAIter &iter)
{
  LOG_PREFIX(BtreeLBAManager::refresh_iterator);
  DEBUGT("{}", t, iter);

  if (iter.ver == t.get_lba_iter_ver()) {
    return make_iterator_iertr::make_ready_future<LBAIter>(iter);
  }

  auto c = get_context(t);
  return with_btree_ret<LBABtree, LBAIter>(
    cache,
    c,
    [this, &iter, c, FNAME](auto &btree) {
      return make_btree_iter(
	c,
	btree,
	iter.parent,
	iter.key,
	iter.ver,
	iter.pos
      ).si_then([c, FNAME](auto iter) {
	TRACET("returned LBABtree::iterator: {}",
	       c.trans, iter);
	return make_iterator_iertr::make_ready_future<
	  LBAIter>(make_erased_iter(c.trans, iter));
      });
    });
}

BtreeLBAManager::make_iterator_ret
BtreeLBAManager::prev_iterator(
  Transaction &t,
  const LBAIter &iter)
{
  LOG_PREFIX(BtreeLBAManager::next_iterator);
  auto c = get_context(t);
  DEBUGT("{}", t, iter);
  return with_btree_ret<LBABtree, LBAIter>(
    cache,
    c,
    [this, c, &iter, FNAME](LBABtree &btree) {
      return make_btree_iter(
	c,
	btree,
	iter.parent,
	iter.key,
	iter.ver,
	iter.pos
      ).si_then([c, FNAME](auto iter) {
	TRACET("LBABtree::iterator: {}", c.trans, iter);
	return iter.prev(c).si_then([c, FNAME](auto iter) {
	  TRACET("prev iterator: {}", c.trans, iter);
	  return insert_mapping_iertr::make_ready_future<
	    LBAIter>(make_erased_iter(c.trans, iter));
	});
      });
    });
}

BtreeLBAManager::make_iterator_ret
BtreeLBAManager::next_iterator(
  Transaction &t,
  const LBAIter &iter)
{
  LOG_PREFIX(BtreeLBAManager::next_iterator);
  auto c = get_context(t);
  DEBUGT("{}", t, iter);
  return with_btree_ret<LBABtree, LBAIter>(
    cache,
    c,
    [this, c, &iter, FNAME](LBABtree &btree) {
      return make_btree_iter(
	c,
	btree,
	iter.parent,
	iter.key,
	iter.ver,
	iter.pos
      ).si_then([c, FNAME](auto iter) {
	TRACET("LBABtree::iterator: {}", c.trans, iter);
	return iter.next(c).si_then([c, FNAME](auto iter) {
	  TRACET("next iterator: {}", c.trans, iter);
	  return insert_mapping_iertr::make_ready_future<
	    LBAIter>(make_erased_iter(c.trans, iter));
	});
      });
    });
}

BtreeLBAManager::make_iterator_ret
BtreeLBAManager::lower_bound(
  Transaction &t,
  laddr_t laddr)
{
  auto c = get_context(t);
  return with_btree_ret<LBABtree, LBAIter>(
    cache,
    c,
    [c, laddr](LBABtree &btree) {
      return btree.lower_bound(c, laddr
      ).si_then([c](LBABtree::iterator iter) {
	return make_erased_iter(c.trans, iter);
      });
    });
}

BtreeLBAManager::make_iterator_ret
BtreeLBAManager::upper_bound(
  Transaction &t,
  laddr_t laddr)
{
  auto c = get_context(t);
  return with_btree_ret<LBABtree, LBAIter>(
    cache,
    c,
    [c, laddr](LBABtree &btree) {
      return btree.upper_bound(c, laddr
      ).si_then([c](LBABtree::iterator iter) {
	return make_erased_iter(c.trans, iter);
      });
    });
}

BtreeLBAManager::make_iterator_ret
BtreeLBAManager::upper_bound_right(
  Transaction &t,
  laddr_t laddr)
{
  auto c = get_context(t);
  return with_btree_ret<LBABtree, LBAIter>(
    cache,
    c,
    [c, laddr](LBABtree &btree) {
      return btree.upper_bound_right(c, laddr
      ).si_then([c](LBABtree::iterator iter) {
	return make_erased_iter(c.trans, iter);
      });
    });
}

BtreeLBAManager::find_region_ret
BtreeLBAManager::find_region(
  Transaction &t,
  laddr_t hint,
  extent_len_t length)
{
  auto c = get_context(t);
  return with_btree_ret<LBABtree, find_region_result_t>(
    cache,
    c,
    [this, &t, hint, length](LBABtree &btree) {
      return search_insert_pos(t, btree, hint, length
      ).si_then([&t](search_insert_pos_result_t res) {
	return find_region_iertr::make_ready_future<find_region_result_t>(
	  res.laddr, make_erased_iter(t, res.iter));
      });
    });
}

BtreeLBAManager::get_iterator_ret
BtreeLBAManager::get_iterator(
  Transaction &t,
  laddr_t laddr)
{
  LOG_PREFIX(BtreeLBAManager::get_iterator);
  DEBUGT("{}", t, laddr);
  auto c = get_context(t);
  return with_btree_ret<LBABtree, LBAIter>(
    cache,
    c,
    [c, laddr, FNAME](LBABtree &btree) {
      return btree.lower_bound(c, laddr
      ).si_then([c, laddr, FNAME](auto iter) -> get_iterator_ret {
	if (iter.is_tree_end(c.trans) || iter.get_key() != laddr) {
	  TRACET("{} not found", c.trans, laddr);
	  return crimson::ct_error::enoent::make();
	}
	TRACET("found {}", c.trans, iter);
	return get_mapping_iertr::make_ready_future<
	  LBAIter>(make_erased_iter(c.trans, iter));
      });
    });
}


BtreeLBAManager::get_iterators_ret
BtreeLBAManager::get_iterators(
  Transaction &t,
  laddr_t laddr,
  extent_len_t length)
{
  auto c = get_context(t);
  LOG_PREFIX(BtreeLBAManager::get_iterators);
  DEBUGT("{}~{}", t, laddr, length);
  return with_btree_state<LBABtree, std::vector<LBAIter>>(
    cache,
    c,
    [c, laddr, length, FNAME](LBABtree &btree, std::vector<LBAIter> &ret) {
      return LBABtree::iterate_repeat(
	c,
	btree.upper_bound_right(c, laddr),
	[c, laddr, length, &ret, FNAME](auto &iter) {
	  if (iter.is_tree_end(c.trans) || iter.get_key() >= (laddr + length)) {
	    if (ret.empty()) {
	      TRACET("{}~{} not found", c.trans, laddr, length);
	    } else {
	      TRACET("found {} mappings for {}~{}",
		     c.trans, ret.size(), laddr, length);
	    }
	    return get_mappings_iertr::make_ready_future<
	      seastar::stop_iteration>(seastar::stop_iteration::yes);
	  }
	  TRACET("found {}", c.trans, iter);
	  ceph_assert((iter.get_key() + iter.get_val().len) > laddr);
	  ret.push_back(make_erased_iter(c.trans, iter));
	  return get_mappings_iertr::make_ready_future<
	    seastar::stop_iteration>(seastar::stop_iteration::no);
	});
    });
}

BtreeLBAManager::insert_mapping_ret
BtreeLBAManager::insert_mapping(
  Transaction &t,
  const LBAIter &iter,
  laddr_t laddr,
  lba_map_val_t value,
  LogicalCachedExtent *nextent)
{
  LOG_PREFIX(BtreeLBAManager::insert_mapping);
  auto c = get_context(t);
  DEBUGT("insert {} {} before {}", t, laddr, value, iter);
  assert(laddr < iter.key);
  assert(laddr + value.len <= iter.key);
  return with_btree_ret<LBABtree, LBAIter>(
    cache,
    c,
    [this, c, &iter, laddr, value, nextent](auto &btree) {
      return make_btree_iter(
	c,
	btree,
	iter.parent,
	iter.key,
	iter.ver,
	iter.pos
      ).si_then([&btree, c, laddr, value, nextent](auto iter) {
	return btree.insert(c, iter, laddr, value, nextent
        ).si_then([c](auto p) {
	  assert(p.second);
	  return insert_mapping_iertr::make_ready_future<
	    LBAIter>(make_erased_iter(c.trans, p.first));
	});
      });
    });
}

BtreeLBAManager::change_mapping_ret
BtreeLBAManager::change_mapping_value(
  Transaction &t,
  const LBAIter &iter,
  lba_map_val_t value,
  LogicalCachedExtent *nextent)
{
  LOG_PREFIX(BtreeLBAManager::change_mapping_value);
  DEBUGT("{} -> {}", t, iter, value);
  auto c = get_context(t);
  return with_btree_ret<LBABtree, LBAIter>(
    cache,
    c,
    [this, c, &iter, value, nextent](auto &btree) {
      return make_btree_iter(
	c,
	btree,
	iter.parent,
	iter.key,
	iter.ver,
	iter.pos
      ).si_then([c, &btree, value, nextent](auto iter) {
	return btree.update(c, iter, value, nextent
        ).si_then([c](auto iter) {
	  return insert_mapping_iertr::make_ready_future<
	    LBAIter>(make_erased_iter(c.trans, iter));
	});
      });
    });
}

BtreeLBAManager::change_mapping_ret
BtreeLBAManager::change_mapping_key_and_value(
  Transaction &t,
  const LBAIter &iter,
  laddr_t laddr,
  lba_map_val_t value,
  LogicalCachedExtent *nextent)
{
  LOG_PREFIX(BtreeLBAManager::change_mapping_key_and_value);
  DEBUGT("{} -> {} {}", t, iter, laddr, value);
  auto c = get_context(t);
  return with_btree_ret<LBABtree, LBAIter>(
    cache,
    c,
    [this, c, &iter, laddr, value, nextent, FNAME](auto &btree) {
      return make_btree_iter(
	c,
	btree,
	iter.parent,
	iter.key,
	iter.ver,
	iter.pos
      ).si_then([c, &btree, laddr, value, nextent, FNAME](auto iter) {
	TRACET("remove original mapping: {}", c.trans, iter);
	return btree.remove(c, iter
        ).si_then([c, &btree, laddr, value, nextent, FNAME](auto iter) {
	  TRACET("insert {} {} before {}", c.trans, laddr, value, iter);
	  return btree.insert(c, iter, laddr, value, nextent);
	}).si_then([c](auto p) {
	  assert(p.second);
	  return insert_mapping_iertr::make_ready_future<
	    LBAIter>(make_erased_iter(c.trans, p.first));
	});
      });
    });
}

BtreeLBAManager::change_mapping_ret
BtreeLBAManager::change_mapping(
  Transaction &t,
  const LBAIter &iter,
  laddr_t laddr,
  lba_map_val_t value,
  LogicalCachedExtent *nextent)
{
  if (iter.key == laddr) {
    return change_mapping_value(t, iter, value, nextent);
  } else {
    assert(iter.key < laddr);
    assert(iter.key + iter.val.len >= laddr + value.len);
    return change_mapping_key_and_value(t, iter, laddr, value, nextent);
  }
}

BtreeLBAManager::remove_mapping_ret
BtreeLBAManager::remove_mapping(
  Transaction &t,
  const LBAIter &iter)
{
  LOG_PREFIX(BtreeLBAManager::remove_mapping);
  DEBUGT("{}", t, iter);
  auto c = get_context(t);
  return with_btree_ret<LBABtree, LBAIter>(
    cache,
    c,
    [this, c, &iter](LBABtree &btree) {
      return make_btree_iter(
	c,
	btree,
	iter.parent,
	iter.key,
	iter.ver,
	iter.pos
      ).si_then([c, &btree](auto iter) {
	return btree.remove(c, iter
        ).si_then([c](auto iter) {
	  return insert_mapping_iertr::make_ready_future<
	    LBAIter>(make_erased_iter(c.trans, iter));
	});
      });
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
  auto c = get_context(t);
  return with_btree<LBABtree>(
    cache,
    c,
    [c, addr, len](auto &btree) mutable {
    return btree.upper_bound_right(
      c, addr
    ).si_then([&btree, addr, len, c](auto iter) {
      return seastar::do_with(
	std::move(iter),
	[&btree, addr, len, c](auto &iter) {
	ceph_assert(!iter.is_tree_end(c.trans));
	ceph_assert(iter.get_key() <= addr);
	auto val = iter.get_val();
	ceph_assert(iter.get_key() + val.len >= addr + len);
	ceph_assert(val.pladdr.is_paddr());
	ceph_assert(val.refcount >= 1);
	val.refcount -= 1;

	LOG_PREFIX(BtreeLBAManager::_decref_intermediate);
	TRACET("decreased refcount of intermediate key {} -- {}",
	  c.trans,
	  iter.get_key(),
	  val);

	if (!val.refcount) {
	  return btree.remove(c, iter
	  ).si_then([val](auto) {
	    auto res = ref_update_result_t{
	      val.refcount,
	      val.pladdr.get_paddr(),
	      val.len
	    };
	    return ref_iertr::make_ready_future<
	      std::optional<ref_update_result_t>>(
	        std::make_optional<ref_update_result_t>(res));
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
    [delta](const lba_map_val_t &in) {
      lba_map_val_t out = in;
      ceph_assert((int)out.refcount + delta >= 0);
      out.refcount += delta;
      return out;
    },
    nullptr
  ).si_then([&t, addr, delta, FNAME, this, cascade_remove](auto res) {
    auto &map_value = res.map_value;
    auto &mapping = res.mapping;
    DEBUGT("laddr={}, delta={} done -- {}", t, addr, delta, map_value);
    auto fut = ref_iertr::make_ready_future<
      std::optional<ref_update_result_t>>();
    if (!map_value.refcount && map_value.pladdr.is_laddr() && cascade_remove) {
      fut = _decref_intermediate(
	t,
	map_value.pladdr.get_laddr(),
	map_value.len
      );
    }
    return fut.si_then([map_value, mapping=std::move(mapping)]
		       (auto decref_intermediate_res) mutable {
      if (map_value.pladdr.is_laddr()
	  && decref_intermediate_res) {
	return update_refcount_ret_bare_t{
	  *decref_intermediate_res,
	  std::move(mapping)
	};
      } else {
	return update_refcount_ret_bare_t{
	  ref_update_result_t{
	    map_value.refcount,
	    map_value.pladdr,
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
	if (iter.is_tree_end(c.trans) || iter.get_key() != addr) {
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

}
