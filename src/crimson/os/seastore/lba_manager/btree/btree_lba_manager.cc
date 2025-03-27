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
  const RootBlockRef &root_block, op_context_t c)
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

template <typename RootT>
class TreeRootLinker<RootBlock, RootT> {
public:
  static void link_root(RootBlockRef &root_block, RootT* lba_root) {
    root_block->lba_root_node = lba_root;
    ceph_assert(lba_root != nullptr);
    lba_root->parent_of_root = root_block;
  }
  static void unlink_root(RootBlockRef &root_block) {
    root_block->lba_root_node = nullptr;
  }
};

template class TreeRootLinker<RootBlock, lba_manager::btree::LBAInternalNode>;
template class TreeRootLinker<RootBlock, lba_manager::btree::LBALeafNode>;

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
  auto c = get_context(t);
  return get_cursors(c, offset, length
  ).si_then([this, c](std::vector<LBACursorRef> cursors) {
    bool all_direct = std::all_of(
      cursors.begin(), cursors.end(), [](const LBACursorRef &cursor) {
	return !cursor->is_indirect();
      });

    if (all_direct) {
      lba_mapping_list_t ret;
      for (auto &cursor : cursors) {
	ret.emplace_back(LBAMapping::create_direct(std::move(cursor)));
      }
      return get_mappings_iertr::make_ready_future<
	lba_mapping_list_t>(std::move(ret));
    }

    return seastar::do_with(
      std::move(cursors),
      lba_mapping_list_t(),
      [this, c](auto &cursors, auto &ret) {
	return trans_intr::do_for_each(cursors, [this, c, &ret](auto &cursor) {
	  if (!cursor->is_indirect()) {
	    ret.emplace_back(LBAMapping::create_direct(std::move(cursor)));
	    return get_mappings_iertr::make_ready_future();
	  }
	  auto &indirect = cursor;
	  return get_direct_cursor(c, indirect
	  ).si_then([&ret, &indirect](auto direct) {
	    ret.emplace_back(std::move(direct), std::move(indirect));
	  });
	}).si_then([&ret] {
	  return get_mappings_iertr::make_ready_future<
	    lba_mapping_list_t>(std::move(ret));
	});
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
  auto c = get_context(t);
  return get_cursor(c, offset
  ).si_then([c, this](LBACursorRef cursor) {
    if (!cursor->is_indirect()) {
      return get_mapping_iertr::make_ready_future<
	LBAMapping>(LBAMapping::create_direct(std::move(cursor)));
    }
    return get_direct_cursor(c, cursor
    ).si_then([indirect=std::move(cursor)](auto direct) mutable {
      return get_mapping_iertr::make_ready_future<
	LBAMapping>(LBAMapping(std::move(direct), std::move(indirect)));
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
  auto lookup_attempts = stats.num_alloc_extents_iter_nexts;
  return seastar::do_with(
    std::vector<LBAMapping>(),
    [this, FNAME, &alloc_infos, hint, &t, total_len, c,
    lookup_attempts, refcount](auto &rets) {
    return crimson::os::seastore::with_btree_state<LBABtree, state_t>(
      cache,
      c,
      hint,
      [this, c, hint, total_len, addr=alloc_infos.front().val, &rets, refcount,
      lookup_attempts, &t, &alloc_infos, FNAME](auto &btree, auto &state) {
      return LBABtree::iterate_repeat(
	c,
	btree.upper_bound_right(c, hint),
	[this, &state, total_len, addr, &t, hint,
	lookup_attempts, FNAME](auto &pos) {
	++stats.num_alloc_extents_iter_nexts;
	if (pos.is_end()) {
	  DEBUGT("{}~{}, hint={}, state: end, done with {} attempts, insert at {}",
		 t, addr, total_len, hint,
		 stats.num_alloc_extents_iter_nexts - lookup_attempts,
		 state.last_end);
	  state.insert_iter = pos;
	  return typename LBABtree::iterate_repeat_ret_inner(
	    interruptible::ready_future_marker{},
	    seastar::stop_iteration::yes);
	} else if (pos.get_key() >= (state.last_end + total_len)) {
	  DEBUGT("{}~{}, hint={}, state: {}~{}, done with {} attempts, insert at {} -- {}",
		 t, addr, total_len, hint,
		 pos.get_key(), pos.get_val().len,
		 stats.num_alloc_extents_iter_nexts - lookup_attempts,
		 state.last_end,
		 pos.get_val());
	  state.insert_iter = pos;
	  return typename LBABtree::iterate_repeat_ret_inner(
	    interruptible::ready_future_marker{},
	    seastar::stop_iteration::yes);
	} else {
	  state.last_end = (pos.get_key() + pos.get_val().len).checked_to_laddr();
	  TRACET("{}~{}, hint={}, state: {}~{}, repeat ... -- {}",
		 t, addr, total_len, hint,
		 pos.get_key(), pos.get_val().len,
		 pos.get_val());
	  return typename LBABtree::iterate_repeat_ret_inner(
	    interruptible::ready_future_marker{},
	    seastar::stop_iteration::no);
	}
      }).si_then([c, addr, hint, &btree, &state, &alloc_infos,
		  total_len, &rets, refcount, FNAME] {
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
	      alloc_info.checksum}
	  ).si_then([&state, c, addr, total_len, hint, FNAME,
		    &alloc_info, &rets](auto &&p) {
	    auto [iter, inserted] = std::move(p);
	    auto &leaf_node = *iter.get_leaf_node();
	    leaf_node.insert_child_ptr(
	      iter.get_leaf_pos(),
	      alloc_info.extent,
	      leaf_node.get_size() - 1 /*the size before the insert*/);
	    TRACET("{}~{}, hint={}, inserted at {}",
		   c.trans, addr, total_len, hint, state.last_end);
	    if (is_valid_child_ptr(alloc_info.extent)) {
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
	    if (iter.get_val().pladdr.is_laddr()) {
	      rets.emplace_back(LBAMapping::create_indirect(iter.get_cursor(c)));
	    } else {
	      rets.emplace_back(LBAMapping::create_direct(iter.get_cursor(c)));
	    }
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
	std::vector<LBAMapping>>(std::move(rets));
    });
  });
}

static bool is_lba_node(const CachedExtent &e)
{
  return is_lba_node(e.get_type());
}

BtreeLBAManager::base_iertr::template future<>
_init_cached_extent(
  op_context_t c,
  const CachedExtentRef &e,
  LBABtree &btree,
  bool &ret)
{
  if (e->is_logical()) {
    auto logn = e->cast<LogicalChildNode>();
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
	logn->set_laddr(iter.get_key());
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

#ifdef UNIT_TESTS_BUILT
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
#endif

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
  LogicalChildNode *nextent)
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
	ceph_assert(!iter.is_end());
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
	  ).si_then([val] {
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
	  return btree.update(c, iter, val
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
  LogicalChildNode* nextent)
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
	  ).si_then([ret] {
	    return update_mapping_ret_bare_t{
	      std::move(ret),
	      LBAMapping()
	    };
	  });
	} else {
	  return btree.update(
	    c,
	    iter,
	    ret
	  ).si_then([c, ret, nextent](auto iter) {
	    // child-ptr may already be correct,
	    // see LBAManager::update_mappings()
	    if (nextent && !nextent->has_parent_tracker()) {
	      iter.get_leaf_node()->update_child_ptr(
		iter.get_leaf_pos(), nextent);
	    }
	    assert(!nextent || 
	      (nextent->has_parent_tracker()
		&& nextent->get_parent_node().get() == iter.get_leaf_node().get()));
	    LBACursorRef cursor = iter.get_cursor(c);
	    auto create_mapping = &LBAMapping::create_direct;
	    if (cursor->is_indirect()) {
	      create_mapping = &LBAMapping::create_indirect;
	    }

	    return update_mapping_ret_bare_t{
	      std::move(ret),
	      create_mapping(std::move(cursor))
	    };
	  });
	}
      });
    });
}

BtreeLBAManager::get_cursor_ret
BtreeLBAManager::get_cursor(
  op_context_t c,
  laddr_t laddr)
{
  LOG_PREFIX(BtreeLBAManager::get_cursor);
  TRACET("{}", c.trans, laddr);
  return with_btree_ret<LBABtree, LBACursorRef>(
    c.cache,
    c,
    [c, laddr, FNAME](LBABtree &btree) {
      return btree.lower_bound(c, laddr
      ).si_then([c, laddr, FNAME](LBABtree::iterator iter)
		-> get_cursor_ret {
	if (iter.is_end() || iter.get_key() != laddr) {
	  ERRORT("laddr={} doesn't exist", c.trans, laddr);
	  return crimson::ct_error::enoent::make();
	}
	TRACET("{} got {}, {}",
	       c.trans, laddr, iter.get_key(), iter.get_val());
	return get_cursor_iertr::make_ready_future<
	  LBACursorRef>(iter.get_cursor(c));
      });
    });
}

BtreeLBAManager::get_cursors_iertr::future<LBACursorRef>
BtreeLBAManager::get_direct_cursor(
  op_context_t c,
  const LBACursorRef &indirect_cursor)
{
  LOG_PREFIX(BtreeLBAManager::get_cursor);
  TRACET("{}", c.trans, *indirect_cursor);
  assert(indirect_cursor->is_indirect());
  auto orig_key = indirect_cursor->get_laddr();
  auto intermediate_key = indirect_cursor->get_intermediate_key();
  auto length = indirect_cursor->get_length();
  return get_cursors(c, intermediate_key, length
  ).si_then([c, orig_key, intermediate_key, length, FNAME]
	    (std::vector<LBACursorRef> cursors) {
    ceph_assert(cursors.size() == 1);
    LBACursorRef &direct = cursors.front();
    TRACET("got direct cursor {} for indirect cursor {}~{} {}",
	   c.trans, *direct, orig_key, length, intermediate_key);
    assert(direct->val.pladdr.is_paddr());
    assert(direct->key <= intermediate_key);
    assert(direct->key + direct->val.len >=
	   intermediate_key + length);
    return get_cursors_iertr::make_ready_future<
      LBACursorRef>(std::move(direct));
  });
}

BtreeLBAManager::get_cursors_ret
BtreeLBAManager::get_cursors(
  op_context_t c,
  laddr_t laddr,
  extent_len_t length)
{
  LOG_PREFIX(BtreeLBAManager::get_cursors);
  TRACET("{}~{}", c.trans, laddr, length);
  return with_btree_state<LBABtree, std::vector<LBACursorRef>>(
    c.cache,
    c,
    [c, laddr, length, FNAME](auto &btree, auto &cursors) {
      return LBABtree::iterate_repeat(
	c,
	btree.upper_bound_right(c, laddr),
	[c, laddr, length, &cursors, FNAME](LBABtree::iterator &iter) {
	  if (iter.is_end() || iter.get_key() >= (laddr + length)) {
	    TRACET("{}~{} done with {} results",
		   c.trans, laddr, length, cursors.size());
	    return get_cursors_iertr::make_ready_future<
	      seastar::stop_iteration>(seastar::stop_iteration::yes);
	  }
	  TRACET("{}~{} got {}, {}, repeat ...",
		 c.trans, laddr, length, iter.get_key(), iter.get_val());
	  ceph_assert((iter.get_key() + iter.get_val().len) > laddr);
	  cursors.push_back(iter.get_cursor(c));
	  return get_cursors_iertr::make_ready_future<
	    seastar::stop_iteration>(seastar::stop_iteration::no);
	});
    });
}

}
