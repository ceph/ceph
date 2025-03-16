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
    bool all_physical = std::all_of(
      cursors.begin(), cursors.end(), [](const LBACursorRef &cursor) {
	assert(cursor->val);
	return cursor->val->pladdr.is_paddr();
      });

    if (all_physical) {
      lba_mapping_list_t ret;
      for (auto &cursor : cursors) {
	ret.emplace_back(LBAMapping::create_physical(std::move(cursor)));
      }
      return get_mappings_iertr::make_ready_future<
	lba_mapping_list_t>(std::move(ret));
    }

    return seastar::do_with(
      std::move(cursors),
      lba_mapping_list_t(),
      [this, c](auto &cursors, auto &ret) {
	return trans_intr::do_for_each(cursors, [this, c, &ret](auto &cursor) {
	  assert(cursor->val);
	  if (cursor->val->pladdr.is_paddr()) {
	    ret.emplace_back(LBAMapping::create_physical(std::move(cursor)));
	    return get_mappings_iertr::make_ready_future();
	  }
	  auto &indirect = cursor;
	  return get_physical_cursor(c, indirect
	  ).si_then([&ret, &indirect](auto physical) {
	    ret.emplace_back(std::move(physical), std::move(indirect));
	  });
	}).si_then([&ret] {
	  return get_mappings_iertr::make_ready_future<
	    lba_mapping_list_t>(std::move(ret));
	});
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
  auto retptr = std::make_unique<lba_mapping_list_t>();
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
  laddr_t offset,
  bool find_containment)
{
  LOG_PREFIX(BtreeLBAManager::get_mapping);
  TRACET("{}", t, offset);
  auto c = get_context(t);
  auto fut = get_cursor_iertr::make_ready_future<LBACursorRef>();
  if (find_containment) {
    fut = get_containing_cursor(c, offset);
  } else {
    fut = get_cursor(c, offset);
  }
  return fut.si_then([c, this](LBACursorRef cursor) {
    assert(cursor->val);
    if (cursor->val->pladdr.is_paddr()) {
      return get_mapping_iertr::make_ready_future<
	LBAMapping>(LBAMapping::create_physical(std::move(cursor)));
    }
    return get_physical_cursor(c, cursor
    ).si_then([indirect=std::move(cursor)](auto physical) mutable {
      return get_mapping_iertr::make_ready_future<
	LBAMapping>(LBAMapping(std::move(physical), std::move(indirect)));
    });
  });
}

BtreeLBAManager::get_mapping_ret
BtreeLBAManager::get_mapping(
  Transaction &t,
  LogicalChildNode &extent)
{
  LOG_PREFIX(BtreeLBAManager::get_mapping);
  TRACET("{}", t, extent);
  assert(extent.get_parent_node()->is_valid());
  auto c = get_context(t);
  return with_btree<LBABtree>(
    cache,
    c,
    [c, &extent, FNAME](auto &btree) {
    return c.cache.get_parent_node(c.trans, extent
    ).si_then([&btree, c, &extent, FNAME](auto leaf) {
      auto [viewable, state] = leaf->is_viewable_by_trans(c.trans.get_trans_id());
      if (!viewable) {
	leaf = leaf->find_pending_version(c.trans, extent.get_laddr());
	TRACET("find pending extent {} for {}",
	       c.trans, (void*)leaf.get(), extent);
      }
      auto it = leaf->lower_bound(extent.get_laddr());
      assert(it != leaf->end() && it.get_key() == extent.get_laddr());
      auto iter = btree.make_partial_iter(
	c, leaf, extent.get_laddr(), it.get_offset());
      return get_mapping_iertr::make_ready_future<
	LBAMapping>(LBAMapping::create_physical(iter.get_cursor(c)));
    });
  });
}

BtreeLBAManager::alloc_extent_ret
BtreeLBAManager::reserve_region(
  Transaction &t,
  LBAMapping pos,
  laddr_t addr,
  extent_len_t len)
{
  LOG_PREFIX(BtreeLBAManager::reserve_region);
  DEBUGT("{} {}~{}", t, pos, addr, len);
  assert(pos.is_valid());
  auto c = get_context(t);
  return with_btree<LBABtree>(
    cache,
    c,
    [pos=std::move(pos), c, addr, len](auto &btree) mutable {
    auto &cursor = pos.get_effective_cursor();
    auto iter = btree.make_partial_iter(c, cursor);
    lba_map_val_t val{len, P_ADDR_ZERO, EXTENT_DEFAULT_REF_COUNT, 0};
    return btree.insert(c, iter, addr, val
    ).si_then([c](auto p) {
      auto &[iter, inserted] = p;
      ceph_assert(inserted);
      auto &leaf_node = *iter.get_leaf_node();
      leaf_node.insert_child_ptr(
	iter.get_leaf_pos(),
	get_reserved_ptr<LBALeafNode, laddr_t>(),
	leaf_node.get_size() - 1 /*the size before the insert*/);
      return LBAMapping::create_physical(iter.get_cursor(c));
    });
  });
}

BtreeLBAManager::alloc_extents_ret
BtreeLBAManager::alloc_extents(
  Transaction &t,
  LBAMapping mapping,
  std::vector<LogicalChildNodeRef> extents)
{
  LOG_PREFIX(BtreeLBAManager::alloc_extents);
  DEBUGT("{}", t, mapping);
  auto c = get_context(t);
  return with_btree<LBABtree>(
    cache,
    c,
    [c, FNAME, mapping=std::move(mapping), this,
    extents=std::move(extents)](auto &btree) mutable {
    auto &cursor = mapping.get_effective_cursor();
    return refresh_lba_cursor(c, btree, &cursor
    ).si_then(
      [&cursor, &btree, extents=std::move(extents),
      mapping=std::move(mapping), c, FNAME, this] {
      return seastar::do_with(
	std::move(extents),
	btree.make_partial_iter(
	  c,
	  cursor.parent->cast<LBALeafNode>(),
	  cursor.key,
	  cursor.pos),
	std::vector<LBAMapping>(),
	[c, &btree, FNAME, this]
	(auto &extents, auto &iter, auto &ret) mutable {
	return trans_intr::do_for_each(
	  extents.rbegin(),
	  extents.rend(),
	  [&btree, FNAME, &iter, c, &ret, this](auto ext) {
	  assert(ext->has_laddr());
	  stats.num_alloc_extents += ext->get_length();
	  return btree.insert(
	    c,
	    iter,
	    ext->get_laddr(),
	    lba_map_val_t{
	      ext->get_length(),
	      ext->get_paddr(),
	      EXTENT_DEFAULT_REF_COUNT,
	      ext->get_last_committed_crc()}
	  ).si_then([ext, c, FNAME, &iter, &ret](auto p) {
	    auto &[it, inserted] = p;
	    ceph_assert(inserted);
	    auto &leaf_node = *it.get_leaf_node();
	    leaf_node.insert_child_ptr(
	      it.get_leaf_pos(),
	      ext.get(),
	      leaf_node.get_size() - 1 /*the size before the insert*/);
	    TRACET("inserted {}", c.trans, *ext);
	    ret.emplace(ret.begin(), LBAMapping::create_physical(it.get_cursor(c)));
	    iter = it;
	  });
#ifndef NDEBUG
	}).si_then([&iter, c] {
	  if (iter.is_begin()) {
	    return base_iertr::now();
	  }
	  auto key = iter.get_key();
	  return iter.prev(c).si_then([key](auto it) {
	    assert(key >= it.get_key() + it.get_val().len);
	    return base_iertr::now();
	  });
#endif
	}).si_then([&ret] { return std::move(ret); });
      });
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
	      rets.emplace_back(LBAMapping::create_physical(iter.get_cursor(c)));
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
  LBAMapping mapping,
  extent_len_t prev_len,
  paddr_t prev_addr,
  extent_len_t len,
  paddr_t addr,
  uint32_t checksum,
  LogicalChildNode *nextent)
{
  LOG_PREFIX(BtreeLBAManager::update_mapping);
  TRACET("mapping={}, paddr {} => {}",
    t, mapping, prev_addr, addr);
  assert(!mapping.is_indirect());
  return seastar::do_with(
    std::move(mapping),
    [&t, this, prev_len, prev_addr, len, FNAME,
    addr, checksum, nextent](auto &mapping) {
    auto &cursor = mapping.get_effective_cursor();
    return _update_mapping(
      t,
      cursor,
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
    ).si_then([&t, prev_addr, addr, FNAME, &mapping](auto res) {
	auto &result = res.map_value;
	DEBUGT("mapping={}, paddr {} => {} done -- {}",
	       t, mapping, prev_addr, addr, result);
	return update_mapping_iertr::make_ready_future<
	  extent_ref_count_t>(result.refcount);
      },
      update_mapping_iertr::pass_further{},
      /* ENOENT in particular should be impossible */
      crimson::ct_error::assert_all{
	"Invalid error in BtreeLBAManager::update_mapping"
      }
    );
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

BtreeLBAManager::refresh_lba_mapping_ret
BtreeLBAManager::refresh_lba_mapping(Transaction &t, LBAMapping mapping)
{
  auto c = get_context(t);
  return with_btree_state<LBABtree, LBAMapping>(
    cache,
    c,
    std::move(mapping),
    [c, this](LBABtree &btree, LBAMapping &mapping) mutable {
      return refresh_lba_cursor(
	c, btree, mapping.physical_cursor
      ).si_then([c, this, &btree, &mapping] {
	return refresh_lba_cursor(c, btree, mapping.indirect_cursor);
      });
    });
}

BtreeLBAManager::refresh_lba_cursor_ret
BtreeLBAManager::refresh_lba_cursor(
  op_context_t c,
  LBABtree &btree,
  LBACursorRef cursor)
{
  if (!cursor) {
    return refresh_lba_cursor_iertr::now();
  }

  return make_btree_partial_iter(c, btree, *cursor
  ).si_then([cursor](LBABtree::iterator iter) {
    iter.update_cursor(*cursor);
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
	  ).si_then([val](auto) {
	    auto res = ref_update_result_t{
	      val.refcount,
	      val.pladdr.get_paddr(),
	      val.len
	    };
	    return ref_iertr::make_ready_future<
	      std::optional<ref_update_result_t>>(
	        std::make_optional<ref_update_result_t>(std::move(res)));
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
  std::variant<laddr_t, LBACursor*> addr_or_cursor,
  int delta,
  bool cascade_remove)
{
  auto addr = addr_or_cursor.index() == 0
    ? std::get<0>(addr_or_cursor)
    : std::get<1>(addr_or_cursor)->key;
  LOG_PREFIX(BtreeLBAManager::update_refcount);
  TRACET("laddr={}, delta={}", t, addr, delta);
  auto fut = _update_mapping_iertr::make_ready_future<
    update_mapping_ret_bare_t>();
  auto update_func =
    [delta](const lba_map_val_t &in) {
      lba_map_val_t out = in;
      ceph_assert((int)out.refcount + delta >= 0);
      out.refcount += delta;
      return out;
    };
  if (addr_or_cursor.index() == 0) {
    fut = _update_mapping(t, addr, std::move(update_func), nullptr);
  } else {
    auto &cursor = std::get<1>(addr_or_cursor);
    fut = _update_mapping(t, *cursor, std::move(update_func), nullptr);
  }
  return fut.si_then([&t, addr, delta, FNAME, this, cascade_remove](auto res) {
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
	decref_intermediate_res->mapping = std::move(mapping);
	return ref_update_result_t{std::move(*decref_intermediate_res)};
      } else {
	return ref_update_result_t{
	    map_value.refcount,
	    map_value.pladdr,
	    map_value.len,
	    std::move(mapping)};
      }
    });
  });
}

BtreeLBAManager::_update_mapping_ret
BtreeLBAManager::_update_mapping(
  Transaction &t,
  LBACursor &cursor,
  update_func_t &&f,
  LogicalChildNode* nextent)
{
  assert(cursor.is_valid());
  auto c = get_context(t);
  return with_btree_ret<LBABtree, update_mapping_ret_bare_t>(
    cache,
    c,
    [c, f=std::move(f), &cursor, nextent](auto &btree) {
    auto iter = btree.make_partial_iter(
      c, cursor.parent->cast<LBALeafNode>(), cursor.key, cursor.pos);
    auto ret = f(iter.get_val());
    if (ret.refcount == 0) {
      return btree.remove(
	c,
	iter
      ).si_then([ret, c](auto iter) {
	auto cursor = iter.get_cursor(c);
	auto indirect = cursor->val && cursor->val->pladdr.is_laddr();
	return update_mapping_ret_bare_t{
	  std::move(ret),
	  indirect
	    ? LBAMapping::create_indirect(std::move(cursor))
	    : LBAMapping::create_physical(std::move(cursor))
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
	auto create_mapping = &LBAMapping::create_physical;
	assert(cursor->val);
	if (cursor->val->pladdr.is_laddr()) {
	  create_mapping = &LBAMapping::create_indirect;
	}

	return update_mapping_ret_bare_t{
	  std::move(ret),
	  create_mapping(std::move(cursor))
	};
      });
    }
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
	  ).si_then([ret, c](auto iter) {
	    bool indirect = !iter.is_end() && iter.get_val().pladdr.is_laddr();
	    return update_mapping_ret_bare_t{
	      std::move(ret),
	      indirect
		? LBAMapping::create_indirect(iter.get_cursor(c))
		: LBAMapping::create_physical(iter.get_cursor(c))
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
	    auto create_mapping = &LBAMapping::create_physical;
	    assert(cursor->val);
	    if (cursor->val->pladdr.is_laddr()) {
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

BtreeLBAManager::get_cursor_ret
BtreeLBAManager::get_containing_cursor(
  op_context_t c,
  laddr_t laddr)
{
  LOG_PREFIX(BtreeLBAManager::get_cursor);
  TRACET("{}", c.trans, laddr);
  return with_btree_ret<LBABtree, LBACursorRef>(
    c.cache,
    c,
    [c, laddr, FNAME](LBABtree &btree) {
    return btree.upper_bound_right(c, laddr
    ).si_then([c, laddr, FNAME](LBABtree::iterator iter)
	      -> get_cursor_ret {
      if (iter.is_end() ||
	  iter.get_key() > laddr ||
	  iter.get_key() + iter.get_val().len <=laddr) {
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
BtreeLBAManager::get_physical_cursor(
  op_context_t c,
  const LBACursorRef &indirect_cursor)
{
  LOG_PREFIX(BtreeLBAManager::get_cursor);
  TRACET("{}", c.trans, *indirect_cursor);
  assert(indirect_cursor->val);
  assert(indirect_cursor->val->pladdr.is_laddr());
  auto orig_key = indirect_cursor->key;
  auto intermediate_key = indirect_cursor->val->pladdr.get_laddr();
  auto length = indirect_cursor->val->len;
  return get_cursors(c, intermediate_key, length
  ).si_then([c, orig_key, intermediate_key, length, FNAME]
	    (std::vector<LBACursorRef> cursors) {
    ceph_assert(cursors.size() == 1);
    LBACursorRef &physical = cursors.front();
    TRACET("got physical cursor {} for indirect cursor {}~{} {}",
	   c.trans, *physical, orig_key, length, intermediate_key);
    assert(physical->val);
    assert(physical->val->pladdr.is_paddr());
    assert(physical->key <= intermediate_key);
    assert(physical->key + physical->val->len >=
	   intermediate_key + length);
    return get_cursors_iertr::make_ready_future<
      LBACursorRef>(std::move(physical));
  });
}

BtreeLBAManager::next_mapping_ret
BtreeLBAManager::next_mapping(
  Transaction &t,
  LBAMapping mapping)
{
  auto c = get_context(t);
  return with_btree<LBABtree>(
    cache,
    c,
    [c, mapping=std::move(mapping)](auto &btree) mutable {
    auto &cursor = mapping.get_effective_cursor();
    auto iter = btree.make_partial_iter(c, cursor);
    return iter.next(c).si_then([c](auto iter) {
      if (!iter.is_end() && iter.get_val().pladdr.is_laddr()) {
	return LBAMapping::create_indirect(iter.get_cursor(c));
      } else {
	return LBAMapping::create_physical(iter.get_cursor(c));
      }
    });
  });
}

BtreeLBAManager::remap_ret
BtreeLBAManager::remap_mappings(
  Transaction &t,
  LBAMapping mapping,
  std::vector<remap_entry> remaps)
{
  assert(mapping.is_indirect() == mapping.is_complete_indirect());
  auto c = get_context(t);
  return with_btree<LBABtree>(
    cache,
    c,
    [mapping=std::move(mapping), c, this,
    remaps=std::move(remaps)](auto &btree) mutable {
    auto &cursor = mapping.get_effective_cursor();
    return seastar::do_with(
      std::move(remaps),
      std::move(mapping),
      btree.make_partial_iter(c, cursor),
      lba_remap_ret_t{},
      [c, &btree, this, &cursor](auto &remaps, auto &mapping, auto &iter, auto &ret) {
      auto val = iter.get_val();
      assert(val.refcount == EXTENT_DEFAULT_REF_COUNT);
      ret.ruret.addr = val.pladdr;
      ret.ruret.length = val.len;
      ret.ruret.refcount--; // refcount == 0
      assert(mapping.is_indirect() ||
	(ret.ruret.addr.is_paddr() &&
	 !ret.ruret.addr.get_paddr().is_zero()));
      return update_refcount(c.trans, &cursor, -1, false
      ).si_then([&mapping, &btree, &iter, c, &ret, &remaps](auto r) {
	assert(r.refcount == 0);
	auto &cursor = r.mapping.get_effective_cursor();
	iter = btree.make_partial_iter(c, cursor);
	return trans_intr::do_for_each(
	  remaps,
	  [&mapping, &btree, &iter, c, &ret](auto &remap) {
	  assert(remap.offset + remap.len <= mapping.get_length());
	  assert((bool)remap.extent == !mapping.is_indirect());
	  lba_map_val_t val;
	  auto old_key = mapping.get_key();
	  auto new_key = (old_key + remap.offset).checked_to_laddr();
	  val.len = remap.len;
	  if (ret.ruret.addr.is_laddr()) {
	    auto laddr = ret.ruret.addr.get_laddr();
	    val.pladdr = (laddr + remap.offset).checked_to_laddr();
	  } else {
	    auto paddr = ret.ruret.addr.get_paddr();
	    val.pladdr = paddr + remap.offset;
	  }
	  val.refcount = EXTENT_DEFAULT_REF_COUNT;
	  val.checksum = 0; // the checksum should be updated later when
			    // committing the transaction
	  return btree.insert(c, iter, new_key, std::move(val)
	  ).si_then([c, &remap, &mapping, &ret, &iter](auto p) {
	    auto &[it, inserted] = p;
	    ceph_assert(inserted);
	    if (mapping.is_indirect()) {
	      ret.remapped_mappings.push_back(
		LBAMapping::create_indirect(it.get_cursor(c)));
	    } else {
	      auto &leaf_node = *it.get_leaf_node();
	      leaf_node.insert_child_ptr(
		it.get_leaf_pos(),
		remap.extent,
		leaf_node.get_size() - 1 /*the size before the insert*/);
	      ret.remapped_mappings.push_back(
		LBAMapping::create_physical(it.get_cursor(c)));
	    }
	    return it.next(c).si_then([&iter](auto it) {
	      iter = std::move(it);
	    });
	  });
	});
      }).si_then([&mapping, c, this, &btree, &ret] {
	if (mapping.is_indirect()) {
	  auto &cursor = mapping.physical_cursor;
	  return refresh_lba_cursor(c, btree, cursor
	  ).si_then([&ret, &mapping] {
	    for (auto &m : ret.remapped_mappings) {
	      m.physical_cursor = mapping.physical_cursor;
	    }
	  });
	}
	return refresh_lba_cursor_iertr::now();
      }).si_then([this, c, &mapping, &remaps] {
	if (remaps.size() > 1 && mapping.is_indirect()) {
	  auto &cursor = mapping.physical_cursor;
	  assert(cursor->is_valid());
	  return update_refcount(
	    c.trans, cursor.get(), 1, false).discard_result();
	}
	return update_refcount_iertr::now();
      }).si_then([&ret] {
	return seastar::make_ready_future<lba_remap_ret_t>(std::move(ret));
      });
    });
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

BtreeLBAManager::make_btree_partial_iter_ret
BtreeLBAManager::make_btree_partial_iter(
  op_context_t c,
  LBABtree &btree,
  LBACursor &cursor)
{
  LOG_PREFIX(BtreeLBAManager::make_btree_partial_iter);
  TRACET("{}", c.trans, cursor);
  assert(cursor.ctx.trans.get_trans_id() == c.trans.get_trans_id());
  if (!cursor.parent->is_valid()) {
    TRACET("{} parent is invalid", c.trans, cursor);
    return btree.lower_bound(c, cursor.key
    ).si_then([&cursor](LBABtree::iterator iter) {
      if (iter.is_end()) {
	ceph_assert(cursor.key == L_ADDR_NULL);
      } else {
	ceph_assert(cursor.key == iter.get_key());
      }
      return make_btree_partial_iter_iertr::make_ready_future<
	LBABtree::iterator>(std::move(iter));
    });
  }

  assert(cursor.parent->is_stable() ||
    cursor.parent->is_pending_in_trans(c.trans.get_trans_id()));
  auto leaf = cursor.parent->cast<LBALeafNode>();
  auto [viewable, state] = leaf->is_viewable_by_trans(c.trans.get_trans_id());
  if (!viewable) {
    leaf = leaf->find_pending_version(c.trans, cursor.key);
    TRACET("find pending extent {} for {}",
	   c.trans, (void*)leaf.get(), cursor);
    auto it = leaf->lower_bound(cursor.key);
    assert(cursor.is_end()
      ? it == leaf->end()
      : it != leaf->end() && it.get_key() == cursor.key);
    return make_btree_partial_iter_iertr::make_ready_future<
      LBABtree::iterator>(btree.make_partial_iter(
	c, leaf, cursor.key, it.get_offset()));
  }

  // parent is viewable for current transaction

  if (leaf->modified_since(cursor.modifications)) {
    // fix cursor pos
    auto it = leaf->lower_bound(cursor.key);
    cursor.pos = it->get_offset();
  }

  return make_btree_partial_iter_iertr::make_ready_future<
    LBABtree::iterator>(btree.make_partial_iter(
      c, leaf, cursor.key, cursor.pos));
}

}
