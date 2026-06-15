// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <sys/mman.h>
#include <string.h>

#include <seastar/core/metrics.hh>

#include "include/buffer.h"
#include "crimson/os/seastore/lba/btree_lba_manager.h"
#include "crimson/os/seastore/lba/lba_btree_node.h"
#include "crimson/os/seastore/logging.h"

SET_SUBSYS(seastore_lba);
/*
 * levels:
 * - INFO:  mkfs
 * - DEBUG: modification operations
 * - TRACE: read operations, DEBUG details
 */

template <> struct fmt::formatter<
  crimson::os::seastore::lba::LBABtree::iterator>
    : public fmt::formatter<std::string_view>
{
  using Iter = crimson::os::seastore::lba::LBABtree::iterator;

  template <typename FmtCtx>
  auto format(const Iter &iter, FmtCtx &ctx) const
      -> decltype(ctx.out()) {
    if (iter.is_end()) {
      return fmt::format_to(ctx.out(), "end");
    }
    return fmt::format_to(ctx.out(), "{}~{}", iter.get_key(), iter.get_val());
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
  crimson::os::seastore::lba::LBABtree>(
  Transaction &t);

template <typename T>
phy_tree_root_t& get_phy_tree_root(root_t &r)
{
  return r.lba_root;
}

template phy_tree_root_t&
get_phy_tree_root<
  crimson::os::seastore::lba::LBABtree>(root_t &r);

template <>
CachedExtentRef get_phy_tree_root_node_sync<
  crimson::os::seastore::lba::LBABtree>(
  const RootBlockRef &root_block, op_context_t c)
{
  auto lba_root = root_block->lba_root_node;
  if (!lba_root) {
    ceph_assert(root_block->is_pending());
    auto &prior = static_cast<RootBlock&>(*root_block->get_prior_instance());
    lba_root = prior.lba_root_node;
  } else {
    ceph_assert(lba_root->is_initial_pending()
      == root_block->is_pending());
  }
  ceph_assert(lba_root);
  auto ret = c.cache.peek_extent_viewable_by_trans(c.trans, lba_root);
  return ret;
}

template <>
const get_phy_tree_root_node_ret get_phy_tree_root_node<
  crimson::os::seastore::lba::LBABtree>(
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
      return {false,
              Cache::get_extent_iertr::make_ready_future<CachedExtentRef>()};
    }
  } else {
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

template class TreeRootLinker<RootBlock, lba::LBAInternalNode>;
template class TreeRootLinker<RootBlock, lba::LBALeafNode>;

}

namespace crimson::os::seastore::lba {

BtreeLBAManager::mkfs_ret
BtreeLBAManager::mkfs(
  Transaction &t)
{
  LOG_PREFIX(BtreeLBAManager::mkfs);
  INFOT("start", t);
  auto croot = co_await cache.get_root(t);
  assert(croot);
  assert(croot->is_mutation_pending());
  croot->get_root().lba_root = LBABtree::mkfs(croot, get_context(t));
}

BtreeLBAManager::get_cursors_ret
BtreeLBAManager::get_cursors(
  Transaction &t,
  laddr_t laddr,
  extent_len_t length)
{
  LOG_PREFIX(BtreeLBAManager::get_cursors);
  TRACET("{}~0x{:x} ...", t, laddr, length);
  auto c = get_context(t);

  auto btree = co_await get_btree<LBABtree>(cache, c);
  co_return co_await get_cursors(c, btree, laddr, length);
}

BtreeLBAManager::get_cursor_ret
BtreeLBAManager::get_cursor(
  Transaction &t,
  laddr_t laddr,
  bool search_containing)
{
  LOG_PREFIX(BtreeLBAManager::get_cursor);
  TRACET("{} ... search_containing={}", t, laddr, search_containing);
  auto c = get_context(t);
  auto btree = co_await get_btree<LBABtree>(cache, c);

  if (search_containing) {
    auto ret = co_await get_containing_cursor(c, btree, laddr);
    assert(ret->contains(laddr));
    co_return ret;
  } else {
    auto ret = co_await get_cursor(c, btree, laddr);
    assert(laddr == ret->get_laddr());
    co_return ret;
  }
}

BtreeLBAManager::get_cursor_ret
BtreeLBAManager::get_cursor(
  Transaction &t,
  LogicalChildNode &extent)
{
  LOG_PREFIX(BtreeLBAManager::get_cursor);
  TRACET("{}", t, extent);
#ifndef NDEBUG
  if (extent.is_mutation_pending()) {
    auto &prior = static_cast<LogicalChildNode&>(
      *extent.get_prior_instance());
    assert(prior.peek_parent_node()->is_valid());
  } else {
    assert(extent.peek_parent_node()->is_valid());
  }
#endif
  auto c = get_context(t);
  auto btree = co_await get_btree<LBABtree>(cache, c);

  auto leaf = co_await extent.get_parent_node(c.trans, c.cache);

  if (leaf->is_pending()) {
    TRACET("find pending extent {} for {}",
	   c.trans, (void*)leaf.get(), extent);
  }
#ifndef NDEBUG
  auto it = leaf->lower_bound(extent.get_laddr());
  assert(it != leaf->end() && it.get_key() == extent.get_laddr());
#endif
  co_return btree.get_cursor(c, leaf, extent.get_laddr());
}

BtreeLBAManager::get_cursors_ret
BtreeLBAManager::get_cursors(
  op_context_t c,
  LBABtree& btree,
  laddr_t laddr,
  extent_len_t length)
{
  LOG_PREFIX(BtreeLBAManager::get_cursors);
  TRACET("{}~0x{:x} ...", c.trans, laddr, length);

  std::list<LBACursorRef> ret;

  auto pos = co_await btree.upper_bound_right(c, laddr);
  while (true) {
    if (pos.is_end() || pos.get_key() >= (laddr + length)) {
      TRACET("{}~0x{:x} done with {} results, stop at {}",
	     c.trans, laddr, length, ret.size(), pos);
      break;
    }
    TRACET("{}~0x{:x} got {}, repeat ...",
	   c.trans, laddr, length, pos);
    ceph_assert((pos.get_key() + pos.get_val().len) > laddr);
    ret.emplace_back(pos.get_cursor(c));
    pos = co_await pos.next(c);
  }
  co_return ret;
}

BtreeLBAManager::resolve_indirect_cursor_ret
BtreeLBAManager::resolve_indirect_cursor(
  op_context_t c,
  LBABtree& btree,
  const LBACursor &indirect_cursor)
{
  ceph_assert(indirect_cursor.is_indirect());
  return get_cursors(
    c,
    btree,
    indirect_cursor.get_intermediate_key(),
    indirect_cursor.get_length()
  ).si_then([&indirect_cursor](auto cursors) {
    ceph_assert(cursors.size() == 1);
    auto& direct_cursor = cursors.front();
    [[maybe_unused]] auto intermediate_key = indirect_cursor.get_intermediate_key();
    assert(!direct_cursor->is_indirect());
    assert(direct_cursor->get_laddr() <= intermediate_key);
    assert(direct_cursor->get_laddr() + direct_cursor->get_length()
	   >= intermediate_key + indirect_cursor.get_length());
    return std::move(direct_cursor);
  });
}

BtreeLBAManager::lower_bound_ret
BtreeLBAManager::lower_bound(
  Transaction &t,
  laddr_t laddr)
{
  auto c = get_context(t);
  auto btree = co_await get_btree<LBABtree>(cache, c);
  auto iter = co_await btree.lower_bound(c, laddr);
  co_return iter.get_cursor(c);
}

BtreeLBAManager::alloc_extent_ret
BtreeLBAManager::reserve_region(
  Transaction &t,
  LBACursorRef cursor,
  laddr_t addr,
  extent_len_t len,
  extent_types_t type)
{
  LOG_PREFIX(BtreeLBAManager::reserve_region);
  DEBUGT("{} {}~{}", t, *cursor, addr, len);
  assert(cursor->is_viewable());
  auto c = get_context(t);
  auto btree = co_await get_btree<LBABtree>(cache, c);
  auto iter = btree.make_partial_iter(c, *cursor);
  lba_map_val_t val{
    len,
    pladdr_t{P_ADDR_ZERO},
    EXTENT_DEFAULT_REF_COUNT,
    0,
    type};
  auto p = co_await btree.insert(
    c, iter, addr, val,
    get_reserved_ptr<LBALeafNode, laddr_t>()
  );
  ceph_assert(p.second);
  iter = p.first;
  co_return iter.get_cursor(c);
}

BtreeLBAManager::alloc_extents_ret
BtreeLBAManager::alloc_extents(
  Transaction &t,
  LBACursorRef cursor,
  std::vector<LogicalChildNodeRef> extents)
{
  LOG_PREFIX(BtreeLBAManager::alloc_extents);
  DEBUGT("{}", t, *cursor);
  auto c = get_context(t);
  auto btree = co_await get_btree<LBABtree>(cache, c);
  auto iter = btree.make_partial_iter(c, *cursor);
  std::vector<LBACursorRef> ret;
  for (auto eiter = extents.rbegin(); eiter != extents.rend(); ++eiter) {
    auto ext = *eiter;
    assert(ext->has_laddr());
    stats.num_alloc_extents += ext->get_length();
    auto p = co_await btree.insert(
      c,
      iter,
      ext->get_laddr(),
      lba_map_val_t{
	ext->get_length(),
	pladdr_t{ext->get_paddr()},
	EXTENT_DEFAULT_REF_COUNT,
	ext->get_last_committed_crc(),
        ext->get_type()},
      ext.get()
    );
    auto &[it, inserted] = p;
    ceph_assert(inserted);
    TRACET("inserted {}", c.trans, *ext);
    ret.emplace(ret.begin(), it.get_cursor(c));
    iter = it;
#ifndef NDEBUG
    if (eiter != extents.rend()) {
      auto key = iter.get_key();
      auto it = co_await iter.prev(c);
      assert(key >= it.get_key() + it.get_val().len);
    }
#endif
  }
  co_return ret;
}

BtreeLBAManager::clone_mapping_ret
BtreeLBAManager::clone_mapping(
  Transaction &t,
  LBACursorRef pos,
  LBACursorRef mapping,
  laddr_t laddr,
  laddr_t inter_key,
  extent_len_t len,
  bool updateref)
{
  LOG_PREFIX(BtreeLBAManager::clone_mapping);
  assert(pos->is_viewable());
  assert(mapping->is_viewable());
  DEBUGT("pos={}, mapping={}, laddr={}~{}, inter_key={} updateref={}",
	 t, *pos, *mapping, laddr, len, inter_key, updateref);
  assert(inter_key.get_byte_distance<extent_len_t>(mapping->get_laddr()) + len
	 <= mapping->get_length());
  auto c = get_context(t);
  if (updateref) {
    mapping = co_await update_mapping_refcount(c.trans, mapping, 1);
  }
  auto btree = co_await get_btree<LBABtree>(cache, c);
  co_await pos->refresh();
  assert(laddr + len <= pos->get_laddr());
  assert(inter_key.get_clone_prefix() != laddr.get_clone_prefix());
  auto p = co_await btree.insert(
    c,
    btree.make_partial_iter(c, *pos),
    laddr,
    lba_map_val_t{
      len,
      pladdr_t{inter_key.get_local_clone_id()},
      EXTENT_DEFAULT_REF_COUNT,
      0,
      mapping->get_extent_type()},
    get_reserved_ptr<LBALeafNode, laddr_t>());
  auto &[iter, inserted] = p;
  co_await mapping->refresh();
  co_return clone_mapping_ret_t{
    iter.get_cursor(c),
    mapping};
}

BtreeLBAManager::get_cursor_ret
BtreeLBAManager::get_cursor(
  op_context_t c,
  LBABtree& btree,
  laddr_t laddr)
{
  LOG_PREFIX(BtreeLBAManager::get_cursor);
  TRACET("{} ...", c.trans, laddr);
  return btree.lower_bound(
    c, laddr
  ).si_then([FNAME, c, laddr](auto iter) -> get_cursor_ret {
    if (iter.is_end() || iter.get_key() != laddr) {
      ERRORT("{} doesn't exist", c.trans, laddr);
      return crimson::ct_error::enoent::make();
    }
    TRACET("{} got value {}", c.trans, laddr, iter.get_val());
    return get_cursor_ret(
      interruptible::ready_future_marker{},
      iter.get_cursor(c));
  });
}

BtreeLBAManager::search_insert_position_ret
BtreeLBAManager::search_insert_position(
  op_context_t c,
  LBABtree &btree,
  laddr_hint_t hint,
  extent_len_t length)
{
  LOG_PREFIX(BtreeLBAManager::search_insert_position);
  assert(hint != LADDR_HINT_NULL);
  assert(hint.addr != L_ADDR_NULL);
  auto orig_hint = hint;
  auto loop_threshold = 32;
  auto next_warn = 1;
  auto lookup_attempts = 1;
  auto check_conflict = [&](LBABtree::iterator &iter) {
    assert(!iter.is_end());
    auto &hint_begin = hint.addr;
    auto hint_end = hint_begin + length;
    auto mapping_begin = iter.get_key();
    auto mapping_end = iter.get_key() + iter.get_val().len;
    return ((hint_begin < mapping_end) && (hint_end > mapping_begin))
	|| hint.conflict_with(mapping_begin);
  };

  TRACET("hint: {}~{}", c.trans, hint, length);
  auto iter = co_await btree.upper_bound_right(c, hint.lower_boundary());

  while (!iter.is_end() && check_conflict(iter)) {
    TRACET("hint {}~{} conflict with {} {}",
	   c.trans, hint, length, iter.get_key(), iter.get_val());

    ceph_assert(hint.condition != laddr_conflict_condition_t::all_at_never);

    lookup_attempts++;
    if (lookup_attempts / loop_threshold == next_warn) {
      next_warn++;
      WARNT("attempt searching LBABtree voer {} times -- "
	    "orig_hint: {}, cur_hint: {}, cur_iter: {}",
	    c.trans, lookup_attempts, orig_hint,
	    hint, iter);
    }

    if (hint.policy == laddr_conflict_policy_t::gen_random) {
      hint.find_next_random();
      TRACET("re-search with random hint {}", c.trans, hint);
      iter = co_await btree.upper_bound_right(c, hint.lower_boundary());
      continue;
    }

    // linear search
    ceph_assert(
      hint.condition == laddr_conflict_condition_t::all_at_block_offset ||
      hint.condition == laddr_conflict_condition_t::all_at_object_content);

    hint.addr = (iter.get_key() + iter.get_val().len).checked_to_laddr();
    bool loop_back = false;

    if (orig_hint.addr.is_global_address()) {
      if (orig_hint.addr.get_object_prefix() !=
	  hint.addr.get_object_prefix()) {
	hint.addr = L_ADDR_MIN;
	loop_back = true;
      }
    } else if (orig_hint.addr.is_onode_extent_address()) {
      if (hint.addr.get_local_object_id() != LOCAL_OBJECT_ID_ZERO) {
	hint.addr.set_local_object_id(LOCAL_OBJECT_ID_ZERO);
	hint.addr.set_object_content(0);
	loop_back = true;
      }
    } else {
      assert(orig_hint.addr.is_metadata());
      if (orig_hint.addr.get_clone_prefix() !=
	  hint.addr.get_clone_prefix()) {
	hint.addr = orig_hint.addr.with_offset_by_blocks(0);
	loop_back = true;
      }
    }

    TRACET("move to next hint: {} loop_back: {}", c.trans, hint, loop_back);

    if (loop_back) {
      iter = co_await btree.lower_bound(c, hint.addr);
    } else {
      iter = co_await iter.next(c);
    }
  }

  DEBUGT("hint: {}~{}, allocated laddr: {}, insert position: {}"
	 "done with {} attempts",
	 c.trans, orig_hint, length,
	 hint.addr, iter, lookup_attempts);
  stats.num_alloc_extents_iter_nexts += lookup_attempts;
  co_return insert_position_t{hint.addr, iter};
}

BtreeLBAManager::alloc_mappings_ret
BtreeLBAManager::alloc_contiguous_mappings(
  Transaction &t,
  laddr_hint_t hint,
  std::vector<alloc_mapping_info_t> &alloc_infos)
{
  ceph_assert(hint.addr != L_ADDR_NULL);
  extent_len_t total_len = 0;
  for (auto &info : alloc_infos) {
    assert(info.key == L_ADDR_NULL);
    total_len += info.value.len;
  }

  auto c = get_context(t);
  return with_btree<LBABtree>(
    cache,
    c,
    [this, c, hint, &alloc_infos, total_len](auto &btree)
  {
    return search_insert_position(c, btree, hint, total_len
    ).si_then([this, c, &alloc_infos, &btree](insert_position_t res) {
      extent_len_t offset = 0;
      for (auto &info : alloc_infos) {
	info.key = (res.laddr + offset).checked_to_laddr();
	offset += info.value.len;
      }
      return insert_mappings(
	c, btree, std::move(res.insert_iter), alloc_infos);
    });
  });
}

BtreeLBAManager::alloc_mappings_ret
BtreeLBAManager::alloc_sparse_mappings(
  Transaction &t,
  laddr_hint_t hint,
  std::vector<alloc_mapping_info_t> &alloc_infos)
{
  ceph_assert(hint.addr != L_ADDR_NULL);
#ifndef NDEBUG
  assert(alloc_infos.front().key != L_ADDR_NULL);
  for (size_t i = 1; i < alloc_infos.size(); i++) {
    auto &prev = alloc_infos[i - 1];
    auto &cur = alloc_infos[i];
    assert(cur.key != L_ADDR_NULL);
    assert(prev.key + prev.value.len <= cur.key);
  }
#endif
  auto total_len = hint.addr.get_byte_distance<extent_len_t>(
    alloc_infos.back().key + alloc_infos.back().value.len);
  auto c = get_context(t);
  return with_btree<LBABtree>(
    cache,
    c,
    [this, c, hint, &alloc_infos, total_len](auto &btree)
  {
    return search_insert_position(c, btree, hint, total_len
    ).si_then([this, c, hint, &alloc_infos, &btree](auto res) {
      if (hint.condition != laddr_conflict_condition_t::all_at_never) {
	for (auto &info : alloc_infos) {
	  auto offset = info.key.get_byte_distance<extent_len_t>(hint.addr);
	  info.key = (res.laddr + offset).checked_to_laddr();
	}
      } // deterministic guarantees hint == res.laddr
      return insert_mappings(
	c, btree, std::move(res.insert_iter), alloc_infos);
    });
  });
}

BtreeLBAManager::alloc_mappings_ret
BtreeLBAManager::insert_mappings(
  op_context_t c,
  LBABtree &btree,
  LBABtree::iterator iter,
  std::vector<alloc_mapping_info_t> &alloc_infos)
{
  return seastar::do_with(
    std::move(iter), std::list<LBACursorRef>(),
    [c, &btree, &alloc_infos]
    (LBABtree::iterator &iter, std::list<LBACursorRef> &ret)
  {
    return trans_intr::do_for_each(
      alloc_infos.begin(),
      alloc_infos.end(),
      [c, &btree, &iter](auto &info)
    {
      assert(info.key != L_ADDR_NULL);
      bool need_reserved_ptr =
        info.is_indirect_mapping() || info.is_zero_mapping();
      return btree.insert(
	c, iter, info.key, info.value,
        need_reserved_ptr
          ? get_reserved_ptr<LBALeafNode, laddr_t>()
          : static_cast<BaseChildNode<LBALeafNode, laddr_t>*>(info.extent)
      ).si_then([c, &iter, &info](auto p) {
	ceph_assert(p.second);
	iter = std::move(p.first);
	if (is_valid_child_ptr(info.extent)) {
	  ceph_assert(info.value.pladdr.is_paddr());
	  assert(info.value.pladdr == iter.get_val().pladdr);
	  assert(info.value.len == iter.get_val().len);
	  assert(info.extent->is_logical());
	  if (info.extent->has_laddr()) {
	    // see TM::remap_pin()
	    assert(info.key == info.extent->get_laddr());
	    assert(info.key == iter.get_key());
	  } else {
	    // see TM::alloc_non_data_extent()
	    //     TM::alloc_data_extents()
	    info.extent->set_laddr(iter.get_key());
	  }
	}
	return iter.next(c).si_then([&iter](auto p) {
	  iter = std::move(p);
	});
      });
    }).si_then([&ret, &iter, alloc_infos, c] {
      return trans_intr::do_for_each(
	boost::make_counting_iterator<size_t>(0),
	boost::make_counting_iterator<size_t>(alloc_infos.size()),
	[&ret, &iter, c](auto) {
	return iter.prev(c).si_then([c, &ret, &iter](auto it) {
	  ret.push_front(it.get_cursor(c));
	  iter = std::move(it);
	});
      });
    }).si_then([&ret] {
      return alloc_mappings_iertr::make_ready_future<
	std::list<LBACursorRef>>(std::move(ret));
    });
  });
}

static bool is_lba_node(const CachedExtent &e)
{
  return is_lba_node(e.get_type());
}

base_iertr::template future<>
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
	assert(iter.get_leaf_node()->is_stable());
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
  LBACursorRef cursor,
  extent_len_t prev_len,
  paddr_t prev_addr,
  LogicalChildNode& nextent)
{
  LOG_PREFIX(BtreeLBAManager::update_mapping);
  auto laddr = cursor->get_laddr();
  auto addr = nextent.get_paddr();
  auto len = nextent.get_length();
  auto checksum = nextent.get_last_committed_crc();
  TRACET("laddr={}, paddr {}~0x{:x} => {}~0x{:x}, crc=0x{:x}",
         t, laddr, prev_addr, prev_len, addr, len, checksum);
  assert(laddr == nextent.get_laddr());
  assert(!addr.is_null());
  auto res = co_await _update_mapping(
    t,
    *cursor,
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
    &nextent
  ).handle_error_interruptible(
    update_mapping_iertr::pass_further{},
    /* ENOENT in particular should be impossible */
    crimson::ct_error::assert_all(
      "Invalid error in BtreeLBAManager::update_mapping"
    )
  );
  DEBUGT("laddr={}, paddr {}~0x{:x} => {}~0x{:x}, crc=0x{:x} done -- {}",
	 t, laddr, prev_addr, prev_len, addr, len, checksum, *cursor);
  co_return res->get_refcount();
}

BtreeLBAManager::update_mappings_ret
BtreeLBAManager::update_mappings(
  Transaction& t,
  const std::list<LogicalChildNodeRef>& extents)
{
  LOG_PREFIX(BtreeLBAManager::update_mappings);
  auto c = get_context(t);
  return with_btree<LBABtree>(
    cache,
    c,
    [c, &extents, FNAME, this](auto &btree) {
    return trans_intr::do_for_each(
      extents,
      [this, FNAME, c, &btree](auto &extent) {
      return extent->get_parent_node(c.trans, c.cache
      ).si_then([c, &extent, FNAME, &btree, this](auto leaf) {
	if (leaf->is_pending()) {
	  TRACET("find pending extent {} for {}",
		 c.trans, (void*)leaf.get(), *extent);
	}
	return seastar::do_with(
	  btree.get_cursor(c, leaf, extent->get_laddr()),
	  [this, c, &extent, FNAME](auto &cursor) {
	  assert(!cursor->is_end() &&
	    cursor->get_laddr() == extent->get_laddr());
	  auto prev_addr = extent->get_prior_paddr_and_reset();
	  auto len = extent->get_length();
	  auto addr = extent->get_paddr();
	  auto checksum = extent->get_last_committed_crc();
	  TRACET("cursor={}, paddr {}~0x{:x} => {}, crc=0x{:x}",
		 c.trans, *cursor, prev_addr, len, addr, checksum);
	  assert(!addr.is_null());
	  return this->_update_mapping(
	    c.trans,
	    *cursor,
	    [prev_addr, addr, len, checksum](
	      const lba_map_val_t &in) {
	      lba_map_val_t ret = in;
	      ceph_assert(in.pladdr.is_paddr());
	      ceph_assert(in.pladdr.get_paddr() == prev_addr);
	      ceph_assert(in.len == len);
	      ret.pladdr = addr;
	      ret.checksum = checksum;
	      return ret;
	    },
	    nullptr   // all the extents should have already been
		      // added to the fixed_kv_btree
	  ).si_then([c, prev_addr, len, addr,
		    checksum, FNAME](auto res) {
	      DEBUGT("paddr {}~0x{:x} => {}, crc=0x{:x} done -- {}",
		     c.trans, prev_addr, len,
		     addr, checksum, *res);
	      return update_mapping_iertr::make_ready_future();
	    },
	    update_mapping_iertr::pass_further{},
	    /* ENOENT in particular should be impossible */
	    crimson::ct_error::assert_all(
	      "Invalid error in BtreeLBAManager::update_mappings"
	    )
	  );
	});
      });
    });
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
  return with_btree<LBABtree>(
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

void BtreeLBAManager::register_metrics(store_index_t store_index)
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
        sm::description("total number of lba alloc_extent operations"),
        {sm::label_instance("shard_store_index", std::to_string(store_index))}
      ),
      sm::make_counter(
        "alloc_extents_iter_nexts",
        stats.num_alloc_extents_iter_nexts,
        sm::description("total number of iterator next operations during extent allocation"),
        {sm::label_instance("shard_store_index", std::to_string(store_index))}
      ),
    }
  );
}

BtreeLBAManager::_update_mapping_ret
BtreeLBAManager::_update_mapping(
  Transaction &t,
  LBACursor &cursor,
  update_func_t f,
  LogicalChildNode* nextent)
{
  assert(!is_reserved_ptr(nextent));
  assert(cursor.is_viewable());
  auto c = get_context(t);
  auto btree = co_await get_btree<LBABtree>(cache, c);
  auto iter = btree.make_partial_iter(c, cursor);
  auto ret = f(iter.get_val());
  if (ret.refcount == 0) {
    iter = co_await btree.remove(
      c,
      iter
    );
    co_return iter.get_cursor(c);
  } else {
    iter = btree.update(
      c,
      iter,
      ret,
      // child-ptr may already be correct,
      // see LBAManager::update_mappings()
      nextent && !nextent->has_parent_tracker()
        ? nextent : nullptr
    );
    assert(!nextent ||
	   (nextent->has_parent_tracker()
	    && nextent->peek_parent_node().get() == iter.get_leaf_node().get()));
    LBACursorRef cursor = iter.get_cursor(c);
    assert(!cursor->is_end());
    co_return cursor;
  }
}

BtreeLBAManager::scan_mapped_space_ret
BtreeLBAManager::scan_mapped_space(
  Transaction &t,
  BtreeLBAManager::scan_mapped_space_func_t &&f)
{
  LOG_PREFIX(BtreeLBAManager::scan_mapped_space);
  DEBUGT("scan lba tree", t);
  auto c = get_context(t);
  auto scan_visitor = std::move(f);
  auto btree = co_await crimson::os::seastore::get_btree<LBABtree>(c);
  auto block_size = cache.get_block_size();
  auto pos = co_await btree.lower_bound(c, L_ADDR_MIN);
  while (!pos.is_end()) {
    if (pos.get_val().pladdr.is_laddr() ||
        pos.get_val().pladdr.get_paddr().is_zero()) {
      pos = co_await pos.next(c);
      continue;
    }
    TRACET("tree value {}~{} {}~{} used, type {}",
           c.trans,
           pos.get_key(),
           pos.get_val().len,
           pos.get_val().pladdr.get_paddr(),
           pos.get_val().len,
           pos.get_val().type);
    ceph_assert(pos.get_val().len > 0 &&
                pos.get_val().len % block_size == 0);
    ceph_assert(pos.get_val().pladdr != pladdr_t{LOCAL_CLONE_ID_NULL});
    scan_visitor(
        pos.get_val().pladdr.get_paddr(),
        pos.get_val().len,
        pos.get_val().type,
        pos.get_key());
    pos = co_await pos.next(c);
  }

  LBABtree::mapped_space_visitor_t tree_visitor =
    [&scan_visitor, block_size, FNAME, c](
      paddr_t paddr, laddr_t key, extent_len_t len,
      depth_t depth, extent_types_t type, LBABtree::iterator&) {
    TRACET("tree node {}~{} {}, depth={} used",
           c.trans, paddr, len, type, depth);
    ceph_assert(paddr.is_absolute());
    ceph_assert(len > 0 && len % block_size == 0);
    ceph_assert(depth >= 1);
    return scan_visitor(paddr, len, type, key);
  };

  pos = co_await btree.lower_bound(c, L_ADDR_MIN, &tree_visitor);
  while (!pos.is_end()) {
    pos = co_await pos.next(c, &tree_visitor);
  }
}

BtreeLBAManager::get_cursor_ret
BtreeLBAManager::get_containing_cursor(
  op_context_t c,
  LBABtree &btree,
  laddr_t laddr)
{
  LOG_PREFIX(BtreeLBAManager::get_containing_cursor);
  TRACET("{}", c.trans, laddr);
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
}

#ifdef UNIT_TESTS_BUILT
BtreeLBAManager::get_end_mapping_ret
BtreeLBAManager::get_end_mapping(
  Transaction &t)
{
  LOG_PREFIX(BtreeLBAManager::get_end_mapping);
  DEBUGT("", t);
  auto c = get_context(t);
  auto btree = co_await get_btree<LBABtree>(cache, c);
  auto iter = co_await btree.end(c);
  co_return iter.get_cursor(c);
}
#endif

BtreeLBAManager::remap_ret
BtreeLBAManager::remap_mappings(
  Transaction &t,
  LBACursorRef cursor,
  std::vector<remap_entry_t> remaps)
{
  LOG_PREFIX(BtreeLBAManager::remap_mappings);
  DEBUGT("{}", t, *cursor);
  assert(cursor->is_viewable());
  auto orig_indirect = cursor->is_indirect();
  auto orig_laddr = cursor->get_laddr();
  [[maybe_unused]] auto orig_len = cursor->get_length();
  auto c = get_context(t);
  auto btree = co_await get_btree<LBABtree>(cache, c);
  auto iter = btree.make_partial_iter(c, *cursor);
  auto orig_val = iter.get_val();
  std::vector<LBACursorRef> ret;
  assert(orig_val.refcount == EXTENT_DEFAULT_REF_COUNT);
  assert(orig_indirect ||
	 (orig_val.pladdr.is_paddr() &&
	  orig_val.pladdr.get_paddr().is_absolute()));
  auto type = cursor->get_extent_type();
  auto off = 0;
  auto last_iter = iter;
  for (auto &remap : remaps) {
    assert(remap.offset + remap.len <= orig_len);
    assert((bool)remap.extent == !orig_indirect);
    auto new_key = (orig_laddr + remap.offset).checked_to_laddr();
    auto f = [&last_iter, &remap, &off, &t, FNAME, type] {
      lba_map_val_t val = last_iter.get_val();
      auto cur_off = remap.offset - off;
      if (val.pladdr.is_laddr()) {
        DEBUGT("{} + {:#x}",
          t,
          val.pladdr.get_local_clone_id(),
          remap.offset);
      } else {
        auto paddr = val.pladdr.get_paddr();
        val.pladdr = paddr + cur_off;
      }
      val.len = remap.len;
      val.refcount = EXTENT_DEFAULT_REF_COUNT;
      // Checksum will be updated when the committing the transaction
      val.checksum = CRC_NULL;
      val.type = type;
      return val;
    };
    // committing the transaction
    if (remap.offset == remaps.front().offset) {
      iter = co_await btree.replace(
        c, std::move(iter), new_key,
        std::move(f),
        orig_indirect
          ? get_reserved_ptr<LBALeafNode, laddr_t>()
          : remap.extent);
      ret.push_back(iter.get_cursor(c));
      iter = co_await iter.next(c);
    } else {
      auto p = co_await btree.insert(
        c, iter, new_key, f(),
        orig_indirect
          ? get_reserved_ptr<LBALeafNode, laddr_t>()
          : remap.extent);
      auto &[it, inserted] = p;
      ceph_assert(inserted);
      ret.push_back(it.get_cursor(c));
      last_iter = it;
      iter = co_await it.next(c);
    }
    off = remap.offset;
  }
  co_await trans_intr::parallel_for_each(
    ret,
    [](auto &cursor) {
      return cursor->refresh();
    });
  co_return ret;
}

void BtreeLBAManager::update_paddr_sync(
  Transaction &t,
  laddr_t laddr,
  paddr_t paddr)
{
  LOG_PREFIX(BtreeLBAManager::update_paddr_sync);
  DEBUGT("laddr={}, paddr={}", t, laddr, paddr);
  auto c = get_context(t);
  auto btree = get_btree_sync<LBABtree>(c);
  auto iter = btree.lower_bound_sync(c, laddr);
  assert(iter.get_leaf_node()->is_pending());
  auto child = iter.get_leaf_node()->get_child_sync<LogicalChildNode>(
    c.trans, c.cache, iter.get_leaf_pos(), iter.get_key());
  ceph_assert(child);
  if (child->is_initial_pending()) {
    TRACET("{} is initial_pending, skipping", t, *child);
    return;
  }
  ceph_assert(child->is_exist_clean());
  auto cursor = iter.get_cursor(c);
  assert(cursor->get_laddr() == laddr);
  btree.update(
    c,
    std::move(iter),
    lba_map_val_t{
      cursor->get_length(),
      pladdr_t{std::move(paddr)},
      cursor->get_refcount(),
      cursor->get_checksum(),
      cursor->get_extent_type()},
    nullptr,
    modification_t::TRANS_SYNC);
}

BtreeLBAManager::move_mapping_ret
BtreeLBAManager::_copy_mapping(
  op_context_t c,
  LBABtree &btree,
  LBACursorRef src,
  laddr_t dest_laddr,
  LBACursorRef dest,
  LogicalChildNode *extent)
{
  LOG_PREFIX(BtreeLBAManager::_copy_mapping);
  assert(src && dest);
  assert(dest->is_viewable());
  assert(src->is_viewable());
  assert(!src->is_end());
  assert(src->get_refcount() == EXTENT_DEFAULT_REF_COUNT);
  assert(!src->is_indirect() == (bool)extent);
  DEBUGT("src={} dest={} dest_laddr={}", c.trans, *src, *dest, dest_laddr);
  move_mapping_ret_t ret{std::move(src), std::move(dest)};
  auto &cursor = *ret.dest;
  auto iter = btree.make_partial_iter(c, cursor);
  auto &scursor = *ret.src;
  auto src_iter = btree.make_partial_iter(c, scursor);
  if (!iter.is_end()) {
    assert(iter.get_key() >= dest_laddr + ret.src->get_length());
  }
  // insert the src mapping to dest
  // attach extent to the new mapping if it exists
  pladdr_t addr;
  if (ret.src->is_indirect()) {
    addr = ret.src->get_intermediate_key().get_local_clone_id();
  } else {
    addr = ret.src->get_paddr();
  }
  c.trans.new_lba_key_copied(
    ret.src->get_key(),
    dest_laddr,
    [this, c](laddr_t laddr, paddr_t paddr) {
      update_paddr_sync(c.trans, laddr, paddr);
    });
  auto [niter, inserted] = co_await btree.copy(
      c,
      std::move(iter),
      dest_laddr,
      std::move(src_iter),
      extent ? extent : get_reserved_ptr<LBALeafNode, laddr_t>());
  ceph_assert(inserted);
  ret.dest = niter.get_cursor(c);
  co_await ret.src->refresh();
  co_return ret;
}

BtreeLBAManager::move_mapping_ret
BtreeLBAManager::_move_mapping(
  Transaction &t,
  LBACursorRef src,
  laddr_t dest_laddr,
  LBACursorRef dest,
  LogicalChildNode *extent) {
  LOG_PREFIX(BtreeLBAManager::_move_mapping);
  assert(src && dest);
  assert(dest->is_viewable());
  assert(src->is_viewable());
  assert(!src->is_end());
  assert(src->get_refcount() == EXTENT_DEFAULT_REF_COUNT);
  DEBUGT("src={} dest={}", t, *src, *dest);
  auto c = get_context(t);
  auto btree = co_await get_btree<LBABtree>(cache, c);
  auto ret = co_await _copy_mapping(
    c, btree, std::move(src), dest_laddr, std::move(dest), extent);

  ret.src = co_await update_mapping_refcount(
    c.trans, ret.src, -1
  ).handle_error_interruptible(
    move_mapping_iertr::pass_further{},
    crimson::ct_error::assert_all("unexpected error"));

  co_await ret.dest->refresh();
  auto iter = btree.make_partial_iter(c, *ret.dest);
  iter = co_await iter.next(c);
  ret.dest = iter.get_cursor(c);

  co_return ret;
}

BtreeLBAManager::move_mapping_ret
BtreeLBAManager::move_and_clone_direct_mapping(
  Transaction &t,
  LBACursorRef src,
  laddr_t dest_laddr,
  LBACursorRef dest,
  LogicalChildNode &extent)
{
  LOG_PREFIX(BtreeLBAManager::move_and_clone_direct_mapping);
  assert(src && dest);
  assert(dest->is_viewable());
  assert(src->is_viewable());
  assert(!src->is_indirect());
  assert(!src->is_end());
  assert(src->get_refcount() == EXTENT_DEFAULT_REF_COUNT);
  DEBUGT("src={} dest={}", t, *src, *dest);
  auto c = get_context(t);
  auto btree = co_await get_btree<LBABtree>(cache, c);
  auto ret = co_await _copy_mapping(
    c, btree, std::move(src), dest_laddr, std::move(dest), &extent);

  // turn the src mapping into an indirect one pointing to
  // the previously inserted mapping
  auto cursor = co_await _update_mapping(
    c.trans,
    *ret.src,
    [&ret](const auto &in) {
      lba_map_val_t val = in;
      val.pladdr = ret.dest->get_key().get_local_clone_id();
      val.checksum = 0;
      return val;
    },
    nullptr
  ).handle_error_interruptible(
    move_mapping_iertr::pass_further{},
    crimson::ct_error::assert_all("unexpected error"));

  assert(cursor->is_indirect());
  auto iter = btree.make_partial_iter(c, *cursor);
  DEBUGT("resetting child ptr, leaf: {}, pos: {}",
	 t, *iter.get_leaf_node(), iter.get_leaf_pos());
  iter.get_leaf_node()->reset_child_ptr(iter.get_leaf_pos());
  ret.src = std::move(cursor);
  co_await ret.dest->refresh();
  co_return ret;
}

}
