// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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
  laddr_t laddr,
  extent_len_t length)
{
  LOG_PREFIX(BtreeLBAManager::get_mappings);
  TRACET("{}~0x{:x} ...", t, laddr, length);
  auto c = get_context(t);
  auto btree = co_await get_btree(t);
  lba_mapping_list_t ret;

  auto cursors = co_await get_cursors(c, btree, laddr, length);
  for (auto &cursor : cursors) {
    assert(!cursor->is_end());
    if (!cursor->is_indirect()) {
      ret.emplace_back(LBAMapping::create_direct(std::move(cursor)));
      TRACET("{}~0x{:x} got {}", t, laddr, length, ret.back());
      continue;
    }

    assert(cursor->val->refcount == EXTENT_DEFAULT_REF_COUNT);
    assert(cursor->val->checksum == 0);
    auto direct = co_await resolve_indirect_cursor(c, btree, *cursor);
    ret.emplace_back(LBAMapping::create_indirect(
	std::move(direct), std::move(cursor)));
    TRACET("{}~0x{:x} got {}", t, laddr, length, ret.back());
  }

  co_return ret;
}

BtreeLBAManager::_get_cursors_ret
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
  while (!pos.is_end() && pos.get_key() < (laddr + length)) {
    TRACET("{}~0x{:x} got {}, repeat ...",
	   c.trans, laddr, length, pos);
    ceph_assert((pos.get_key() + pos.get_val().len) > laddr);
    ret.emplace_back(pos.get_cursor(c));
    pos = co_await pos.next(c);
  }

  TRACET("{}~0x{:x} done with {} results, stop at {}",
	 c.trans, laddr, length, ret.size(), pos);
  co_return ret;
}

BtreeLBAManager::resolve_indirect_cursor_ret
BtreeLBAManager::resolve_indirect_cursor(
  op_context_t c,
  LBABtree& btree,
  const LBACursor &indirect_cursor)
{
  ceph_assert(indirect_cursor.is_indirect());
  auto cursors = co_await get_cursors(
    c,
    btree,
    indirect_cursor.get_intermediate_key(),
    indirect_cursor.get_length());

  ceph_assert(cursors.size() == 1);
  auto& direct_cursor = cursors.front();
  auto intermediate_key = indirect_cursor.get_intermediate_key();
  assert(!direct_cursor->is_indirect());
  assert(direct_cursor->get_laddr() <= intermediate_key);
  assert(direct_cursor->get_laddr() + direct_cursor->get_length()
	 >= intermediate_key + indirect_cursor.get_length());
  co_return direct_cursor;
}

BtreeLBAManager::get_mapping_ret
BtreeLBAManager::get_mapping(
  Transaction &t,
  laddr_t laddr,
  bool search_containing)
{
  LOG_PREFIX(BtreeLBAManager::get_mapping);
  TRACET("{} ... search_containing={}", t, laddr, search_containing);
  auto c = get_context(t);
  return with_btree<LBABtree>(
    cache, c,
    [FNAME, this, c, laddr, search_containing](auto& btree)
  {
    auto fut = get_mapping_iertr::make_ready_future<LBACursorRef>();
    if (search_containing) {
      fut = get_containing_cursor(c, btree, laddr);
    } else {
      fut = get_cursor(c, btree, laddr);
    }
    return fut.si_then([FNAME, laddr, &btree, c, this,
			search_containing](LBACursorRef cursor) {
      assert(!cursor->is_end());
      if (!cursor->is_indirect()) {
        TRACET("{} got direct cursor {}",
               c.trans, laddr, *cursor);
	auto mapping = LBAMapping::create_direct(std::move(cursor));
        return get_mapping_iertr::make_ready_future<
	  LBAMapping>(std::move(mapping));
      }
      if (search_containing) {
	assert(cursor->contains(laddr));
      } else {
	assert(laddr == cursor->get_laddr());
      }
      assert(cursor->val->refcount == EXTENT_DEFAULT_REF_COUNT);
      assert(cursor->val->checksum == 0);
      return resolve_indirect_cursor(c, btree, *cursor
      ).si_then([FNAME, c, laddr, indirect=std::move(cursor)]
		(auto direct) mutable {
	auto mapping = LBAMapping::create_indirect(
	  std::move(direct), std::move(indirect));
        TRACET("{} got indirect mapping {}",
               c.trans, laddr, mapping);
        return get_mapping_iertr::make_ready_future<
	  LBAMapping>(std::move(mapping));
      });
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
  auto btree = co_await get_btree(t);
  auto leaf = co_await extent.get_parent_node(c.trans, c.cache);

  if (leaf->is_pending()) {
    TRACET("find pending extent {} for {}", t, (void*)leaf.get(), extent);
  }
#ifndef NDEBUG
  auto it = leaf->lower_bound(extent.get_laddr());
  assert(it != leaf->end() && it.get_key() == extent.get_laddr());
#endif
  co_return LBAMapping::create_direct(
    btree.get_cursor(c, leaf, extent.get_laddr()));
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
  assert(pos.is_viewable());
  auto c = get_context(t);
  auto btree = co_await get_btree(t);

  auto &cursor = pos.get_effective_cursor();
  lba_map_val_t val{len, P_ADDR_ZERO, EXTENT_DEFAULT_REF_COUNT, 0};
  auto [iter, inserted] = co_await btree.insert(
    c, btree.make_partial_iter(c, cursor), addr, val);
  ceph_assert(inserted);
  auto &leaf_node = *iter.get_leaf_node();
  leaf_node.insert_child_ptr(
    iter.get_leaf_pos(),
    get_reserved_ptr<LBALeafNode, laddr_t>(),
    leaf_node.get_size() - 1 /*the size before the insert*/);
  co_return LBAMapping::create_direct(iter.get_cursor(c));
}

BtreeLBAManager::alloc_extent_ret
BtreeLBAManager::reserve_region(
  Transaction &t,
  laddr_t hint,
  extent_len_t len)
{
  std::vector<alloc_mapping_info_t> alloc_infos = {
    alloc_mapping_info_t::create_zero(len)};
  auto cursors = co_await alloc_contiguous_mappings(
    t, hint, alloc_infos, alloc_policy_t::linear_search);
  assert(cursors.size() == 1);
  co_return LBAMapping::create_direct(std::move(cursors.front()));
}

BtreeLBAManager::alloc_extents_ret
BtreeLBAManager::alloc_extents(
  Transaction &t,
  LBAMapping pos,
  std::vector<LogicalChildNodeRef> extents)
{
  LOG_PREFIX(BtreeLBAManager::alloc_extents);
  DEBUGT("{}", t, pos);
  assert(pos.is_viewable());
  auto c = get_context(t);
  return with_btree<LBABtree>(
    cache,
    c,
    [c, FNAME, pos=std::move(pos), this,
    extents=std::move(extents)](auto &btree) mutable {
    auto &cursor = pos.get_effective_cursor();
    return cursor.refresh(
    ).si_then(
      [&cursor, &btree, extents=std::move(extents),
      pos=std::move(pos), c, FNAME, this] {
      return seastar::do_with(
	std::move(extents),
	btree.make_partial_iter(c, cursor),
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
	    ret.emplace(ret.begin(), LBAMapping::create_direct(it.get_cursor(c)));
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

BtreeLBAManager::alloc_extent_ret
BtreeLBAManager::alloc_extent(
  Transaction &t,
  laddr_t hint,
  LogicalChildNode &ext,
  extent_ref_count_t refcount)
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
    t, hint, alloc_infos, alloc_policy_t::linear_search);
  assert(cursors.size() == 1);
  co_return LBAMapping::create_direct(std::move(cursors.front()));
}

BtreeLBAManager::alloc_extents_ret
BtreeLBAManager::alloc_extents(
  Transaction &t,
  laddr_t hint,
  std::vector<LogicalChildNodeRef> extents,
  extent_ref_count_t refcount)
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

  auto cursors = has_laddr
      ? co_await alloc_sparse_mappings(
	t, hint, alloc_infos, alloc_policy_t::deterministic)
      : co_await alloc_contiguous_mappings(
	t, hint, alloc_infos, alloc_policy_t::linear_search);
#ifndef NDEBUG
  if (has_laddr) {
    assert(alloc_infos.size() == cursors.size());
    auto info_p = alloc_infos.begin();
    auto cursor_p = cursors.begin();
    for (; info_p != alloc_infos.end(); info_p++, cursor_p++) {
      auto &cursor = *cursor_p;
      assert(cursor->get_laddr() == info_p->key);
    }
  }
#endif
  std::vector<LBAMapping> ret;
  for (auto &cursor : cursors) {
    ret.emplace_back(LBAMapping::create_direct(std::move(cursor)));
  }
  co_return ret;
}

BtreeLBAManager::ref_ret
BtreeLBAManager::remove_mapping(
  Transaction &t,
  laddr_t addr)
{
  auto res = co_await update_refcount(t, addr, -1);
  ceph_assert(res.refcount == 0);
  if (res.addr.is_paddr()) {
    co_return ref_update_result_t{std::move(res), std::nullopt};
  }
  auto direct_result = co_await update_refcount(t, res.key, -1);
  co_await res.mapping.refresh();
  co_return ref_update_result_t{std::move(res), std::move(direct_result)};
}

BtreeLBAManager::ref_ret
BtreeLBAManager::remove_indirect_mapping_only(
  Transaction &t,
  LBAMapping mapping)
{
  assert(mapping.is_viewable());
  assert(mapping.is_indirect());
  auto res = co_await update_refcount(t, mapping.indirect_cursor.get(), -1);
  co_return ref_update_result_t{std::move(res), std::nullopt};
}

BtreeLBAManager::ref_ret
BtreeLBAManager::remove_mapping(
  Transaction &t,
  LBAMapping mapping)
{
  assert(mapping.is_viewable());
  assert(mapping.is_complete());

  auto res = co_await update_refcount(t, &mapping.get_effective_cursor(), -1);
  ceph_assert(res.refcount == 0);
  if (res.addr.is_paddr()) {
    assert(!mapping.is_indirect());
    co_return ref_update_result_t{std::move(res), std::nullopt};
  }

  assert(mapping.is_indirect());
  auto &cursor = *mapping.direct_cursor;
  co_await cursor.refresh();
  auto direct_result = co_await update_refcount(t, &cursor, -1);
  res.mapping = co_await res.mapping.refresh();
  co_return ref_update_result_t{std::move(res), std::move(direct_result)};
}

BtreeLBAManager::ref_ret
BtreeLBAManager::incref_extent(
  Transaction &t,
  laddr_t addr)
{
  auto res = co_await update_refcount(t, addr, 1);
  co_return ref_update_result_t(std::move(res), std::nullopt);
}

BtreeLBAManager::ref_ret
BtreeLBAManager::incref_extent(
  Transaction &t,
  LBAMapping mapping)
{
  assert(mapping.is_viewable());
  auto &cursor = mapping.get_effective_cursor();
  auto res = co_await update_refcount(t, &cursor, 1);
  co_return ref_update_result_t(std::move(res), std::nullopt);
}

BtreeLBAManager::clone_mapping_ret
BtreeLBAManager::clone_mapping(
  Transaction &t,
  LBAMapping pos,
  LBAMapping mapping,
  laddr_t laddr,
  bool updateref)
{
  LOG_PREFIX(BtreeLBAManager::clone_mapping);
  assert(pos.is_viewable());
  assert(mapping.is_viewable());
  DEBUGT("pos={}, mapping={}, laddr={}, updateref={}",
    t, pos, mapping, laddr, updateref);
  struct state_t {
    LBAMapping pos;
    LBAMapping mapping;
    laddr_t laddr;
    extent_len_t len;
  };
  auto c = get_context(t);
  return seastar::do_with(
    std::move(mapping),
    [this, updateref, c](auto &mapping) {
    if (updateref) {
      auto fut = update_refcount_iertr::make_ready_future<
	LBACursorRef>(mapping.direct_cursor);
      if (!mapping.direct_cursor) {
	fut = resolve_indirect_cursor(c, *mapping.indirect_cursor);
      }
      return fut.si_then([this, c, &mapping](auto cursor) {
	mapping.direct_cursor = cursor;
	assert(mapping.direct_cursor->is_viewable());
	return update_refcount(c.trans, cursor.get(), 1
	).si_then([&mapping](auto res) {
	  assert(!res.mapping.is_indirect());
	  mapping.direct_cursor = std::move(res.mapping.direct_cursor);
	  return std::move(mapping);
	});
      });
    } else {
      return update_refcount_iertr::make_ready_future<
	LBAMapping>(std::move(mapping));
    }
  }).si_then([c, this, pos=std::move(pos),
		      laddr](auto mapping) mutable {
    auto len = mapping.get_length();
    return seastar::do_with(
      state_t{std::move(pos), std::move(mapping), laddr, len},
      [this, c](auto &state) {
      return with_btree<LBABtree>(
	cache,
	c,
	[c, &state](auto &btree) mutable {
	auto &cursor = state.pos.get_effective_cursor();
	return cursor.refresh(
	).si_then([&state, c, &btree]() mutable {
	  auto &cursor = state.pos.get_effective_cursor();
	  assert(state.laddr + state.len <= cursor.key);
	  return btree.insert(
	    c,
	    btree.make_partial_iter(c, cursor),
	    state.laddr,
	    lba_map_val_t{
	      state.len,
	      state.mapping.is_indirect()
		? state.mapping.get_intermediate_key()
		: state.mapping.get_key(),
	      EXTENT_DEFAULT_REF_COUNT,
	      0});
	}).si_then([c, &state](auto p) {
	  auto &[iter, inserted] = p;
	  auto &leaf_node = *iter.get_leaf_node();
	  leaf_node.insert_child_ptr(
	    iter.get_leaf_pos(),
	    get_reserved_ptr<LBALeafNode, laddr_t>(),
	    leaf_node.get_size() - 1 /*the size before the insert*/);
	  auto cursor = iter.get_cursor(c);
	  return state.mapping.refresh(
	  ).si_then([cursor=std::move(cursor)](auto mapping) mutable {
	    return clone_mapping_ret_t{
	      LBAMapping(mapping.direct_cursor, std::move(cursor)),
	      mapping};
	  });
	});
      });
    });
  }).handle_error_interruptible(
    clone_mapping_iertr::pass_further{},
    crimson::ct_error::assert_all{"unexpected error"}
  );
}

BtreeLBAManager::_get_cursor_ret
BtreeLBAManager::get_cursor(
  op_context_t c,
  LBABtree& btree,
  laddr_t laddr)
{
  LOG_PREFIX(BtreeLBAManager::get_cursor);
  TRACET("{} ...", c.trans, laddr);
  auto iter = co_await btree.lower_bound(c, laddr);
  if (iter.is_end() || iter.get_key() != laddr) {
    ERRORT("{} doesn't exist", c.trans, laddr);
    co_await get_mapping_iertr::future<int>(
      crimson::ct_error::enoent::make());
  }
  TRACET("{} got value {}", c.trans, laddr, iter.get_val());
  co_return iter.get_cursor(c);
}

BtreeLBAManager::search_insert_position_ret
BtreeLBAManager::search_insert_position(
  op_context_t c,
  LBABtree &btree,
  laddr_t hint,
  extent_len_t length,
  alloc_policy_t policy)
{
  LOG_PREFIX(BtreeLBAManager::search_insert_position);
  auto lookup_attempts = stats.num_alloc_extents_iter_nexts++;
  auto iter = co_await btree.upper_bound_right(c, hint);
  auto last_end = hint;

  while (!iter.is_end() && iter.get_key() < (last_end + length)) {
    ceph_assert(policy == alloc_policy_t::linear_search);
    last_end = (iter.get_key() + iter.get_val().len).checked_to_laddr();
    TRACET("hint: {}~0x{:x}, current iter: {}, repeat ...",
	   c.trans, hint, length, iter);
    iter = co_await iter.next(c);
    ++stats.num_alloc_extents_iter_nexts;
  }

  if (policy == alloc_policy_t::deterministic) {
    ceph_assert(hint == last_end);
  }
  DEBUGT("hint: {}~0x{:x}, allocated laddr: {}, insert position: {}, "
	 "done with {} attempts",
	 c.trans, hint, length, last_end, iter,
	 stats.num_alloc_extents_iter_nexts - lookup_attempts);
  co_return insert_position_t(last_end, std::move(iter));
}

BtreeLBAManager::alloc_mappings_ret
BtreeLBAManager::alloc_contiguous_mappings(
  Transaction &t,
  laddr_t hint,
  std::vector<alloc_mapping_info_t> &alloc_infos,
  alloc_policy_t policy)
{
  ceph_assert(hint != L_ADDR_NULL);
  extent_len_t total_len = 0;
  for (auto &info : alloc_infos) {
    assert(info.key == L_ADDR_NULL);
    total_len += info.value.len;
  }

  auto c = get_context(t);
  auto btree = co_await get_btree(t);
  auto res = co_await search_insert_position(c, btree, hint, total_len, policy);
  extent_len_t offset = 0;
  for (auto &info : alloc_infos) {
    info.key = (res.laddr + offset).checked_to_laddr();
    offset += info.value.len;
  }
  co_return co_await insert_mappings(
    c, btree, std::move(res.insert_iter), alloc_infos);
}

BtreeLBAManager::alloc_mappings_ret
BtreeLBAManager::alloc_sparse_mappings(
  Transaction &t,
  laddr_t hint,
  std::vector<alloc_mapping_info_t> &alloc_infos,
  alloc_policy_t policy)
{
  ceph_assert(hint != L_ADDR_NULL);
#ifndef NDEBUG
  assert(alloc_infos.front().key != L_ADDR_NULL);
  for (size_t i = 1; i < alloc_infos.size(); i++) {
    auto &prev = alloc_infos[i - 1];
    auto &cur = alloc_infos[i];
    assert(cur.key != L_ADDR_NULL);
    assert(prev.key + prev.value.len <= cur.key);
  }
#endif
  auto total_len = hint.get_byte_distance<extent_len_t>(
    alloc_infos.back().key + alloc_infos.back().value.len);
  auto c = get_context(t);
  auto btree = co_await get_btree(t);
  auto res = co_await search_insert_position(c, btree, hint, total_len, policy);
  if (policy != alloc_policy_t::deterministic) {
    for (auto &info : alloc_infos) {
      auto offset = info.key.get_byte_distance<extent_len_t>(hint);
      info.key = (res.laddr + offset).checked_to_laddr();
    }
  } // deterministic guarantees hint == res.laddr
  co_return co_await insert_mappings(
    c, btree, std::move(res.insert_iter), alloc_infos);
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
      [c, &btree, &iter, &ret](auto &info)
    {
      assert(info.key != L_ADDR_NULL);
      return btree.insert(
	c, iter, info.key, info.value
      ).si_then([c, &iter, &ret, &info](auto p) {
	ceph_assert(p.second);
	iter = std::move(p.first);
	auto &leaf_node = *iter.get_leaf_node();
	bool need_reserved_ptr =
	  info.is_indirect_mapping() || info.is_zero_mapping();
	leaf_node.insert_child_ptr(
	  iter.get_leaf_pos(),
	  need_reserved_ptr
	    ? get_reserved_ptr<LBALeafNode, laddr_t>()
	    : static_cast<BaseChildNode<LBALeafNode, laddr_t>*>(info.extent),
	  leaf_node.get_size() - 1 /*the size before the insert*/);
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
	ret.push_back(iter.get_cursor(c));
	return iter.next(c).si_then([&iter](auto p) {
	  iter = std::move(p);
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

BtreeLBAManager::init_cached_extent_ret
BtreeLBAManager::init_cached_extent(
  Transaction &t,
  CachedExtentRef e)
{
  LOG_PREFIX(BtreeLBAManager::init_cached_extent);
  TRACET("{}", t, *e);
  auto c = get_context(t);
  auto btree = co_await get_btree(t);

  if (e->is_logical()) {
    auto logn = e->cast<LogicalChildNode>();
    auto iter = co_await btree.lower_bound(c, logn->get_laddr());
    if (!iter.is_end() &&
	iter.get_key() == logn->get_laddr() &&
	iter.get_val().pladdr.is_paddr() &&
	iter.get_val().pladdr.get_paddr() == logn->get_paddr()) {
      assert(iter.get_leaf_node()->is_stable());
      iter.get_leaf_node()->link_child(logn.get(), iter.get_leaf_pos());
      logn->set_laddr(iter.get_key());
      ceph_assert(iter.get_val().len == e->get_length());
      DEBUGT("logical extent {} live", c.trans, *logn);
      co_return true;
    } else {
      DEBUGT("logical extent {} not live", c.trans, *logn);
      co_return false;
    }
  } else {
    co_return co_await btree.init_cached_extent(c, e);
  }
}

#ifdef UNIT_TESTS_BUILT
BtreeLBAManager::check_child_trackers_ret
BtreeLBAManager::check_child_trackers(
  Transaction &t) {
  auto c = get_context(t);
  auto btree = co_await get_btree(t);
  co_return co_await btree.check_child_trackers(c);
}
#endif

BtreeLBAManager::scan_mappings_ret
BtreeLBAManager::scan_mappings(
  Transaction &t,
  laddr_t begin,
  laddr_t end,
  scan_mappings_func_t f)
{
  LOG_PREFIX(BtreeLBAManager::scan_mappings);
  DEBUGT("begin: {}, end: {}", t, begin, end);

  auto c = get_context(t);
  auto btree = co_await get_btree(t);
  auto pos = co_await btree.upper_bound_right(c, begin);
  while (!pos.is_end() && pos.get_key() < end) {
    ceph_assert((pos.get_key() + pos.get_val().len) > begin);
    if (pos.get_val().pladdr.is_paddr()) {
      f(pos.get_key(), pos.get_val().pladdr.get_paddr(), pos.get_val().len);
    }
    pos = co_await pos.next(c);
  }
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

  if (!is_lba_node(*extent)) {
    DEBUGT("skip non lba extent -- {}", t, *extent);
    co_return;
  }

  DEBUGT("rewriting lba extent -- {}", t, *extent);
  auto c = get_context(t);
  auto btree = co_await get_btree(t);
  co_await btree.rewrite_extent(c, extent);
  co_return;
}

BtreeLBAManager::update_mapping_ret
BtreeLBAManager::update_mapping(
  Transaction& t,
  LBAMapping mapping,
  extent_len_t prev_len,
  paddr_t prev_addr,
  LogicalChildNode& nextent)
{
  LOG_PREFIX(BtreeLBAManager::update_mapping);
  auto laddr = mapping.get_key();
  auto addr = nextent.get_paddr();
  auto len = nextent.get_length();
  auto checksum = nextent.get_last_committed_crc();
  TRACET("laddr={}, paddr {}~0x{:x} => {}~0x{:x}, crc=0x{:x}",
         t, laddr, prev_addr, prev_len, addr, len, checksum);
  assert(laddr == nextent.get_laddr());
  assert(!addr.is_null());
  assert(mapping.is_viewable());
  assert(!mapping.is_indirect());

  auto res = co_await _update_mapping(
    t,
    mapping.get_effective_cursor(),
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
    crimson::ct_error::assert_all{
      "Invalid error in BtreeLBAManager::update_mapping"
    });

  assert(res.is_alive_mapping());
  DEBUGT("laddr={}, paddr {}~0x{:x} => {}~0x{:x}, crc=0x{:x} done -- {}",
	 t, laddr, prev_addr, prev_len, addr, len, checksum, res.get_cursor());
  co_return res.get_cursor().get_refcount();
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
	  ).si_then([c, &cursor, prev_addr, len, addr,
		    checksum, FNAME](auto res) {
	      DEBUGT("cursor={}, paddr {}~0x{:x} => {}, crc=0x{:x} done -- {}",
		     c.trans, *cursor, prev_addr, len,
		     addr, checksum, res.get_cursor());
	      return update_mapping_iertr::make_ready_future();
	    },
	    update_mapping_iertr::pass_further{},
	    /* ENOENT in particular should be impossible */
	    crimson::ct_error::assert_all{
	      "Invalid error in BtreeLBAManager::update_mappings"
	    }
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
  auto btree = co_await get_btree(t);
  if (type == extent_types_t::LADDR_INTERNAL) {
    co_return co_await btree.get_internal_if_live(c, addr, laddr, len);
  } else {
    assert(type == extent_types_t::LADDR_LEAF ||
	   type == extent_types_t::DINK_LADDR_LEAF);
    co_return co_await btree.get_leaf_if_live(c, addr, laddr, len);
  }
}

BtreeLBAManager::complete_lba_mapping_ret
BtreeLBAManager::complete_indirect_lba_mapping(
  Transaction &t,
  LBAMapping mapping)
{
  assert(mapping.is_viewable());
  assert(mapping.is_indirect());
  if (mapping.is_complete_indirect()) {
    co_return mapping;
  }
  auto c = get_context(t);
  auto btree = co_await get_btree(t);
  auto cursor = co_await resolve_indirect_cursor(c, btree, *mapping.indirect_cursor);
  mapping.direct_cursor = std::move(cursor);
  co_return mapping;
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
  std::variant<laddr_t, LBACursor*> addr_or_cursor,
  int delta)
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
  return fut.si_then([delta, &t, addr, FNAME, this](auto res) {
    DEBUGT("laddr={}, delta={} done -- {}",
	   t, addr, delta,
	   res.is_alive_mapping()
	     ? res.get_cursor().val
	     : res.get_removed_mapping().map_value);
    return update_mapping_iertr::make_ready_future<
      mapping_update_result_t>(get_mapping_update_result(res));
  });
}

BtreeLBAManager::_update_mapping_ret
BtreeLBAManager::_update_mapping(
  Transaction &t,
  LBACursor &cursor,
  update_func_t &&f,
  LogicalChildNode* nextent)
{
  assert(cursor.is_viewable());
  auto c = get_context(t);
  auto btree = co_await get_btree(t);
  auto iter = btree.make_partial_iter(c, cursor);
  auto ret = f(iter.get_val());
  if (ret.refcount == 0) {
    auto laddr = cursor.key;
    iter = co_await btree.remove(c, iter);
    co_return update_mapping_ret_bare_t{
      laddr, std::move(ret), iter.get_cursor(c)};
  } else {
    iter = co_await btree.update(c, iter, ret);
    // child-ptr may already be correct,
    // see LBAManager::update_mappings()
    if (nextent && !nextent->has_parent_tracker()) {
      iter.get_leaf_node()->update_child_ptr(
	iter.get_leaf_pos(), nextent);
    }
    assert(!nextent ||
	   (nextent->has_parent_tracker()
	    && nextent->peek_parent_node().get() == iter.get_leaf_node().get()));
    LBACursorRef cursor = iter.get_cursor(c);
    assert(cursor->val);
    co_return update_mapping_ret_bare_t{std::move(cursor)};
  }
}

BtreeLBAManager::_update_mapping_ret
BtreeLBAManager::_update_mapping(
  Transaction &t,
  laddr_t addr,
  update_func_t &&f,
  LogicalChildNode* nextent)
{
  auto c = get_context(t);
  auto btree = co_await get_btree(t);
  auto iter = co_await btree.lower_bound(c, addr);
  if (iter.is_end() || iter.get_key() != addr) {
    LOG_PREFIX(BtreeLBAManager::_update_mapping);
    ERRORT("laddr={} doesn't exist", c.trans, addr);
    co_await _update_mapping_iertr::future<int>(
      crimson::ct_error::enoent::make());
  }

  auto ret = f(iter.get_val());
  if (ret.refcount == 0) {
    assert(nextent == nullptr);
    iter = co_await btree.remove(c, iter);
    co_return update_mapping_ret_bare_t(addr, ret, iter.get_cursor(c));
  } else {
    iter = co_await btree.update(c, iter, ret);
    if (nextent) {
      // nextent is provided iff unlinked,
      // also see TM::rewrite_logical_extent()
      assert(!nextent->has_parent_tracker());
      iter.get_leaf_node()->update_child_ptr(
	iter.get_leaf_pos(), nextent);
    }
    assert(!nextent ||
	   (nextent->has_parent_tracker() &&
	    nextent->peek_parent_node().get() == iter.get_leaf_node().get()));
    co_return update_mapping_ret_bare_t(iter.get_cursor(c));
  }
}

BtreeLBAManager::_get_cursor_ret
BtreeLBAManager::get_containing_cursor(
  op_context_t c,
  LBABtree &btree,
  laddr_t laddr)
{
  LOG_PREFIX(BtreeLBAManager::get_containing_cursor);
  TRACET("{}", c.trans, laddr);
  auto iter = co_await btree.upper_bound_right(c, laddr);
  if (iter.is_end() ||
      iter.get_key() > laddr ||
      iter.get_key() + iter.get_val().len <=laddr) {
    ERRORT("laddr={} doesn't exist", c.trans, laddr);
    co_await get_mapping_iertr::future<int>(
      crimson::ct_error::enoent::make());
  }
  TRACET("{} got {}, {}",
	 c.trans, laddr, iter.get_key(), iter.get_val());
  co_return iter.get_cursor(c);
}

#ifdef UNIT_TESTS_BUILT
BtreeLBAManager::get_end_mapping_ret
BtreeLBAManager::get_end_mapping(
  Transaction &t)
{
  LOG_PREFIX(BtreeLBAManager::get_end_mapping);
  DEBUGT("", t);
  auto c = get_context(t);
  auto btree = co_await get_btree(t);
  auto iter = co_await btree.end(c);
  co_return LBAMapping::create_direct(iter.get_cursor(c));
}
#endif

BtreeLBAManager::remap_ret
BtreeLBAManager::remap_mappings(
  Transaction &t,
  LBAMapping mapping,
  std::vector<remap_entry_t> remaps)
{
  LOG_PREFIX(BtreeLBAManager::remap_mappings);
  DEBUGT("{}", t, mapping);
  assert(mapping.is_viewable());
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
      std::vector<LBAMapping>(),
      [c, &btree, this, &cursor](auto &remaps, auto &mapping, auto &iter, auto &ret) {
      auto val = iter.get_val();
      assert(val.refcount == EXTENT_DEFAULT_REF_COUNT);
      assert(mapping.is_indirect() ||
	(val.pladdr.is_paddr() &&
	 val.pladdr.get_paddr().is_absolute()));
      return update_refcount(c.trans, &cursor, -1
      ).si_then([&mapping, &btree, &iter, c, &ret,
		&remaps, pladdr=val.pladdr](auto r) {
	assert(r.refcount == 0);
	auto &cursor = r.mapping.get_effective_cursor();
	iter = btree.make_partial_iter(c, cursor);
	return trans_intr::do_for_each(
	  remaps,
	  [&mapping, &btree, &iter, c, &ret, pladdr](auto &remap) {
	  assert(remap.offset + remap.len <= mapping.get_length());
	  assert((bool)remap.extent == !mapping.is_indirect());
	  lba_map_val_t val;
	  auto old_key = mapping.get_key();
	  auto new_key = (old_key + remap.offset).checked_to_laddr();
	  val.len = remap.len;
	  if (pladdr.is_laddr()) {
	    auto laddr = pladdr.get_laddr();
	    val.pladdr = (laddr + remap.offset).checked_to_laddr();
	  } else {
	    auto paddr = pladdr.get_paddr();
	    val.pladdr = paddr + remap.offset;
	  }
	  val.refcount = EXTENT_DEFAULT_REF_COUNT;
	  val.checksum = 0; // the checksum should be updated later when
			    // committing the transaction
	  return btree.insert(c, iter, new_key, std::move(val)
	  ).si_then([c, &remap, &mapping, &ret, &iter](auto p) {
	    auto &[it, inserted] = p;
	    ceph_assert(inserted);
	    auto &leaf_node = *it.get_leaf_node();
	    if (mapping.is_indirect()) {
	      leaf_node.insert_child_ptr(
		it.get_leaf_pos(),
		get_reserved_ptr<LBALeafNode, laddr_t>(),
		leaf_node.get_size() - 1 /*the size before the insert*/);
	      ret.push_back(
		LBAMapping::create_indirect(nullptr, it.get_cursor(c)));
	    } else {
	      leaf_node.insert_child_ptr(
		it.get_leaf_pos(),
		remap.extent,
		leaf_node.get_size() - 1 /*the size before the insert*/);
	      ret.push_back(
		LBAMapping::create_direct(it.get_cursor(c)));
	    }
	    return it.next(c).si_then([&iter](auto it) {
	      iter = std::move(it);
	    });
	  });
	});
      }).si_then([&mapping, &ret] {
	if (mapping.is_indirect()) {
	  auto &cursor = mapping.direct_cursor;
	  return cursor->refresh(
	  ).si_then([&ret, &mapping] {
	    for (auto &m : ret) {
	      m.direct_cursor = mapping.direct_cursor;
	    }
	  });
	}
	return base_iertr::now();
      }).si_then([this, c, &mapping, &remaps] {
	if (remaps.size() > 1 && mapping.is_indirect()) {
	  auto &cursor = mapping.direct_cursor;
	  assert(cursor->is_viewable());
	  return update_refcount(
	    c.trans, cursor.get(), 1).discard_result();
	}
	return update_refcount_iertr::now();
      }).si_then([&ret] {
	return trans_intr::parallel_for_each(
	  ret,
	  [](auto &remapped_mapping) {
	  return remapped_mapping.refresh(
	  ).si_then([&remapped_mapping](auto mapping) {
	    remapped_mapping = std::move(mapping);
	  });
	});
      }).si_then([&ret] {
	return std::move(ret);
      });
    });
  });
}

}
