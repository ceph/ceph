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
  return with_btree_state<LBABtree, lba_mapping_list_t>(
    cache, c,
    [FNAME, this, c, laddr, length](auto& btree, auto& ret)
  {
    return get_cursors(c, btree, laddr, length
    ).si_then([FNAME, this, c, laddr, length, &btree, &ret](auto cursors) {
      return seastar::do_with(
        std::move(cursors),
        [FNAME, this, c, laddr, length, &btree, &ret](auto& cursors)
      {
        return trans_intr::do_for_each(
          cursors,
          [FNAME, this, c, laddr, length, &btree, &ret](auto& cursor)
        {
          if (!cursor->is_indirect()) {
            ret.emplace_back(LBAMapping::create_direct(std::move(cursor)));
            TRACET("{}~0x{:x} got {}",
                   c.trans, laddr, length, ret.back());
            return get_mappings_iertr::now();
          }
	  assert(cursor->val->refcount == EXTENT_DEFAULT_REF_COUNT);
	  assert(cursor->val->checksum == 0);
          return resolve_indirect_cursor(c, btree, *cursor
          ).si_then([FNAME, c, &ret, &cursor, laddr, length](auto direct) {
            ret.emplace_back(LBAMapping::create_indirect(
		std::move(direct), std::move(cursor)));
            TRACET("{}~0x{:x} got {}",
                   c.trans, laddr, length, ret.back());
            return get_mappings_iertr::now();
          });
        });
      });
    });
  });
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
  return seastar::do_with(
    std::list<LBACursorRef>(),
    [FNAME, c, laddr, length, &btree](auto& ret)
  {
    return LBABtree::iterate_repeat(
      c,
      btree.upper_bound_right(c, laddr),
      [FNAME, c, laddr, length, &ret](auto& pos)
    {
      if (pos.is_end() || pos.get_key() >= (laddr + length)) {
        TRACET("{}~0x{:x} done with {} results, stop at {}",
               c.trans, laddr, length, ret.size(), pos);
        return LBABtree::iterate_repeat_ret_inner(
          interruptible::ready_future_marker{},
          seastar::stop_iteration::yes);
      }
      TRACET("{}~0x{:x} got {}, repeat ...",
             c.trans, laddr, length, pos);
      ceph_assert((pos.get_key() + pos.get_val().len) > laddr);
      ret.emplace_back(pos.get_cursor(c));
      return LBABtree::iterate_repeat_ret_inner(
        interruptible::ready_future_marker{},
        seastar::stop_iteration::no);
    }).si_then([&ret] {
      return std::move(ret);
    });
  });
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
    auto intermediate_key = indirect_cursor.get_intermediate_key();
    assert(!direct_cursor->is_indirect());
    assert(direct_cursor->get_laddr() <= intermediate_key);
    assert(direct_cursor->get_laddr() + direct_cursor->get_length()
	   >= intermediate_key + indirect_cursor.get_length());
    return std::move(direct_cursor);
  });
}

BtreeLBAManager::get_mapping_ret
BtreeLBAManager::get_mapping(
  Transaction &t,
  laddr_t laddr)
{
  LOG_PREFIX(BtreeLBAManager::get_mapping);
  TRACET("{} ...", t, laddr);
  auto c = get_context(t);
  return with_btree<LBABtree>(
    cache, c,
    [FNAME, this, c, laddr](auto& btree)
  {
    return get_cursor(c, btree, laddr
    ).si_then([FNAME, this, c, laddr, &btree](LBACursorRef cursor) {
      if (!cursor->is_indirect()) {
        TRACET("{} got direct cursor {}",
               c.trans, laddr, *cursor);
	auto mapping = LBAMapping::create_direct(std::move(cursor));
        return get_mapping_iertr::make_ready_future<
	  LBAMapping>(std::move(mapping));
      }
      assert(laddr == cursor->get_laddr());
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

BtreeLBAManager::_get_cursor_ret
BtreeLBAManager::get_cursor(
  op_context_t c,
  LBABtree& btree,
  laddr_t laddr)
{
  LOG_PREFIX(BtreeLBAManager::get_cursor);
  TRACET("{} ...", c.trans, laddr);
  return btree.lower_bound(
    c, laddr
  ).si_then([FNAME, c, laddr](auto iter) -> _get_cursor_ret {
    if (iter.is_end() || iter.get_key() != laddr) {
      ERRORT("{} doesn't exist", c.trans, laddr);
      return crimson::ct_error::enoent::make();
    }
    TRACET("{} got value {}", c.trans, laddr, iter.get_val());
    return _get_cursor_ret(
      interruptible::ready_future_marker{},
      iter.get_cursor(c));
  });
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
  auto lookup_attempts = stats.num_alloc_extents_iter_nexts;
  using OptIter = std::optional<LBABtree::iterator>;
  return seastar::do_with(
    hint, OptIter(std::nullopt),
    [this, c, &btree, hint, length, lookup_attempts, policy, FNAME]
    (laddr_t &last_end, OptIter &insert_iter)
  {
    return LBABtree::iterate_repeat(
      c,
      btree.upper_bound_right(c, hint),
      [this, c, hint, length, lookup_attempts, policy,
       &last_end, &insert_iter, FNAME](auto &iter)
    {
      ++stats.num_alloc_extents_iter_nexts;
      if (iter.is_end() ||
	  iter.get_key() >= (last_end + length)) {
	if (policy == alloc_policy_t::deterministic) {
	  ceph_assert(hint == last_end);
	}
	DEBUGT("hint: {}~0x{:x}, allocated laddr: {}, insert position: {}, "
	       "done with {} attempts",
	       c.trans, hint, length, last_end, iter,
	       stats.num_alloc_extents_iter_nexts - lookup_attempts);
	insert_iter.emplace(iter);
	return search_insert_position_iertr::make_ready_future<
	  seastar::stop_iteration>(seastar::stop_iteration::yes);
      }
      ceph_assert(policy == alloc_policy_t::linear_search);
      last_end = (iter.get_key() + iter.get_val().len).checked_to_laddr();
      TRACET("hint: {}~0x{:x}, current iter: {}, repeat ...",
	     c.trans, hint, length, iter);
      return search_insert_position_iertr::make_ready_future<
	seastar::stop_iteration>(seastar::stop_iteration::no);
    }).si_then([&last_end, &insert_iter] {
      ceph_assert(insert_iter);
      return search_insert_position_iertr::make_ready_future<
	insert_position_t>(last_end, *std::move(insert_iter));
    });
  });
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
  return with_btree<LBABtree>(
    cache,
    c,
    [this, c, hint, &alloc_infos, total_len, policy](auto &btree)
  {
    return search_insert_position(c, btree, hint, total_len, policy
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
  return with_btree<LBABtree>(
    cache,
    c,
    [this, c, hint, &alloc_infos, total_len, policy](auto &btree)
  {
    return search_insert_position(c, btree, hint, total_len, policy
    ).si_then([this, c, hint, &alloc_infos, &btree, policy](auto res) {
      if (policy != alloc_policy_t::deterministic) {
	for (auto &info : alloc_infos) {
	  auto offset = info.key.get_byte_distance<extent_len_t>(hint);
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
      [c, &btree, &iter, &ret](auto &info)
    {
      assert(info.key != L_ADDR_NULL);
      return btree.insert(
	c, iter, info.key, info.value
      ).si_then([c, &iter, &ret, &info](auto p) {
	ceph_assert(p.second);
	iter = std::move(p.first);
	auto &leaf_node = *iter.get_leaf_node();
	leaf_node.insert_child_ptr(
	  iter.get_leaf_pos(),
	  info.extent,
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
  LogicalChildNode& nextent)
{
  LOG_PREFIX(BtreeLBAManager::update_mapping);
  auto addr = nextent.get_paddr();
  auto len = nextent.get_length();
  auto checksum = nextent.get_last_committed_crc();
  TRACET("laddr={}, paddr {}~0x{:x} => {}~0x{:x}, crc=0x{:x}",
         t, laddr, prev_addr, prev_len, addr, len, checksum);
  assert(laddr == nextent.get_laddr());
  assert(!addr.is_null());
  return _update_mapping(
    t,
    laddr,
    [prev_addr, addr, prev_len, len, checksum]
    (const lba_map_val_t &in) {
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
  ).si_then([&t, laddr, prev_addr, prev_len, addr, len, checksum, FNAME](auto res) {
      assert(res.is_alive_mapping());
      DEBUGT("laddr={}, paddr {}~0x{:x} => {}~0x{:x}, crc=0x{:x} done -- {}",
             t, laddr, prev_addr, prev_len, addr, len, checksum, res.get_cursor());
      return update_mapping_iertr::make_ready_future<
	extent_ref_count_t>(res.get_cursor().get_refcount());
    },
    update_mapping_iertr::pass_further{},
    /* ENOENT in particular should be impossible */
    crimson::ct_error::assert_all{
      "Invalid error in BtreeLBAManager::update_mapping"
    }
  );
}

BtreeLBAManager::update_mappings_ret
BtreeLBAManager::update_mappings(
  Transaction& t,
  const std::list<LogicalChildNodeRef>& extents)
{
  return trans_intr::do_for_each(extents, [this, &t](auto &extent) {
    LOG_PREFIX(BtreeLBAManager::update_mappings);
    auto laddr = extent->get_laddr();
    auto prev_addr = extent->get_prior_paddr_and_reset();
    auto len = extent->get_length();
    auto addr = extent->get_paddr();
    auto checksum = extent->get_last_committed_crc();
    TRACET("laddr={}, paddr {}~0x{:x} => {}, crc=0x{:x}",
           t, laddr, prev_addr, len, addr, checksum);
    assert(!addr.is_null());
    return _update_mapping(
      t,
      laddr,
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
    ).si_then([&t, laddr, prev_addr, len, addr, checksum, FNAME](auto res) {
        DEBUGT("laddr={}, paddr {}~0x{:x} => {}, crc=0x{:x} done -- {}",
               t, laddr, prev_addr, len, addr, checksum, res.get_cursor());
        return update_mapping_iertr::make_ready_future();
      },
      update_mapping_iertr::pass_further{},
      /* ENOENT in particular should be impossible */
      crimson::ct_error::assert_all{
        "Invalid error in BtreeLBAManager::update_mappings"
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

BtreeLBAManager::refresh_lba_mapping_ret
BtreeLBAManager::refresh_lba_mapping(Transaction &t, LBAMapping mapping)
{
  assert(mapping.is_linked_direct());
  if (mapping.is_viewable()) {
    return refresh_lba_mapping_iertr::make_ready_future<
      LBAMapping>(std::move(mapping));
  }
  auto c = get_context(t);
  return with_btree_state<LBABtree, LBAMapping>(
    cache,
    c,
    std::move(mapping),
    [c, this](LBABtree &btree, LBAMapping &mapping) mutable
  {
    return refresh_lba_cursor(c, btree, *mapping.direct_cursor
    ).si_then([c, this, &btree, &mapping] {
      if (mapping.indirect_cursor) {
	return refresh_lba_cursor(c, btree, *mapping.indirect_cursor);
      }
      return refresh_lba_cursor_iertr::make_ready_future();
#ifndef NDEBUG
    }).si_then([&mapping] {
      assert(mapping.is_viewable());
#endif
    });
  });
}

BtreeLBAManager::refresh_lba_cursor_ret
BtreeLBAManager::refresh_lba_cursor(
  op_context_t c,
  LBABtree &btree,
  LBACursor &cursor)
{
  LOG_PREFIX(BtreeLBAManager::refresh_lba_cursor);
  stats.num_refresh_parent_total++;

  if (!cursor.parent->is_valid()) {
    stats.num_refresh_invalid_parent++;
    TRACET("cursor {} parent is invalid, re-search from scratch",
	   c.trans, cursor);
    return btree.lower_bound(c, cursor.get_laddr()
    ).si_then([&cursor](LBABtree::iterator iter) {
      auto leaf = iter.get_leaf_node();
      cursor.parent = leaf;
      cursor.modifications = leaf->modifications;
      cursor.pos = iter.get_leaf_pos();
      if (!cursor.is_end()) {
	ceph_assert(!iter.is_end());
	ceph_assert(iter.get_key() == cursor.get_laddr());
	cursor.val = iter.get_val();
	assert(cursor.is_viewable());
      }
    });
  }

  auto [viewable, state] = cursor.parent->is_viewable_by_trans(c.trans);
  auto leaf = cursor.parent->cast<LBALeafNode>();

  TRACET("cursor: {} viewable: {} state: {}",
	 c.trans, cursor, viewable, state);

  if (!viewable) {
    stats.num_refresh_unviewable_parent++;
    leaf = leaf->find_pending_version(c.trans, cursor.get_laddr());
    cursor.parent = leaf;
  }

  if (!viewable ||
      leaf->modified_since(cursor.modifications)) {
    if (viewable) {
      stats.num_refresh_modified_viewable_parent++;
    }

    cursor.modifications = leaf->modifications;
    if (cursor.is_end()) {
      cursor.pos = leaf->get_size();
      assert(!cursor.val);
    } else {
      auto i = leaf->lower_bound(cursor.get_laddr());
      cursor.pos = i.get_offset();
      cursor.val = i.get_val();

      auto iter = LBALeafNode::iterator(leaf.get(), cursor.pos);
      ceph_assert(iter.get_key() == cursor.key);
      ceph_assert(iter.get_val() == cursor.val);
      assert(cursor.is_viewable());
    }
  }

  return refresh_lba_cursor_iertr::make_ready_future();
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
      sm::make_counter(
        "refresh_parent_total",
        stats.num_refresh_parent_total,
        sm::description("total number of refreshed cursors")
      ),
      sm::make_counter(
        "refresh_invalid_parent",
        stats.num_refresh_invalid_parent,
        sm::description("total number of refreshed cursors with invalid parents")
      ),
      sm::make_counter(
        "refresh_unviewable_parent",
        stats.num_refresh_unviewable_parent,
        sm::description("total number of refreshed cursors with unviewable parents")
      ),
      sm::make_counter(
        "refresh_modified_viewable_parent",
        stats.num_refresh_modified_viewable_parent,
        sm::description("total number of refreshed cursors with viewable but modified parents")
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
      ceph_assert(!iter.is_end());
      laddr_t key = iter.get_key();
      ceph_assert(key <= addr);
      auto val = iter.get_val();
      ceph_assert(key + val.len >= addr + len);
      ceph_assert(val.pladdr.is_paddr());
      ceph_assert(val.refcount >= 1);
      val.refcount -= 1;

      LOG_PREFIX(BtreeLBAManager::_decref_intermediate);
      TRACET("decreased refcount of intermediate key {} -- {}",
	     c.trans, key, val);

      if (val.refcount == 0) {
	return btree.remove(c, iter
	).si_then([key, val] {
	  return ref_iertr::make_ready_future<
	    update_mapping_ret_bare_t>(key, val);
	});
      } else {
	return btree.update(c, iter, val
	).si_then([c](auto iter) {
	  return ref_iertr::make_ready_future<
	    update_mapping_ret_bare_t>(iter.get_cursor(c));
	});
      }
    });
  });
}

BtreeLBAManager::remap_ret
BtreeLBAManager::remap_mappings(
  Transaction &t,
  LBAMapping orig_mapping,
  std::vector<remap_entry_t> remaps,
  std::vector<LogicalChildNodeRef> extents)
{
  LOG_PREFIX(BtreeLBAManager::remap_mappings);
  struct state_t {
    LBAMapping orig_mapping;
    std::vector<remap_entry_t> remaps;
    std::vector<LogicalChildNodeRef> extents;
    std::vector<alloc_mapping_info_t> alloc_infos;
    std::vector<LBAMapping> ret;
  };
  return seastar::do_with(
    state_t(std::move(orig_mapping), std::move(remaps), std::move(extents), {}, {}),
    [this, &t, FNAME](state_t &state)
  {
    return update_refcount(
      t, state.orig_mapping.get_key(), -1, false
    ).si_then([this, &t, &state, FNAME](auto ret) {
      // Remapping the shared direct mapping is prohibited,
      // the refcount of indirect mapping should always be 1.
      ceph_assert(ret.is_removed_mapping());

      auto orig_laddr = state.orig_mapping.get_key();
      if (!state.orig_mapping.is_indirect()) {
	auto &addr = ret.get_removed_mapping().map_value.pladdr;
	ceph_assert(addr.is_paddr() && !addr.get_paddr().is_zero());
	return alloc_extents(
	  t,
	  (state.remaps.front().offset + orig_laddr).checked_to_laddr(),
	  std::move(state.extents),
	  EXTENT_DEFAULT_REF_COUNT
	).si_then([&state](auto ret) {
	  state.ret = std::move(ret);
	  return remap_iertr::make_ready_future();
	});
      }

      extent_len_t orig_len = state.orig_mapping.get_length();
      auto intermediate_key = state.orig_mapping.get_intermediate_key();
      ceph_assert(intermediate_key != L_ADDR_NULL);
      DEBUGT("remap indirect mapping {}", t, state.orig_mapping);
      for (auto &remap : state.remaps) {
	DEBUGT("remap 0x{:x}~0x{:x}", t, remap.offset, remap.len);
	ceph_assert(remap.len != 0);
	ceph_assert(remap.offset + remap.len <= orig_len);
	auto remapped_laddr = (orig_laddr + remap.offset)
	    .checked_to_laddr();
	auto remapped_intermediate_key = (intermediate_key + remap.offset)
	    .checked_to_laddr();
	state.alloc_infos.emplace_back(
	  alloc_mapping_info_t::create_indirect(
	    remapped_laddr, remap.len, remapped_intermediate_key));
      }

      return alloc_sparse_mappings(
	t, state.alloc_infos.front().key, state.alloc_infos,
	alloc_policy_t::deterministic
      ).si_then([&t, &state, this](std::list<LBACursorRef> cursors) {
	return seastar::futurize_invoke([&t, &state, this] {
	  if (state.remaps.size() > 1) {
	    auto base = state.orig_mapping.get_intermediate_base();
	    return update_refcount(
	      t, base, state.remaps.size() - 1, false
	    ).si_then([](update_mapping_ret_bare_t ret) {
	      return ret.take_cursor();
	    });
	  } else {
	    return remap_iertr::make_ready_future<
	      LBACursorRef>(state.orig_mapping.direct_cursor->duplicate());
	  }
	}).si_then([&state, cursors=std::move(cursors)](auto direct) mutable {
	  for (auto &cursor : cursors) {
	    state.ret.emplace_back(LBAMapping::create_indirect(
	      direct->duplicate(), std::move(cursor)));
	  }
	  return remap_iertr::make_ready_future();
	});
      });
    }).si_then([&state] {
      assert(state.ret.size() == state.remaps.size());
#ifndef NDEBUG
      auto mapping_it = state.ret.begin();
      auto remap_it = state.remaps.begin();
      for (;mapping_it != state.ret.end(); mapping_it++, remap_it++) {
	auto &mapping = *mapping_it;
	auto &remap = *remap_it;
	assert(mapping.get_key() == state.orig_mapping.get_key() + remap.offset);
	assert(mapping.get_length() == remap.len);
      }
#endif
      return remap_iertr::make_ready_future<
	std::vector<LBAMapping>>(std::move(state.ret));
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
    DEBUGT("laddr={}, delta={} done -- {}",
	   t, addr, delta,
	   res.is_alive_mapping()
	     ? res.get_cursor().val
	     : res.get_removed_mapping().map_value);
    if (res.is_removed_mapping() && cascade_remove &&
	res.get_removed_mapping().map_value.pladdr.is_laddr()) {
      auto &val = res.get_removed_mapping().map_value;
      TRACET("decref intermediate {} -> {}",
	     t, addr, val.pladdr.get_laddr());
      return _decref_intermediate(t, val.pladdr.get_laddr(), val.len
      ).handle_error_interruptible(
	update_mapping_iertr::pass_further{},
	crimson::ct_error::assert_all{
	  "unexpect ENOENT"
	}
      );
    }
    return update_mapping_iertr::make_ready_future<
      update_mapping_ret_bare_t>(std::move(res));
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
  return with_btree<LBABtree>(
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
	  assert(nextent == nullptr);
	  return btree.remove(
	    c,
	    iter
	  ).si_then([addr, ret] {
	    return update_mapping_ret_bare_t(addr, ret);
	  });
	} else {
	  return btree.update(
	    c,
	    iter,
	    ret
	  ).si_then([c, nextent](auto iter) {
	    if (nextent) {
	      // nextent is provided iff unlinked,
              // also see TM::rewrite_logical_extent()
	      assert(!nextent->has_parent_tracker());
	      iter.get_leaf_node()->update_child_ptr(
		iter.get_leaf_pos(), nextent);
	    }
	    assert(!nextent || 
	           (nextent->has_parent_tracker() &&
		    nextent->get_parent_node().get() == iter.get_leaf_node().get()));
	    return update_mapping_ret_bare_t(iter.get_cursor(c));
	  });
	}
      });
    });
}

}
