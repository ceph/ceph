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
		seastar::make_ready_future<
		  CachedExtentRef>(CachedExtentRef()))};
    }
  } else {
    return {false,
	    trans_intr::make_interruptible(
	      seastar::make_ready_future<
		CachedExtentRef>(CachedExtentRef()))};
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
	  ceph_assert((pos.get_key() + pos.get_val().len) > offset);
	  ret.push_back(pos.get_pin());
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
  return with_btree_ret<LBABtree, LBAPinRef>(
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
	  auto e = iter.get_pin();
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
  paddr_t addr)
{
  struct state_t {
    laddr_t last_end;

    std::optional<typename LBABtree::iterator> insert_iter;
    std::optional<typename LBABtree::iterator> ret;

    state_t(laddr_t hint) : last_end(hint) {}
  };

  LOG_PREFIX(BtreeLBAManager::alloc_extent);
  TRACET("{}~{}, hint={}", t, addr, len, hint);
  auto c = get_context(t);
  ++stats.num_alloc_extents;
  auto lookup_attempts = stats.num_alloc_extents_iter_nexts;
  return crimson::os::seastore::with_btree_state<LBABtree, state_t>(
    cache,
    c,
    hint,
    [this, FNAME, c, hint, len, addr, lookup_attempts, &t](auto &btree, auto &state) {
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
	    state.last_end = pos.get_key() + pos.get_val().len;
	    TRACET("{}~{}, hint={}, state: {}~{}, repeat ... -- {}",
                   t, addr, len, hint,
                   pos.get_key(), pos.get_val().len,
                   pos.get_val());
	    return typename LBABtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::no);
	  }
	}).si_then([FNAME, c, addr, len, hint, &btree, &state] {
	  return btree.insert(
	    c,
	    *state.insert_iter,
	    state.last_end,
	    lba_map_val_t{len, addr, 1, 0}
	  ).si_then([&state, FNAME, c, addr, len, hint](auto &&p) {
	    auto [iter, inserted] = std::move(p);
	    TRACET("{}~{}, hint={}, inserted at {}",
	           c.trans, addr, len, hint, state.last_end);
	    ceph_assert(inserted);
	    state.ret = iter;
	  });
	});
    }).si_then([](auto &&state) {
      return state.ret->get_pin();
    });
}

static bool is_lba_node(const CachedExtent &e)
{
  return is_lba_node(e.get_type());
}

btree_range_pin_t<laddr_t> &BtreeLBAManager::get_pin(
  CachedExtent &e)
{
  if (is_lba_node(e)) {
    return e.cast<LBANode>()->pin;
  } else if (e.is_logical()) {
    return static_cast<BtreeLBAPin &>(
      e.cast<LogicalCachedExtent>()->get_pin()).get_range_pin();
  } else {
    ceph_abort_msg("impossible");
  }
}

static depth_t get_depth(const CachedExtent &e)
{
  if (is_lba_node(e)) {
    return e.cast<LBANode>()->get_node_meta().depth;
  } else if (e.is_logical()) {
    return 0;
  } else {
    ceph_assert(0 == "currently impossible");
    return 0;
  }
}

void BtreeLBAManager::complete_transaction(
  Transaction &t,
  std::vector<CachedExtentRef> &to_clear,
  std::vector<CachedExtentRef> &to_link)
{
  LOG_PREFIX(BtreeLBAManager::complete_transaction);
  DEBUGT("start", t);
  // need to call check_parent from leaf->parent
  std::sort(
    to_clear.begin(), to_clear.end(),
    [](auto &l, auto &r) { return get_depth(*l) < get_depth(*r); });

  for (auto &e: to_clear) {
    auto &pin = get_pin(*e);
    DEBUGT("retiring extent {} -- {}", t, pin, *e);
    pin_set.retire(pin);
  }

  std::sort(
    to_link.begin(), to_link.end(),
    [](auto &l, auto &r) -> bool { return get_depth(*l) > get_depth(*r); });

  for (auto &e : to_link) {
    DEBUGT("linking extent -- {}", t, *e);
    pin_set.add_pin(get_pin(*e));
  }

  for (auto &e: to_clear) {
    auto &pin = get_pin(*e);
    TRACET("checking extent {} -- {}", t, pin, *e);
    pin_set.check_parent(pin);
  }
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
	logn->set_pin(iter.get_pin());
	ceph_assert(iter.get_val().len == e->get_length());
	if (c.pins) {
	  c.pins->add_pin(
	    static_cast<BtreeLBAPin&>(logn->get_pin()).get_range_pin());
	}
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
  paddr_t addr)
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
    }
  ).si_then([&t, laddr, prev_addr, addr, FNAME](auto result) {
      DEBUGT("laddr={}, paddr {} => {} done -- {}",
             t, laddr, prev_addr, addr, result);
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
  return _update_mapping(
    t,
    addr,
    [delta](const lba_map_val_t &in) {
      lba_map_val_t out = in;
      ceph_assert((int)out.refcount + delta >= 0);
      out.refcount += delta;
      return out;
    }
  ).si_then([&t, addr, delta, FNAME](auto result) {
    DEBUGT("laddr={}, delta={} done -- {}", t, addr, delta, result);
    return ref_update_result_t{
      result.refcount,
      result.paddr,
      result.len
     };
  });
}

BtreeLBAManager::_update_mapping_ret
BtreeLBAManager::_update_mapping(
  Transaction &t,
  laddr_t addr,
  update_func_t &&f)
{
  auto c = get_context(t);
  return with_btree_ret<LBABtree, lba_map_val_t>(
    cache,
    c,
    [f=std::move(f), c, addr](auto &btree) mutable {
      return btree.lower_bound(
	c, addr
      ).si_then([&btree, f=std::move(f), c, addr](auto iter)
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
	    return ret;
	  });
	} else {
	  return btree.update(
	    c,
	    iter,
	    ret
	  ).si_then([ret](auto) {
	    return ret;
	  });
	}
      });
    });
}

}
