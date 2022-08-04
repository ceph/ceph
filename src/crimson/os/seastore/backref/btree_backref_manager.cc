// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/backref/btree_backref_manager.h"

SET_SUBSYS(seastore_backref);

namespace crimson::os::seastore {

template<>
Transaction::tree_stats_t& get_tree_stats<
  crimson::os::seastore::backref::BackrefBtree>(Transaction &t) {
  return t.get_backref_tree_stats();
}

template<>
phy_tree_root_t& get_phy_tree_root<
  crimson::os::seastore::backref::BackrefBtree>(root_t &r) {
  return r.backref_root;
}

}

namespace crimson::os::seastore::backref {

static depth_t get_depth(const CachedExtent &e)
{
  assert(is_backref_node(e.get_type()));
  return e.cast<BackrefNode>()->get_node_meta().depth;
}

BtreeBackrefManager::mkfs_ret
BtreeBackrefManager::mkfs(
  Transaction &t)
{
  LOG_PREFIX(BtreeBackrefManager::mkfs);
  INFOT("start", t);
  return cache.get_root(t).si_then([this, &t](auto croot) {
    croot->get_root().backref_root = BackrefBtree::mkfs(get_context(t));
    return mkfs_iertr::now();
  }).handle_error_interruptible(
    mkfs_iertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in BtreeBackrefManager::mkfs"
    }
  );
}

BtreeBackrefManager::get_mapping_ret
BtreeBackrefManager::get_mapping(
  Transaction &t,
  paddr_t offset)
{
  LOG_PREFIX(BtreeBackrefManager::get_mapping);
  TRACET("{}", t, offset);
  auto c = get_context(t);
  return with_btree_ret<BackrefBtree, BackrefPinRef>(
    cache,
    c,
    [c, offset](auto &btree) {
    return btree.lower_bound(
      c, offset
    ).si_then([offset, c](auto iter) -> get_mapping_ret {
      LOG_PREFIX(BtreeBackrefManager::get_mapping);
      if (iter.is_end() || iter.get_key() != offset) {
	ERRORT("{} doesn't exist", c.trans, offset);
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

BtreeBackrefManager::get_mappings_ret
BtreeBackrefManager::get_mappings(
  Transaction &t,
  paddr_t offset,
  paddr_t end)
{
  LOG_PREFIX(BtreeBackrefManager::get_mappings);
  TRACET("{}~{}", t, offset, end);
  auto c = get_context(t);
  return with_btree_state<BackrefBtree, backref_pin_list_t>(
    cache,
    c,
    [c, offset, end](auto &btree, auto &ret) {
      return BackrefBtree::iterate_repeat(
	c,
	btree.upper_bound_right(c, offset),
	[&ret, offset, end, c](auto &pos) {
	  LOG_PREFIX(BtreeBackrefManager::get_mappings);
	  if (pos.is_end() || pos.get_key() >= end) {
	    TRACET("{}~{} done with {} results",
	           c.trans, offset, end, ret.size());
	    return BackrefBtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::yes);
	  }
	  TRACET("{}~{} got {}, {}, repeat ...",
	         c.trans, offset, end, pos.get_key(), pos.get_val());
	  ceph_assert((pos.get_key().add_offset(pos.get_val().len)) > offset);
	  ret.push_back(pos.get_pin());
	  return BackrefBtree::iterate_repeat_ret_inner(
	    interruptible::ready_future_marker{},
	    seastar::stop_iteration::no);
	});
    });
}

BtreeBackrefManager::new_mapping_ret
BtreeBackrefManager::new_mapping(
  Transaction &t,
  paddr_t key,
  extent_len_t len,
  laddr_t addr,
  extent_types_t type)
{
  ceph_assert(
    is_aligned(
      key.as_seg_paddr().get_segment_off(),
      (uint64_t)cache.get_block_size()));
  struct state_t {
    paddr_t last_end;

    std::optional<BackrefBtree::iterator> insert_iter;
    std::optional<BackrefBtree::iterator> ret;

    state_t(paddr_t hint) : last_end(hint) {}
  };

  LOG_PREFIX(BtreeBackrefManager::new_mapping);
  DEBUGT("{}~{}, paddr={}", t, addr, len, key);
  backref_map_val_t val{len, addr, type};
  auto c = get_context(t);
  //++stats.num_alloc_extents;
  //auto lookup_attempts = stats.num_alloc_extents_iter_nexts;
  return crimson::os::seastore::with_btree_state<BackrefBtree, state_t>(
    cache,
    c,
    key,
    [val, c, key, len, addr, /*lookup_attempts,*/ &t]
    (auto &btree, auto &state) {
      return BackrefBtree::iterate_repeat(
	c,
	btree.upper_bound_right(c, key),
	[&state, len, addr, &t, key/*, lookup_attempts*/](auto &pos) {
	  LOG_PREFIX(BtreeBackrefManager::new_mapping);
	  //++stats.num_alloc_extents_iter_nexts;
	  if (pos.is_end()) {
	    DEBUGT("{}~{}, paddr={}, state: end, insert at {}",
                   t, addr, len, key,
                   //stats.num_alloc_extents_iter_nexts - lookup_attempts,
                   state.last_end);
	    state.insert_iter = pos;
	    return BackrefBtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::yes);
	  } else if (pos.get_key() >= (state.last_end.add_offset(len))) {
	    DEBUGT("{}~{}, paddr={}, state: {}~{}, "
		   "insert at {} -- {}",
                   t, addr, len, key,
                   pos.get_key(), pos.get_val().len,
                   //stats.num_alloc_extents_iter_nexts - lookup_attempts,
                   state.last_end,
                   pos.get_val());
	    state.insert_iter = pos;
	    return BackrefBtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::yes);
	  } else {
	    ERRORT("{}~{}, paddr={}, state: {}~{}, repeat ... -- {}",
                   t, addr, len, key,
                   pos.get_key(), pos.get_val().len,
                   pos.get_val());
	    ceph_abort("not possible for the backref tree");
	    return BackrefBtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::no);
	  }
	}).si_then([c, addr, len, key, &btree, &state, val] {
	  return btree.insert(
	    c,
	    *state.insert_iter,
	    state.last_end,
	    val
	  ).si_then([&state, c, addr, len, key](auto &&p) {
	    LOG_PREFIX(BtreeBackrefManager::alloc_extent);
	    auto [iter, inserted] = std::move(p);
	    TRACET("{}~{}, paddr={}, inserted at {}",
	           c.trans, addr, len, key, state.last_end);
	    ceph_assert(inserted);
	    state.ret = iter;
	  });
	});
    }).si_then([](auto &&state) {
      return state.ret->get_pin();
    });
}

BtreeBackrefManager::merge_cached_backrefs_ret
BtreeBackrefManager::merge_cached_backrefs(
  Transaction &t,
  const journal_seq_t &limit,
  const uint64_t max)
{
  LOG_PREFIX(BtreeBackrefManager::merge_cached_backrefs);
  DEBUGT("insert up to {}", t, limit);
  return seastar::do_with(
    limit,
    JOURNAL_SEQ_NULL,
    [this, &t, max](auto &limit, auto &inserted_to) {
    auto &backref_buffer = cache.get_backref_buffer();
    if (backref_buffer) {
      return seastar::do_with(
	backref_buffer->backrefs_by_seq.begin(),
	JOURNAL_SEQ_NULL,
	[this, &t, &limit, &backref_buffer, max](auto &iter, auto &inserted_to) {
	return trans_intr::repeat(
	  [&iter, this, &t, &limit, &backref_buffer, max, &inserted_to]()
	  -> merge_cached_backrefs_iertr::future<seastar::stop_iteration> {
	  if (iter == backref_buffer->backrefs_by_seq.end())
	    return seastar::make_ready_future<seastar::stop_iteration>(
	      seastar::stop_iteration::yes);
	  auto &seq = iter->first;
	  auto &backref_list = iter->second.br_list;
	  LOG_PREFIX(BtreeBackrefManager::merge_cached_backrefs);
	  DEBUGT("seq {}, limit {}, num_fresh_backref {}"
	    , t, seq, limit, t.get_num_fresh_backref());
	  if (seq <= limit && t.get_num_fresh_backref() * BACKREF_NODE_SIZE < max) {
	    inserted_to = seq;
	    return trans_intr::do_for_each(
	      backref_list,
	      [this, &t](auto &backref) {
	      LOG_PREFIX(BtreeBackrefManager::merge_cached_backrefs);
	      if (backref.laddr != L_ADDR_NULL) {
		DEBUGT("new mapping: {}~{} -> {}",
		  t, backref.paddr, backref.len, backref.laddr);
		return new_mapping(
		  t,
		  backref.paddr,
		  backref.len,
		  backref.laddr,
		  backref.type).si_then([](auto &&pin) {
		  return seastar::now();
		});
	      } else {
		DEBUGT("remove mapping: {}", t, backref.paddr);
		return remove_mapping(
		  t,
		  backref.paddr).si_then([](auto&&) {
		  return seastar::now();
		}).handle_error_interruptible(
		  crimson::ct_error::input_output_error::pass_further(),
		  crimson::ct_error::assert_all("no enoent possible")
		);
	      }
	    }).si_then([&iter] {
	      iter++;
	      return seastar::make_ready_future<seastar::stop_iteration>(
		seastar::stop_iteration::no);
	    });
	  }
	  return seastar::make_ready_future<seastar::stop_iteration>(
	    seastar::stop_iteration::yes);
	}).si_then([&inserted_to] {
	  return seastar::make_ready_future<journal_seq_t>(
	    std::move(inserted_to));
	});
      });
    }
    return merge_cached_backrefs_iertr::make_ready_future<journal_seq_t>(
      std::move(inserted_to));
  });
}

BtreeBackrefManager::scan_mapped_space_ret
BtreeBackrefManager::scan_mapped_space(
  Transaction &t,
  BtreeBackrefManager::scan_mapped_space_func_t &&f)
{
  LOG_PREFIX(BtreeBackrefManager::scan_mapped_space);
  DEBUGT("start", t);
  auto c = get_context(t);
  return seastar::do_with(
    std::move(f),
    [this, c](auto &visitor) {
      return with_btree<BackrefBtree>(
	cache,
	c,
	[c, &visitor](auto &btree) {
	  return BackrefBtree::iterate_repeat(
	    c,
	    btree.lower_bound(
	      c,
	      paddr_t::make_seg_paddr(
		segment_id_t{0, 0}, 0),
	      &visitor),
	    [&visitor](auto &pos) {
	      if (pos.is_end()) {
		return BackrefBtree::iterate_repeat_ret_inner(
		  interruptible::ready_future_marker{},
		  seastar::stop_iteration::yes);
	      }
	      visitor(pos.get_key(), pos.get_val().len, 0, pos.get_val().type);
	      return BackrefBtree::iterate_repeat_ret_inner(
		interruptible::ready_future_marker{},
		seastar::stop_iteration::no);
	    },
	    &visitor);
	});
    });
}

BtreeBackrefManager::base_iertr::future<> _init_cached_extent(
  op_context_t<paddr_t> c,
  const CachedExtentRef &e,
  BackrefBtree &btree,
  bool &ret)
{
  return btree.init_cached_extent(c, e
  ).si_then([&ret](bool is_alive) {
    ret = is_alive;
  });
}

BtreeBackrefManager::init_cached_extent_ret BtreeBackrefManager::init_cached_extent(
  Transaction &t,
  CachedExtentRef e)
{
  LOG_PREFIX(BtreeBackrefManager::init_cached_extent);
  TRACET("{}", t, *e);
  return seastar::do_with(bool(), [this, e, &t](bool &ret) {
    auto c = get_context(t);
    return with_btree<BackrefBtree>(cache, c, [c, e, &ret](auto &btree)
      -> base_iertr::future<> {
      LOG_PREFIX(BtreeBackrefManager::init_cached_extent);
      DEBUGT("extent {}", c.trans, *e);
      return _init_cached_extent(c, e, btree, ret);
    }).si_then([&ret] { return ret; });
  });
}

BtreeBackrefManager::rewrite_extent_ret
BtreeBackrefManager::rewrite_extent(
  Transaction &t,
  CachedExtentRef extent)
{
  auto c = get_context(t);
  return with_btree<BackrefBtree>(
    cache,
    c,
    [c, extent](auto &btree) mutable {
    return btree.rewrite_extent(c, extent);
  });
}

BtreeBackrefManager::remove_mapping_ret
BtreeBackrefManager::remove_mapping(
  Transaction &t,
  paddr_t addr)
{
  auto c = get_context(t);
  return with_btree_ret<BackrefBtree, remove_mapping_result_t>(
    cache,
    c,
    [c, addr](auto &btree) mutable {
      return btree.lower_bound(
	c, addr
      ).si_then([&btree, c, addr](auto iter)
		-> remove_mapping_ret {
	if (iter.is_end() || iter.get_key() != addr) {
	  LOG_PREFIX(BtreeBackrefManager::remove_mapping);
	  DEBUGT("paddr={} doesn't exist", c.trans, addr);
	  return remove_mapping_iertr::make_ready_future<
	    remove_mapping_result_t>(remove_mapping_result_t());
	}

	auto ret = remove_mapping_result_t{
	  iter.get_key(),
	  iter.get_val().len,
	  iter.get_val().laddr};
	return btree.remove(
	  c,
	  iter
	).si_then([ret] {
	  return ret;
	});
      });
    });
}

void BtreeBackrefManager::complete_transaction(
  Transaction &t,
  std::vector<CachedExtentRef> &to_clear,
  std::vector<CachedExtentRef> &to_link)
{
  LOG_PREFIX(BtreeBackrefManager::complete_transaction);
  DEBUGT("start", t);
  // need to call check_parent from leaf->parent
  std::sort(
    to_clear.begin(), to_clear.end(),
    [](auto &l, auto &r) { return get_depth(*l) < get_depth(*r); });

  for (auto &e: to_clear) {
    auto &pin = e->cast<BackrefNode>()->pin;
    DEBUGT("retiring extent {} -- {}", t, pin, *e);
    pin_set.retire(pin);
  }

  std::sort(
    to_link.begin(), to_link.end(),
    [](auto &l, auto &r) -> bool { return get_depth(*l) > get_depth(*r); });

  for (auto &e : to_link) {
    DEBUGT("linking extent -- {}", t, *e);
    pin_set.add_pin(e->cast<BackrefNode>()->pin);
  }

  for (auto &e: to_clear) {
    auto &pin = e->cast<BackrefNode>()->pin;
    TRACET("checking extent {} -- {}", t, pin, *e);
    pin_set.check_parent(pin);
  }
}

Cache::backref_buf_entry_query_set_t
BtreeBackrefManager::get_cached_backref_entries_in_range(
  paddr_t start,
  paddr_t end)
{
  return cache.get_backref_entries_in_range(start, end);
}

const backref_set_t&
BtreeBackrefManager::get_cached_backrefs()
{
  return cache.get_backrefs();
}

Cache::backref_extent_buf_entry_query_set_t
BtreeBackrefManager::get_cached_backref_extents_in_range(
  paddr_t start,
  paddr_t end)
{
  return cache.get_backref_extents_in_range(start, end);
}

void BtreeBackrefManager::cache_new_backref_extent(
  paddr_t paddr,
  extent_types_t type)
{
  return cache.add_backref_extent(paddr, type);
}

BtreeBackrefManager::retrieve_backref_extents_ret
BtreeBackrefManager::retrieve_backref_extents(
  Transaction &t,
  Cache::backref_extent_buf_entry_query_set_t &&backref_extents,
  std::vector<CachedExtentRef> &extents)
{
  return trans_intr::parallel_for_each(
    backref_extents,
    [this, &extents, &t](auto &ent) {
    // only the gc fiber which is single can rewrite backref extents,
    // so it must be alive
    assert(is_backref_node(ent.type));
    LOG_PREFIX(BtreeBackrefManager::retrieve_backref_extents);
    DEBUGT("getting backref extent of type {} at {}",
      t,
      ent.type,
      ent.paddr);
    return cache.get_extent_by_type(
      t, ent.type, ent.paddr, L_ADDR_NULL, BACKREF_NODE_SIZE
    ).si_then([&extents](auto ext) {
      extents.emplace_back(std::move(ext));
    });
  });
}

} // namespace crimson::os::seastore::backref
