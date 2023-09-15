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

template<>
const get_phy_tree_root_node_ret get_phy_tree_root_node<
  crimson::os::seastore::backref::BackrefBtree>(
  const RootBlockRef &root_block, op_context_t<paddr_t> c) {
  auto backref_root = root_block->backref_root_node;
  if (backref_root) {
    ceph_assert(backref_root->is_initial_pending()
      == root_block->is_pending());
    return {true,
	    trans_intr::make_interruptible(
	      c.cache.get_extent_viewable_by_trans(c.trans, backref_root))};
  } else if (root_block->is_pending()) {
    auto &prior = static_cast<RootBlock&>(*root_block->get_prior_instance());
    backref_root = prior.backref_root_node;
    if (backref_root) {
      return {true,
	      trans_intr::make_interruptible(
		c.cache.get_extent_viewable_by_trans(c.trans, backref_root))};
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
void link_phy_tree_root_node(RootBlockRef &root_block, ROOT* backref_root) {
  root_block->backref_root_node = backref_root;
  ceph_assert(backref_root != nullptr);
  backref_root->root_block = root_block;
}

template void link_phy_tree_root_node(
  RootBlockRef &root_block, backref::BackrefInternalNode* backref_root);
template void link_phy_tree_root_node(
  RootBlockRef &root_block, backref::BackrefLeafNode* backref_root);
template void link_phy_tree_root_node(
  RootBlockRef &root_block, backref::BackrefNode* backref_root);

template <>
void unlink_phy_tree_root_node<paddr_t>(RootBlockRef &root_block) {
  root_block->backref_root_node = nullptr;
}

}

namespace crimson::os::seastore::backref {

BtreeBackrefManager::mkfs_ret
BtreeBackrefManager::mkfs(
  Transaction &t)
{
  LOG_PREFIX(BtreeBackrefManager::mkfs);
  INFOT("start", t);
  return cache.get_root(t).si_then([this, &t](auto croot) {
    assert(croot->is_mutation_pending());
    croot->get_root().backref_root = BackrefBtree::mkfs(croot, get_context(t));
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
  return with_btree_ret<BackrefBtree, BackrefMappingRef>(
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
	return get_mapping_ret(
	  interruptible::ready_future_marker{},
	  iter.get_pin(c));
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
	  ret.emplace_back(pos.get_pin(c));
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
      key.get_addr_type() == paddr_types_t::SEGMENT ?
	key.as_seg_paddr().get_segment_off() :
	key.as_blk_paddr().get_device_off(),
      cache.get_block_size()));
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
	    val,
	    nullptr
	  ).si_then([&state, c, addr, len, key](auto &&p) {
	    LOG_PREFIX(BtreeBackrefManager::new_mapping);
	    auto [iter, inserted] = std::move(p);
	    TRACET("{}~{}, paddr={}, inserted at {}, leaf {}",
	           c.trans, addr, len, key, state.last_end, *iter.get_leaf_node());
	    ceph_assert(inserted);
	    state.ret = iter;
	  });
	});
    }).si_then([c](auto &&state) {
      return new_mapping_iertr::make_ready_future<BackrefMappingRef>(
	state.ret->get_pin(c));
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
    auto &backref_entryrefs_by_seq = cache.get_backref_entryrefs_by_seq();
    return seastar::do_with(
      backref_entryrefs_by_seq.begin(),
      JOURNAL_SEQ_NULL,
      [this, &t, &limit, &backref_entryrefs_by_seq, max](auto &iter, auto &inserted_to) {
      return trans_intr::repeat(
        [&iter, this, &t, &limit, &backref_entryrefs_by_seq, max, &inserted_to]()
        -> merge_cached_backrefs_iertr::future<seastar::stop_iteration> {
        if (iter == backref_entryrefs_by_seq.end()) {
          return seastar::make_ready_future<seastar::stop_iteration>(
            seastar::stop_iteration::yes);
        }
        auto &seq = iter->first;
        auto &backref_entry_refs = iter->second;
        LOG_PREFIX(BtreeBackrefManager::merge_cached_backrefs);
        DEBUGT("seq {}, limit {}, num_fresh_backref {}"
          , t, seq, limit, t.get_num_fresh_backref());
        if (seq <= limit && t.get_num_fresh_backref() * BACKREF_NODE_SIZE < max) {
          inserted_to = seq;
          return trans_intr::do_for_each(
            backref_entry_refs,
            [this, &t](auto &backref_entry_ref) {
            LOG_PREFIX(BtreeBackrefManager::merge_cached_backrefs);
            auto &backref_entry = *backref_entry_ref;
            if (backref_entry.laddr != L_ADDR_NULL) {
              DEBUGT("new mapping: {}~{} -> {}",
                t,
                backref_entry.paddr,
                backref_entry.len,
                backref_entry.laddr);
              return new_mapping(
                t,
                backref_entry.paddr,
                backref_entry.len,
                backref_entry.laddr,
                backref_entry.type).si_then([](auto &&pin) {
                return seastar::now();
              });
            } else {
              DEBUGT("remove mapping: {}", t, backref_entry.paddr);
              return remove_mapping(
                t,
                backref_entry.paddr
              ).si_then([](auto&&) {
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
  DEBUGT("scan backref tree", t);
  auto c = get_context(t);
  return seastar::do_with(
    std::move(f),
    [this, c, FNAME](auto &scan_visitor)
  {
    auto block_size = cache.get_block_size();
    // traverse leaf-node entries
    return with_btree<BackrefBtree>(
      cache, c,
      [c, &scan_visitor, block_size, FNAME](auto &btree)
    {
      return BackrefBtree::iterate_repeat(
	c,
	btree.lower_bound(
	  c,
	  P_ADDR_MIN),
	[c, &scan_visitor, block_size, FNAME](auto &pos) {
	  if (pos.is_end()) {
	    return BackrefBtree::iterate_repeat_ret_inner(
	      interruptible::ready_future_marker{},
	      seastar::stop_iteration::yes);
	  }
	  TRACET("tree value {}~{} {}~{} {} used",
		 c.trans,
		 pos.get_key(),
		 pos.get_val().len,
		 pos.get_val().laddr,
		 pos.get_val().len,
		 pos.get_val().type);
	  ceph_assert(pos.get_key().is_absolute());
	  ceph_assert(pos.get_val().len > 0 &&
		      pos.get_val().len % block_size == 0);
	  ceph_assert(!is_backref_node(pos.get_val().type));
	  ceph_assert(pos.get_val().laddr != L_ADDR_NULL);
	  scan_visitor(
	      pos.get_key(),
	      P_ADDR_NULL,
	      pos.get_val().len,
	      pos.get_val().type,
	      pos.get_val().laddr);
	  return BackrefBtree::iterate_repeat_ret_inner(
	    interruptible::ready_future_marker{},
	    seastar::stop_iteration::no);
	}
      );
    }).si_then([this, &scan_visitor, c, FNAME, block_size] {
      // traverse alloc-deltas in order
      auto &backref_entryrefs = cache.get_backref_entryrefs_by_seq();
      for (auto &[seq, refs] : backref_entryrefs) {
	boost::ignore_unused(seq);
	DEBUGT("scan {} backref entries", c.trans, refs.size());
	for (auto &backref_entry : refs) {
	  if (backref_entry->laddr == L_ADDR_NULL) {
	    TRACET("backref entry {}~{} {} free",
		   c.trans,
		   backref_entry->paddr,
		   backref_entry->len,
		   backref_entry->type);
	  } else {
	    TRACET("backref entry {}~{} {}~{} {} used",
		   c.trans,
		   backref_entry->paddr,
		   backref_entry->len,
		   backref_entry->laddr,
		   backref_entry->len,
		   backref_entry->type);
	  }
	  ceph_assert(backref_entry->paddr.is_absolute());
	  ceph_assert(backref_entry->len > 0 &&
		      backref_entry->len % block_size == 0);
	  ceph_assert(!is_backref_node(backref_entry->type));
	  scan_visitor(
	    backref_entry->paddr,
	    P_ADDR_NULL,
	    backref_entry->len,
	    backref_entry->type,
	    backref_entry->laddr);
	}
      }
    }).si_then([this, &scan_visitor, block_size, c, FNAME] {
      BackrefBtree::mapped_space_visitor_t f =
	[&scan_visitor, block_size, FNAME, c](
	  paddr_t paddr, paddr_t key, extent_len_t len,
	  depth_t depth, extent_types_t type, BackrefBtree::iterator&) {
	TRACET("tree node {}~{} {}, depth={} used",
	       c.trans, paddr, len, type, depth);
	ceph_assert(paddr.is_absolute());
	ceph_assert(len > 0 && len % block_size == 0);
	ceph_assert(depth >= 1);
	ceph_assert(is_backref_node(type));
	return scan_visitor(paddr, key, len, type, L_ADDR_NULL);
      };
      return seastar::do_with(
	std::move(f),
	[this, c](auto &tree_visitor)
      {
	// traverse internal-node entries
	return with_btree<BackrefBtree>(
	  cache, c,
	  [c, &tree_visitor](auto &btree)
	{
	  return BackrefBtree::iterate_repeat(
	    c,
	    btree.lower_bound(
	      c,
	      P_ADDR_MIN,
	      &tree_visitor),
	    [](auto &pos) {
	      if (pos.is_end()) {
		return BackrefBtree::iterate_repeat_ret_inner(
		  interruptible::ready_future_marker{},
		  seastar::stop_iteration::yes);
	      }
	      return BackrefBtree::iterate_repeat_ret_inner(
		interruptible::ready_future_marker{},
		seastar::stop_iteration::no);
	    },
	    &tree_visitor
	  );
	});
      });
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
	  WARNT("paddr={} doesn't exist, state: {}, leaf {}",
	    c.trans, addr, iter.get_key(), *iter.get_leaf_node());
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

Cache::backref_entry_query_mset_t
BtreeBackrefManager::get_cached_backref_entries_in_range(
  paddr_t start,
  paddr_t end)
{
  return cache.get_backref_entries_in_range(start, end);
}

void BtreeBackrefManager::cache_new_backref_extent(
  paddr_t paddr,
  paddr_t key,
  extent_types_t type)
{
  return cache.add_backref_extent(paddr, key, type);
}

BtreeBackrefManager::retrieve_backref_extents_in_range_ret
BtreeBackrefManager::retrieve_backref_extents_in_range(
  Transaction &t,
  paddr_t start,
  paddr_t end)
{
  auto backref_extents = cache.get_backref_extents_in_range(start, end);
  return seastar::do_with(
      std::vector<CachedExtentRef>(),
      std::move(backref_extents),
      [this, &t](auto &extents, auto &backref_extents) {
    return trans_intr::parallel_for_each(
      backref_extents,
      [this, &extents, &t](auto &ent) {
      // only the gc fiber which is single can rewrite backref extents,
      // so it must be alive
      assert(is_backref_node(ent.type));
      LOG_PREFIX(BtreeBackrefManager::retrieve_backref_extents_in_range);
      DEBUGT("getting backref extent of type {} at {}, key {}",
	t,
	ent.type,
	ent.paddr,
	ent.key);

      auto c = get_context(t);
      return with_btree_ret<BackrefBtree, CachedExtentRef>(
	cache,
	c,
	[c, &ent](auto &btree) {
	if (ent.type == extent_types_t::BACKREF_INTERNAL) {
	  return btree.get_internal_if_live(
	    c, ent.paddr, ent.key, BACKREF_NODE_SIZE);
	} else {
	  assert(ent.type == extent_types_t::BACKREF_LEAF);
	  return btree.get_leaf_if_live(
	    c, ent.paddr, ent.key, BACKREF_NODE_SIZE);
	}
      }).si_then([&extents](auto ext) {
	ceph_assert(ext);
	extents.emplace_back(std::move(ext));
      });
    }).si_then([&extents] {
      return std::move(extents);
    });
  });
}

} // namespace crimson::os::seastore::backref
