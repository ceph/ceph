// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include "crimson/common/log.h"

#include "include/buffer.h"
#include "crimson/os/seastore/lba_manager/btree/btree_lba_manager.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node_impl.h"


namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::lba_manager::btree {

BtreeLBAManager::mkfs_ret BtreeLBAManager::mkfs(
  Transaction &t)
{
  logger().debug("BtreeLBAManager::mkfs");
  return cache.get_root(t).safe_then([this, &t](auto croot) {
    auto root_leaf = cache.alloc_new_extent<LBALeafNode>(
      t,
      LBA_BLOCK_SIZE);
    root_leaf->set_size(0);
    lba_node_meta_t meta{0, L_ADDR_MAX, 1};
    root_leaf->set_meta(meta);
    root_leaf->pin.set_range(meta);
    croot->get_lba_root() =
      root_t{
        1,
        0,
        root_leaf->get_paddr(),
        make_record_relative_paddr(0)};
    return mkfs_ertr::now();
  });
}

BtreeLBAManager::get_root_ret
BtreeLBAManager::get_root(Transaction &t)
{
  return cache.get_root(t).safe_then([this, &t](auto croot) {
    logger().debug(
      "BtreeLBAManager::get_root: reading root at {} depth {}",
      paddr_t{croot->get_lba_root().lba_root_addr},
      unsigned(croot->get_lba_root().lba_depth));
    return get_lba_btree_extent(
      get_context(t),
      croot->get_lba_root().lba_depth,
      croot->get_lba_root().lba_root_addr,
      paddr_t());
  });
}

BtreeLBAManager::get_mapping_ret
BtreeLBAManager::get_mapping(
  Transaction &t,
  laddr_t offset, extent_len_t length)
{
  logger().debug("BtreeLBAManager::get_mapping: {}, {}", offset, length);
  return get_root(
    t).safe_then([this, &t, offset, length](auto extent) {
      return extent->lookup_range(
	get_context(t),
	offset, length);
    }).safe_then([](auto &&e) {
      logger().debug("BtreeLBAManager::get_mapping: got mapping {}", e);
      return get_mapping_ret(
	get_mapping_ertr::ready_future_marker{},
	std::move(e));
    });
}


BtreeLBAManager::get_mappings_ret
BtreeLBAManager::get_mappings(
  Transaction &t,
  laddr_list_t &&list)
{
  logger().debug("BtreeLBAManager::get_mappings: {}", list);
  auto l = std::make_unique<laddr_list_t>(std::move(list));
  auto retptr = std::make_unique<lba_pin_list_t>();
  auto &ret = *retptr;
  return crimson::do_for_each(
    l->begin(),
    l->end(),
    [this, &t, &ret](const auto &p) {
      return get_mapping(t, p.first, p.second).safe_then(
	[&ret](auto res) {
	  ret.splice(ret.end(), res, res.begin(), res.end());
	});
    }).safe_then([l=std::move(l), retptr=std::move(retptr)]() mutable {
      return std::move(*retptr);
    });
}

BtreeLBAManager::alloc_extent_ret
BtreeLBAManager::alloc_extent(
  Transaction &t,
  laddr_t hint,
  extent_len_t len,
  paddr_t addr)
{
  // TODO: we can certainly combine the lookup and the insert.
  return get_root(
    t).safe_then([this, &t, hint, len](auto extent) {
      logger().debug(
	"BtreeLBAManager::alloc_extent: beginning search at {}",
	*extent);
      return extent->find_hole(
	get_context(t),
	hint,
	L_ADDR_MAX,
	len).safe_then([extent](auto ret) {
	  return std::make_pair(ret, extent);
	});
    }).safe_then([this, &t, len, addr](auto allocation_pair) {
      auto &[laddr, extent] = allocation_pair;
      ceph_assert(laddr != L_ADDR_MAX);
      return insert_mapping(
	t,
	extent,
	laddr,
	{ len, addr, 1, 0 }
      ).safe_then([laddr=laddr, addr, len](auto pin) {
	logger().debug(
	  "BtreeLBAManager::alloc_extent: alloc {}~{} for {}",
	  laddr,
	  len,
	  addr);
	return alloc_extent_ret(
	  alloc_extent_ertr::ready_future_marker{},
	  LBAPinRef(pin.release()));
      });
    });
}

BtreeLBAManager::set_extent_ret
BtreeLBAManager::set_extent(
  Transaction &t,
  laddr_t off, extent_len_t len, paddr_t addr)
{
  return get_root(
    t).safe_then([this, &t, off, len, addr](auto root) {
      return insert_mapping(
	t,
	root,
	off,
	{ len, addr, 1, 0 });
    }).safe_then([](auto ret) {
      return set_extent_ret(
	set_extent_ertr::ready_future_marker{},
	LBAPinRef(ret.release()));
    });
}

static bool is_lba_node(const CachedExtent &e)
{
  return e.get_type() == extent_types_t::LADDR_INTERNAL ||
    e.get_type() == extent_types_t::LADDR_LEAF;
}

btree_range_pin_t &BtreeLBAManager::get_pin(CachedExtent &e)
{
  if (is_lba_node(e)) {
    return e.cast<LBANode>()->pin;
  } else if (e.is_logical()) {
    return static_cast<BtreeLBAPin &>(
      e.cast<LogicalCachedExtent>()->get_pin()).pin;
  } else {
    assert(0 == "impossible");
    return *static_cast<btree_range_pin_t*>(nullptr);
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

BtreeLBAManager::complete_transaction_ret
BtreeLBAManager::complete_transaction(
  Transaction &t)
{
  std::vector<CachedExtentRef> to_clear;
  to_clear.reserve(t.get_retired_set().size());
  for (auto &e: t.get_retired_set()) {
    if (e->is_logical() || is_lba_node(*e))
      to_clear.push_back(e);
  }
  // need to call check_parent from leaf->parent
  std::sort(
    to_clear.begin(), to_clear.end(),
    [](auto &l, auto &r) { return get_depth(*l) < get_depth(*r); });

  for (auto &e: to_clear) {
    auto &pin = get_pin(*e);
    logger().debug("{}: retiring {}, {}", __func__, *e, pin);
    pin_set.retire(pin);
  }

  // ...but add_pin from parent->leaf
  std::vector<CachedExtentRef> to_link;
  to_link.reserve(
    t.get_fresh_block_list().size() +
    t.get_mutated_block_list().size());
  for (auto &l: {t.get_fresh_block_list(), t.get_mutated_block_list()}) {
    for (auto &e: l) {
      if (e->is_valid() && (is_lba_node(*e) || e->is_logical()))
	to_link.push_back(e);
    }
  }
  std::sort(
    to_link.begin(), to_link.end(),
    [](auto &l, auto &r) -> bool { return get_depth(*l) > get_depth(*r); });

  for (auto &e : to_link) {
    logger().debug("{}: linking {}", __func__, *e);
    pin_set.add_pin(get_pin(*e));
  }

  for (auto &e: to_clear) {
    auto &pin = get_pin(*e);
    logger().debug("{}: checking {}, {}", __func__, *e, pin);
    pin_set.check_parent(pin);
  }
  return complete_transaction_ertr::now();
}

BtreeLBAManager::init_cached_extent_ret BtreeLBAManager::init_cached_extent(
  Transaction &t,
  CachedExtentRef e)
{
  logger().debug("{}: {}", __func__, *e);
  return get_root(t).safe_then(
    [this, &t, e=std::move(e)](LBANodeRef root) mutable {
      if (is_lba_node(*e)) {
	auto lban = e->cast<LBANode>();
	logger().debug("init_cached_extent: lba node, getting root");
	return root->lookup(
	  op_context_t{cache, pin_set, t},
	  lban->get_node_meta().begin,
	  lban->get_node_meta().depth
	).safe_then([this, &t, e=std::move(e)](LBANodeRef c) {
	  if (c->get_paddr() == e->get_paddr()) {
	    assert(&*c == &*e);
	    logger().debug("init_cached_extent: {} initialized", *e);
	  } else {
	    // e is obsolete
	    logger().debug("init_cached_extent: {} obsolete", *e);
	    cache.retire_extent(t, e);
	  }
	  return init_cached_extent_ertr::now();
	});
      } else if (e->is_logical()) {
	auto logn = e->cast<LogicalCachedExtent>();
	return root->lookup_range(
	  op_context_t{cache, pin_set, t},
	  logn->get_laddr(),
	  logn->get_length()).safe_then(
	    [this, &t, logn=std::move(logn)](auto pins) {
	      if (pins.size() == 1) {
		auto pin = std::move(pins.front());
		pins.pop_front();
		if (pin->get_paddr() == logn->get_paddr()) {
		  logn->set_pin(std::move(pin));
		  logger().debug("init_cached_extent: {} initialized", *logn);
		} else {
		  // paddr doesn't match, remapped, obsolete
		  logger().debug("init_cached_extent: {} obsolete", *logn);
		  cache.retire_extent(t, logn);
		}
	      } else {
		// set of extents changed, obsolete
		logger().debug("init_cached_extent: {} obsolete", *logn);
		cache.retire_extent(t, logn);
	      }
	      return init_cached_extent_ertr::now();
	    });
      } else {
	logger().debug("init_cached_extent: {} skipped", *e);
	return init_cached_extent_ertr::now();
      }
    });
}

BtreeLBAManager::BtreeLBAManager(
  SegmentManager &segment_manager,
  Cache &cache)
  : segment_manager(segment_manager),
    cache(cache) {}

BtreeLBAManager::insert_mapping_ret BtreeLBAManager::insert_mapping(
  Transaction &t,
  LBANodeRef root,
  laddr_t laddr,
  lba_map_val_t val)
{
  auto split = insert_mapping_ertr::future<LBANodeRef>(
    insert_mapping_ertr::ready_future_marker{},
    root);
  if (root->at_max_capacity()) {
    split = cache.get_root(t).safe_then(
      [this, root, laddr, &t](RootBlockRef croot) {
	logger().debug(
	  "BtreeLBAManager::insert_mapping: splitting root {}",
	  *croot);
	{
	  auto mut_croot = cache.duplicate_for_write(t, croot);
	  croot = mut_croot->cast<RootBlock>();
	}
	auto nroot = cache.alloc_new_extent<LBAInternalNode>(t, LBA_BLOCK_SIZE);
	lba_node_meta_t meta{0, L_ADDR_MAX, root->get_node_meta().depth + 1};
	nroot->set_meta(meta);
	nroot->pin.set_range(meta);
	nroot->journal_insert(
	  nroot->begin(),
	  L_ADDR_MIN,
	  root->get_paddr(),
	  nullptr);
	croot->get_lba_root().lba_root_addr = nroot->get_paddr();
	croot->get_lba_root().lba_depth = root->get_node_meta().depth + 1;
	return nroot->split_entry(
	  get_context(t),
	  laddr, nroot->begin(), root);
      });
  }
  return split.safe_then([this, &t, laddr, val](LBANodeRef node) {
    return node->insert(
      get_context(t),
      laddr, val);
  });
}

BtreeLBAManager::update_refcount_ret BtreeLBAManager::update_refcount(
  Transaction &t,
  laddr_t addr,
  int delta)
{
  return update_mapping(
    t,
    addr,
    [delta](const lba_map_val_t &in) {
      lba_map_val_t out = in;
      ceph_assert((int)out.refcount + delta >= 0);
      out.refcount += delta;
      if (out.refcount == 0) {
	return std::optional<lba_map_val_t>();
      } else {
	return std::optional<lba_map_val_t>(out);
      }
    }).safe_then([](auto result) {
      if (!result)
	return 0u;
      else
	return result->refcount;
    });
}

BtreeLBAManager::update_mapping_ret BtreeLBAManager::update_mapping(
  Transaction &t,
  laddr_t addr,
  update_func_t &&f)
{
  return get_root(t
  ).safe_then([this, f=std::move(f), &t, addr](LBANodeRef root) mutable {
    return root->mutate_mapping(
      get_context(t),
      addr,
      std::move(f));
  });
}

}
