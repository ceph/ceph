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
    return crimson::get_logger(ceph_subsys_seastore);
  }
}

namespace crimson::os::seastore::lba_manager::btree {

BtreeLBAManager::mkfs_ret BtreeLBAManager::mkfs(
  Transaction &t)
{
  logger().debug("BtreeLBAManager::mkfs");
  return with_trans_intr(
    t,
    [this](auto &t) {
      return cache.get_root(t).si_then([this, &t](auto croot) {
	auto root_leaf = cache.alloc_new_extent<LBALeafNode>(
	  t,
	  LBA_BLOCK_SIZE);
	root_leaf->set_size(0);
	lba_node_meta_t meta{0, L_ADDR_MAX, 1};
	root_leaf->set_meta(meta);
	root_leaf->pin.set_range(meta);
	croot->get_root().lba_root =
	  lba_root_t{root_leaf->get_paddr(), 1u};
	return mkfs_ertr::now();
      });
    }).handle_error(
      mkfs_ertr::pass_further{},
      crimson::ct_error::assert_all{
	"Invalid error in BtreeLBAManager::mkfs"
      }
    );
}

BtreeLBAManager::get_root_ret
BtreeLBAManager::get_root(Transaction &t)
{
  return cache.get_root(t).si_then([this, &t](auto croot) {
    logger().debug(
      "BtreeLBAManager::get_root: reading root at {} depth {}",
      paddr_t{croot->get_root().lba_root.get_location()},
      croot->get_root().lba_root.get_depth());
    return get_lba_btree_extent(
      get_context(t),
      croot,
      croot->get_root().lba_root.get_depth(),
      croot->get_root().lba_root.get_location(),
      paddr_t());
  });
}

BtreeLBAManager::get_mappings_ret
BtreeLBAManager::get_mappings(
  Transaction &t,
  laddr_t offset, extent_len_t length)
{
  logger().debug("BtreeLBAManager::get_mappings: {}, {}", offset, length);
  return get_root(
    t).si_then([this, &t, offset, length](auto extent) {
      return extent->lookup_range(
	get_context(t),
	offset, length
      ).si_then([](auto &&e) {
        logger().debug("BtreeLBAManager::get_mappings: got mappings {}", e);
        return std::move(e);
      });
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
  return trans_intr::do_for_each(
    l->begin(),
    l->end(),
    [this, &t, &ret](const auto &p) {
      return get_mappings(t, p.first, p.second).si_then(
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
  logger().debug("BtreeLBAManager::get_mapping: {}", offset);
  return get_root(t
  ).si_then([this, &t, offset] (auto extent) {
    return extent->lookup_pin(get_context(t), offset);
  }).si_then([] (auto &&e) {
    logger().debug("BtreeLBAManager::get_mapping: got mapping {}", *e);
    return std::move(e);
  });
}

BtreeLBAManager::find_hole_ret
BtreeLBAManager::find_hole(
  Transaction &t,
  laddr_t hint,
  extent_len_t len)
{
  return get_root(t
  ).si_then([this, hint, len, &t](auto extent) {
    return extent->find_hole(
      get_context(t),
      hint,
      L_ADDR_MAX,
      len);
  }).si_then([len](auto addr) {
    return std::make_pair(addr, len);
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
    t).si_then([this, &t, hint, len](auto extent) {
      logger().debug(
	"BtreeLBAManager::alloc_extent: beginning search at {}",
	*extent);
      return extent->find_hole(
	get_context(t),
	hint,
	L_ADDR_MAX,
	len).si_then([extent](auto ret) {
	  return std::make_pair(ret, extent);
	});
    }).si_then([this, &t, len, addr](auto allocation_pair) {
      auto &[laddr, extent] = allocation_pair;
      ceph_assert(laddr != L_ADDR_MAX);
      return insert_mapping(
	t,
	extent,
	laddr,
	{ len, addr, 1, 0 }
      ).si_then([laddr=laddr, addr, len](auto pin) {
	logger().debug(
	  "BtreeLBAManager::alloc_extent: alloc {}~{} for {}",
	  laddr,
	  len,
	  addr);
	return LBAPinRef(pin.release());
      });
    });
}

BtreeLBAManager::set_extent_ret
BtreeLBAManager::set_extent(
  Transaction &t,
  laddr_t off, extent_len_t len, paddr_t addr)
{
  return get_root(
    t).si_then([this, &t, off, len, addr](auto root) {
      return insert_mapping(
	t,
	root,
	off,
	{ len, addr, 1, 0 });
    }).si_then([](auto ret) {
      return set_extent_ret(
	interruptible::ready_future_marker{},
	LBAPinRef(ret.release()));
    });
}

static bool is_lba_node(extent_types_t type)
{
  return type == extent_types_t::LADDR_INTERNAL ||
    type == extent_types_t::LADDR_LEAF;
}

static bool is_lba_node(const CachedExtent &e)
{
  return is_lba_node(e.get_type());
}

btree_range_pin_t &BtreeLBAManager::get_pin(CachedExtent &e)
{
  if (is_lba_node(e)) {
    return e.cast<LBANode>()->pin;
  } else if (e.is_logical()) {
    return static_cast<BtreeLBAPin &>(
      e.cast<LogicalCachedExtent>()->get_pin()).pin;
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
  to_link.reserve(t.get_fresh_block_list().size());
  for (auto &e: t.get_fresh_block_list()) {
    if (e->is_valid() && (is_lba_node(*e) || e->is_logical()))
      to_link.push_back(e);
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
}

BtreeLBAManager::init_cached_extent_ret BtreeLBAManager::init_cached_extent(
  Transaction &t,
  CachedExtentRef e)
{
  logger().debug("{}: {}", __func__, *e);
  return get_root(t).si_then(
    [this, &t, e=std::move(e)](LBANodeRef root) mutable {
      if (is_lba_node(*e)) {
	auto lban = e->cast<LBANode>();
	logger().debug("init_cached_extent: lba node, getting root");
	return root->lookup(
	  op_context_t{cache, pin_set, t},
	  lban->get_node_meta().begin,
	  lban->get_node_meta().depth
	).si_then([this, e=std::move(e)](LBANodeRef c) {
	  if (c->get_paddr() == e->get_paddr()) {
	    assert(&*c == &*e);
	    logger().debug("init_cached_extent: {} initialized", *e);
	  } else {
	    // e is obsolete
	    logger().debug("init_cached_extent: {} obsolete", *e);
	    cache.drop_from_cache(e);
	  }
	  return init_cached_extent_iertr::now();
	});
      } else if (e->is_logical()) {
	auto logn = e->cast<LogicalCachedExtent>();
	return root->lookup_range(
	  op_context_t{cache, pin_set, t},
	  logn->get_laddr(),
	  logn->get_length()).si_then(
	    [this, logn=std::move(logn)](auto pins) {
	      if (pins.size() == 1) {
		auto pin = std::move(pins.front());
		pins.pop_front();
		if (pin->get_paddr() == logn->get_paddr()) {
		  logn->set_pin(std::move(pin));
		  pin_set.add_pin(
		    static_cast<BtreeLBAPin&>(logn->get_pin()).pin);
		  logger().debug("init_cached_extent: {} initialized", *logn);
		} else {
		  // paddr doesn't match, remapped, obsolete
		  logger().debug("init_cached_extent: {} obsolete", *logn);
		  cache.drop_from_cache(logn);
		}
	      } else {
		// set of extents changed, obsolete
		logger().debug("init_cached_extent: {} obsolete", *logn);
		cache.drop_from_cache(logn);
	      }
	      return init_cached_extent_iertr::now();
	    });
      } else {
	logger().debug("init_cached_extent: {} skipped", *e);
	return init_cached_extent_iertr::now();
      }
    });
}

BtreeLBAManager::scan_mappings_ret BtreeLBAManager::scan_mappings(
  Transaction &t,
  laddr_t begin,
  laddr_t end,
  scan_mappings_func_t &&f)
{
  return seastar::do_with(
    std::move(f),
    LBANodeRef(),
    [=, &t](auto &f, auto &lbarootref) {
      return get_root(t).si_then(
	[=, &t, &f](LBANodeRef lbaroot) mutable {
	  lbarootref = lbaroot;
	  return lbaroot->scan_mappings(
	    get_context(t),
	    begin,
	    end,
	    f);
	});
    });
}

BtreeLBAManager::scan_mapped_space_ret BtreeLBAManager::scan_mapped_space(
    Transaction &t,
    scan_mapped_space_func_t &&f)
{
  return seastar::do_with(
    std::move(f),
    LBANodeRef(),
    [=, &t](auto &f, auto &lbarootref) {
      return get_root(t).si_then(
	[=, &t, &f](LBANodeRef lbaroot) mutable {
	  lbarootref = lbaroot;
	  return lbaroot->scan_mapped_space(
	    get_context(t),
	    f);
	});
    });
}

BtreeLBAManager::rewrite_extent_ret BtreeLBAManager::rewrite_extent(
  Transaction &t,
  CachedExtentRef extent)
{
  assert(!extent->has_been_invalidated());

  logger().debug(
    "{}: rewriting {}", 
    __func__,
    *extent);

  if (extent->is_logical()) {
    auto lextent = extent->cast<LogicalCachedExtent>();
    cache.retire_extent(t, extent);
    auto nlextent = cache.alloc_new_extent_by_type(
      t,
      lextent->get_type(),
      lextent->get_length())->cast<LogicalCachedExtent>();
    lextent->get_bptr().copy_out(
      0,
      lextent->get_length(),
      nlextent->get_bptr().c_str());
    nlextent->set_laddr(lextent->get_laddr());
    nlextent->set_pin(lextent->get_pin().duplicate());

    logger().debug(
      "{}: rewriting {} into {}",
      __func__,
      *lextent,
      *nlextent);

    return update_mapping(
      t,
      lextent->get_laddr(),
      [prev_addr = lextent->get_paddr(), addr = nlextent->get_paddr()](
	const lba_map_val_t &in) {
	lba_map_val_t ret = in;
	ceph_assert(in.paddr == prev_addr);
	ret.paddr = addr;
	return ret;
      }).si_then(
	[nlextent](auto) {},
	rewrite_extent_iertr::pass_further{},
        /* ENOENT in particular should be impossible */
	crimson::ct_error::assert_all{
	  "Invalid error in BtreeLBAManager::rewrite_extent after update_mapping"
	}
      );
  } else if (is_lba_node(*extent)) {
    auto lba_extent = extent->cast<LBANode>();
    cache.retire_extent(t, extent);
    auto nlba_extent = cache.alloc_new_extent_by_type(
      t,
      lba_extent->get_type(),
      lba_extent->get_length())->cast<LBANode>();
    lba_extent->get_bptr().copy_out(
      0,
      lba_extent->get_length(),
      nlba_extent->get_bptr().c_str());
    nlba_extent->pin.set_range(nlba_extent->get_node_meta());

    /* This is a bit underhanded.  Any relative addrs here must necessarily
     * be record relative as we are rewriting a dirty extent.  Thus, we
     * are using resolve_relative_addrs with a (likely negative) block
     * relative offset to correct them to block-relative offsets adjusted
     * for our new transaction location.
     *
     * Upon commit, these now block relative addresses will be interpretted
     * against the real final address.
     */
    nlba_extent->resolve_relative_addrs(
      make_record_relative_paddr(0) - nlba_extent->get_paddr());

    logger().debug(
      "{}: rewriting {} into {}",
      __func__,
      *lba_extent,
      *nlba_extent);

    return update_internal_mapping(
      t,
      nlba_extent->get_node_meta().depth,
      nlba_extent->get_node_meta().begin,
      nlba_extent->get_paddr()).si_then(
	[](auto) {},
	rewrite_extent_iertr::pass_further{},
	crimson::ct_error::assert_all{
	  "Invalid error in BtreeLBAManager::rewrite_extent update_internal_mapping"
	});
  } else {
    return rewrite_extent_iertr::now();
  }
}

BtreeLBAManager::get_physical_extent_if_live_ret
BtreeLBAManager::get_physical_extent_if_live(
  Transaction &t,
  extent_types_t type,
  paddr_t addr,
  laddr_t laddr,
  segment_off_t len)
{
  ceph_assert(is_lba_node(type));
  return cache.get_extent_by_type(
    t,
    type,
    addr,
    laddr,
    len
  ).si_then([=, &t](CachedExtentRef extent) {
    return get_root(t).si_then([=, &t](LBANodeRef root) {
      auto lba_node = extent->cast<LBANode>();
      return root->lookup(
	op_context_t{cache, pin_set, t},
	lba_node->get_node_meta().begin,
	lba_node->get_node_meta().depth).si_then([=](LBANodeRef c) {
	  if (c->get_paddr() == lba_node->get_paddr()) {
	    return get_physical_extent_if_live_ret(
	      interruptible::ready_future_marker{},
	      lba_node);
	  } else {
	    cache.drop_from_cache(lba_node);
	    return get_physical_extent_if_live_ret(
	      interruptible::ready_future_marker{},
	      CachedExtentRef());
	  }
	});
    });
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
  auto split = insert_mapping_iertr::future<LBANodeRef>(
    interruptible::ready_future_marker{},
    root);
  if (root->at_max_capacity()) {
    split = cache.get_root(t).si_then(
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
	croot->get_root().lba_root = lba_root_t{
	  nroot->get_paddr(),
	  root->get_node_meta().depth + 1
	};
	return nroot->split_entry(
	  get_context(t),
	  laddr, nroot->begin(), root);
      });
  }
  return split.si_then([this, &t, laddr, val](LBANodeRef node) {
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
      return out;
    }).si_then([](auto result) {
      return ref_update_result_t{
	result.refcount,
	result.paddr,
	result.len
       };
    });
}

BtreeLBAManager::update_mapping_ret BtreeLBAManager::update_mapping(
  Transaction &t,
  laddr_t addr,
  update_func_t &&f)
{
  return get_root(t
  ).si_then([this, f=std::move(f), &t, addr](LBANodeRef root) mutable {
    return root->mutate_mapping(
      get_context(t),
      addr,
      std::move(f));
  });
}

BtreeLBAManager::update_internal_mapping_ret
BtreeLBAManager::update_internal_mapping(
  Transaction &t,
  depth_t depth,
  laddr_t laddr,
  paddr_t paddr)
{
  return cache.get_root(t).si_then([=, &t](RootBlockRef croot) {
    if (depth == croot->get_root().lba_root.get_depth()) {
      logger().debug(
	"update_internal_mapping: updating lba root to: {}->{}",
	laddr,
	paddr);
      {
	auto mut_croot = cache.duplicate_for_write(t, croot);
	croot = mut_croot->cast<RootBlock>();
      }
      ceph_assert(laddr == 0);
      auto old_paddr = croot->get_root().lba_root.get_location();
      croot->get_root().lba_root.set_location(paddr);
      return update_internal_mapping_ret(
	interruptible::ready_future_marker{},
	old_paddr);
    } else {
      logger().debug(
	"update_internal_mapping: updating lba node at depth {} to: {}->{}",
	depth,
	laddr,
	paddr);
      return get_lba_btree_extent(
	get_context(t),
	croot,
	croot->get_root().lba_root.get_depth(),
	croot->get_root().lba_root.get_location(),
	paddr_t()).si_then([=, &t](LBANodeRef broot) {
	  return broot->mutate_internal_address(
	    get_context(t),
	    depth,
	    laddr,
	    paddr);
	});
    }
  });
}

BtreeLBAManager::~BtreeLBAManager()
{
  pin_set.scan([](auto &i) {
    logger().error("Found {} {} has_ref={}", i, i.get_extent(), i.has_ref());
  });
}

}
