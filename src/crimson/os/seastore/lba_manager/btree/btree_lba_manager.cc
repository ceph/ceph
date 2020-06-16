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
  return cache.get_root(t).safe_then([this, &t](auto root) {
    auto root_leaf = cache.alloc_new_extent<LBALeafNode>(
      t,
      LBA_BLOCK_SIZE);
    root_leaf->set_size(0);
    root->get_lba_root() =
      btree_lba_root_t{
        0,
        0,
        root_leaf->get_paddr() - root->get_paddr(),
        paddr_t{}};
    return mkfs_ertr::now();
  });
}

BtreeLBAManager::get_root_ret
BtreeLBAManager::get_root(Transaction &t)
{
  return cache.get_root(t).safe_then([this, &t](auto root) {
    paddr_t root_addr = root->get_lba_root().lba_root_addr;
    root_addr = root_addr.maybe_relative_to(root->get_paddr());
    logger().debug(
      "BtreeLBAManager::get_root: reading root at {} depth {}",
      root->get_lba_root().lba_root_addr,
      root->get_lba_root().lba_depth);
    return get_lba_btree_extent(
      cache,
      t,
      root->get_lba_root().lba_depth,
      root->get_lba_root().lba_root_addr,
      root->get_paddr());
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
	cache, t, offset, length);
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
	cache,
	t,
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

BtreeLBAManager::submit_lba_transaction_ret
BtreeLBAManager::submit_lba_transaction(
  Transaction &t)
{
  // This is a noop for now and may end up not being necessary
  return submit_lba_transaction_ertr::now();
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
	nroot->set_depth(root->depth + 1);
	nroot->journal_insert(
	  nroot->begin(),
	  L_ADDR_MIN,
	  root->get_paddr(),
	  nullptr);
	croot->get_lba_root().lba_root_addr = nroot->get_paddr();
	croot->get_lba_root().lba_depth = root->depth + 1;
	return nroot->split_entry(cache, t, laddr, nroot->begin(), root);
      });
  }
  return split.safe_then([this, &t, laddr, val](LBANodeRef node) {
    return node->insert(cache, t, laddr, val);
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
      cache,
      t,
      addr,
      std::move(f));
  });
}

}
