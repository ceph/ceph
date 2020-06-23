// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <sys/mman.h>
#include <string.h>

#include "crimson/common/log.h"

#include "include/buffer.h"
#include "crimson/os/seastore/extentmap_manager/btree/btree_extentmap_manager.h"
#include "crimson/os/seastore/extentmap_manager/btree/extentmap_btree_node_impl.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::extentmap_manager {

BtreeExtentMapManager::BtreeExtentMapManager(
  TransactionManager *tm)
  : tm(tm) {}

BtreeExtentMapManager::BtreeExtentMapManager(
  TransactionManager *tm,  extmap_root_ref extmap_root)
  : tm(tm),
    extmap_root(extmap_root) {}


BtreeExtentMapManager::alloc_extmap_root_ret
BtreeExtentMapManager::alloc_extmap_root(Transaction &t)
{

  logger().debug("BtreeExtentMapManager::alloc_extmap_root");
  if (extmap_root)
    return alloc_extmap_root_ertr::now();

  return tm->alloc_extent<ExtMapLeafNode>(t, L_ADDR_MIN, EXTMAP_BLOCK_SIZE)
    .safe_then([this](auto&& root_extent) {
      root_extent->set_tm(tm);
      root_extent->set_size(0);
      root_extent->set_depth(0);
      extmap_root = std::make_shared<extmap_root_t>(0, root_extent->get_laddr());
      return alloc_extmap_root_ertr::now();
  });
}

BtreeExtentMapManager::get_root_ret
BtreeExtentMapManager::get_extmap_root(Transaction &t)
{
  assert(extmap_root != NULL);
  laddr_t laddr = extmap_root->extmap_root_laddr;
  return extmap_load_extent(tm, t, laddr, extmap_root->depth);
}

BtreeExtentMapManager::seek_lextent_ret
BtreeExtentMapManager::seek_lextent(Transaction &t, uint32_t lo,
		                    uint32_t len)
{
  logger().debug("seek_lextent: {}, {}", lo, len);
  return get_extmap_root(t).safe_then([this, &t, lo, len](auto&& extent) {
    return extent->seek_lextent(t, lo, len);
  }).safe_then([](auto &&e) {
     // logger().debug("seek_lextent: found_lextent {}", e);
      return seek_lextent_ret(
        seek_lextent_ertr::ready_future_marker{},
	std::move(e));
  });

}

BtreeExtentMapManager::add_lextent_ret
BtreeExtentMapManager::add_lextent(Transaction &t, uint32_t lo,
                                   lext_map_val_t val)
{
  logger().debug("add_lextent: {}, {}, {}, {}", lo, val.laddr, val.lextent_offset, val.length);
  return get_extmap_root(t).safe_then([this, &t, lo, val](auto &&root) {
    return insert_lextent(t, root, lo, val);
  }).safe_then([](auto ret) {
  //    logger().debug("add_lextent:  {}", ret);
      return add_lextent_ret(
        add_lextent_ertr::ready_future_marker{},
        std::move(ret));
  });

}

BtreeExtentMapManager::insert_lextent_ret
BtreeExtentMapManager::insert_lextent(Transaction &t, ExtMapNodeRef root,
                    uint32_t logical_offset, lext_map_val_t val)
{
  auto split = insert_lextent_ertr::make_ready_future<ExtMapNodeRef>(root);
  if (root->at_max_capacity()) {
      logger().debug("BtreeExtentMapManager::splitting root {}", *root);
    split =  root->extmap_alloc_extent<ExtMapInnerNode>(t, EXTMAP_BLOCK_SIZE)
        .safe_then([this, root, &t, logical_offset](auto&& nroot) {
        nroot->set_depth(root->depth + 1);
	nroot->journal_insert(nroot->begin(), OBJ_ADDR_MIN,
			      root->get_laddr(), nullptr);
        extmap_root->extmap_root_laddr = nroot->get_laddr();
        extmap_root->depth = root->depth + 1;
        return nroot->split_entry(t, logical_offset, nroot->begin(), root);
      });
  }
  return split.safe_then([this, &t, logical_offset, val](ExtMapNodeRef node) {
    return node->insert(t, logical_offset, val);
  });
}

BtreeExtentMapManager::has_any_ret
BtreeExtentMapManager::has_any_lextents(Transaction &t, uint32_t lo, uint32_t len)
{
  return seek_lextent(t, lo, len).safe_then([this, lo, len](auto&& extents) {
    if (extents.empty())
      return has_any_ertr::make_ready_future<bool>(false);

    auto &ext = *extents.begin();
    if (ext->logical_offset >= lo + len)
      return has_any_ertr::make_ready_future<bool>(false);

    return has_any_ertr::make_ready_future<bool>(true);

  });
}

BtreeExtentMapManager::rm_lextent_ret
BtreeExtentMapManager::rm_lextent(Transaction &t, uint32_t lo, lext_map_val_t val)
{

  logger().debug("rm_lextent: {}, {}, {}, {}", lo, val.laddr, val.lextent_offset, val.length);
  return get_extmap_root(t).safe_then([this, &t, lo, val](auto extent) {
    return extent->rm_lextent(t, lo, val);
  }).safe_then([](auto ret) {
    logger().debug("rm_lextent: {}", ret);
    return rm_lextent_ret(
      rm_lextent_ertr::ready_future_marker{},
      ret);
  });
}

BtreeExtentMapManager::punch_lextent_ret
BtreeExtentMapManager::punch_lextent(Transaction &t, uint32_t lo, uint32_t len)
{
  logger().debug("punch_lextent: {}, {}", lo, len);
  return get_extmap_root(t).safe_then([this, &t, lo, len](auto extent) {
    return extent->punch_lextent(t, lo, len);
  }).safe_then([](auto &&ret) {
  //  logger().debug("punch_lextent: {}", ret);
    return punch_lextent_ret(
      punch_lextent_ertr::ready_future_marker{},
      std::move(ret));
  });

}

BtreeExtentMapManager::find_hole_ret
BtreeExtentMapManager::find_hole(Transaction &t, uint32_t lo, uint32_t len)
{
  logger().debug("find_lextent: {}, {}", lo, len);
  return get_extmap_root(t).safe_then([this, &t, lo, len](auto extent) {
    return extent->find_hole(t, lo, len);
  }).safe_then([](auto &&ret) {
  //  logger().debug("find_lextent: {}", ret);
    return find_hole_ret(
      find_hole_ertr::ready_future_marker{},
      std::move(ret));
  });

}

BtreeExtentMapManager::set_lextent_ret
BtreeExtentMapManager::set_lextent(Transaction &t, uint32_t lo, lext_map_val_t val)
{
  return punch_lextent(t, lo, val.length).safe_then([this, &t, lo, val](auto &&extent) {
    return add_lextent(t, lo, val);
  });

}

}
