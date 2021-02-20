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
  TransactionManager &tm)
  : tm(tm) {}

BtreeExtentMapManager::initialize_extmap_ret
BtreeExtentMapManager::initialize_extmap(Transaction &t)
{

  logger().debug("{}", __func__);
  return tm.alloc_extent<ExtMapLeafNode>(t, L_ADDR_MIN, EXTMAP_BLOCK_SIZE)
    .safe_then([](auto&& root_extent) {
      root_extent->set_size(0);
      extmap_node_meta_t meta{1};
      root_extent->set_meta(meta);
      extmap_root_t extmap_root = extmap_root_t(1, root_extent->get_laddr());
      return initialize_extmap_ertr::make_ready_future<extmap_root_t>(extmap_root);
  });
}

BtreeExtentMapManager::get_root_ret
BtreeExtentMapManager::get_extmap_root(const extmap_root_t &extmap_root, Transaction &t)
{
  assert(extmap_root.extmap_root_laddr != L_ADDR_NULL);
  laddr_t laddr = extmap_root.extmap_root_laddr;
  return extmap_load_extent(get_ext_context(t), laddr, extmap_root.depth);
}

BtreeExtentMapManager::find_lextent_ret
BtreeExtentMapManager::find_lextent(const extmap_root_t &extmap_root, Transaction &t,
	                            objaddr_t lo, extent_len_t len)
{
  logger().debug("{}: {}, {}", __func__, lo, len);
  return get_extmap_root(extmap_root, t).safe_then([this, &t, lo, len](auto&& extent) {
    return extent->find_lextent(get_ext_context(t), lo, len);
  }).safe_then([](auto &&e) {
    logger().debug("{}: found_lextent {}", __func__, e);
    return find_lextent_ret(
           find_lextent_ertr::ready_future_marker{},
	          std::move(e));
  });

}

BtreeExtentMapManager::add_lextent_ret
BtreeExtentMapManager::add_lextent(extmap_root_t &extmap_root, Transaction &t,
                                   objaddr_t lo, lext_map_val_t val)
{
  logger().debug("{}: {}, {}, {}", __func__, lo, val.laddr, val.length);
  return get_extmap_root(extmap_root, t).safe_then([this, &extmap_root, &t, lo, val](auto &&root) {
    return insert_lextent(extmap_root, t, root, lo, val);
  }).safe_then([](auto ret) {
      logger().debug("{}:  {}", __func__, ret);
      return add_lextent_ret(
             add_lextent_ertr::ready_future_marker{},
             std::move(ret));
  });

}

BtreeExtentMapManager::insert_lextent_ret
BtreeExtentMapManager::insert_lextent(extmap_root_t &extmap_root, Transaction &t,
	               ExtMapNodeRef root, objaddr_t logical_offset, lext_map_val_t val)
{
  auto split = insert_lextent_ertr::make_ready_future<ExtMapNodeRef>(root);
  if (root->at_max_capacity()) {
    logger().debug("{}::splitting root {}", __func__, *root);
    split =  root->extmap_alloc_extent<ExtMapInnerNode>(get_ext_context(t), EXTMAP_BLOCK_SIZE)
      .safe_then([this, &extmap_root, root, &t, logical_offset](auto&& nroot) {
        extmap_node_meta_t meta{root->get_node_meta().depth + 1};
        nroot->set_meta(meta);
        nroot->journal_insert(nroot->begin(), OBJ_ADDR_MIN,
        root->get_laddr(), nullptr);
        extmap_root.extmap_root_laddr = nroot->get_laddr();
        extmap_root.depth = root->get_node_meta().depth + 1;
        extmap_root.state = extmap_root_state_t::MUTATED;
        return nroot->split_entry(get_ext_context(t), logical_offset, nroot->begin(), root);
      });
  }
  return split.safe_then([this, &t, logical_offset, val](ExtMapNodeRef node) {
    return node->insert(get_ext_context(t), logical_offset, val);
  });
}

BtreeExtentMapManager::rm_lextent_ret
BtreeExtentMapManager::rm_lextent(extmap_root_t &extmap_root, Transaction &t, objaddr_t lo, lext_map_val_t val)
{
  logger().debug("{}: {}, {}, {}", __func__, lo, val.laddr, val.length);
  return get_extmap_root(extmap_root, t).safe_then([this, &t, lo, val](auto extent) {
    return extent->rm_lextent(get_ext_context(t), lo, val);
  }).safe_then([](auto removed) {
    logger().debug("{}: {}", __func__, removed);
    return rm_lextent_ret(
           rm_lextent_ertr::ready_future_marker{},
           removed);
  });
}


}
