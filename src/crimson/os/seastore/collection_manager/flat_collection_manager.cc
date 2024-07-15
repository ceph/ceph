// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include "crimson/common/log.h"

#include "include/buffer.h"
#include "include/stringify.h"
#include "crimson/os/seastore/collection_manager/flat_collection_manager.h"
#include "crimson/os/seastore/collection_manager/collection_flat_node.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore);
  }
}

namespace crimson::os::seastore::collection_manager {

constexpr static extent_len_t MIN_FLAT_BLOCK_SIZE = 4<<10;
[[maybe_unused]] constexpr static extent_len_t MAX_FLAT_BLOCK_SIZE = 4<<20;

FlatCollectionManager::FlatCollectionManager(
  TransactionManager &tm)
  : tm(tm) {}

FlatCollectionManager::mkfs_ret
FlatCollectionManager::mkfs(Transaction &t)
{

  logger().debug("FlatCollectionManager: {}", __func__);
  return tm.alloc_non_data_extent<CollectionNode>(
    t, L_ADDR_MIN, MIN_FLAT_BLOCK_SIZE
  ).si_then([](auto&& root_extent) {
    coll_root_t coll_root = coll_root_t(
      root_extent->get_laddr(),
      MIN_FLAT_BLOCK_SIZE
    );
    return mkfs_iertr::make_ready_future<coll_root_t>(coll_root);
  });
}

FlatCollectionManager::get_root_ret
FlatCollectionManager::get_coll_root(const coll_root_t &coll_root, Transaction &t)
{
  logger().debug("FlatCollectionManager: {}", __func__);
  assert(coll_root.get_location() != L_ADDR_NULL);
  auto cc = get_coll_context(t);
  return cc.tm.read_extent<CollectionNode>(
    cc.t,
    coll_root.get_location(),
    coll_root.get_size()
  ).si_then([](auto&& e) {
    return get_root_iertr::make_ready_future<CollectionNodeRef>(std::move(e));
  });
}

FlatCollectionManager::create_ret
FlatCollectionManager::create(coll_root_t &coll_root, Transaction &t,
                              coll_t cid, coll_info_t info)
{
  logger().debug("FlatCollectionManager: {}", __func__);
  return get_coll_root(coll_root, t
  ).si_then([=, this, &coll_root, &t] (auto &&extent) {
    return extent->create(
      get_coll_context(t), cid, info.split_bits
    ).si_then([=, this, &coll_root, &t] (auto ret) {
      switch (ret) {
      case CollectionNode::create_result_t::OVERFLOW: {
        logger().debug("FlatCollectionManager: {} overflow!", __func__);
	auto new_size = coll_root.get_size() * 2; // double each time

	// TODO return error probably, but such a nonsensically large number of
	// collections would create a ton of other problems as well
	assert(new_size < MAX_FLAT_BLOCK_SIZE);
        return tm.alloc_non_data_extent<CollectionNode>(
	  t, L_ADDR_MIN, new_size
	).si_then([=, this, &coll_root, &t] (auto &&root_extent) {
          coll_root.update(root_extent->get_laddr(), root_extent->get_length());

	  root_extent->decoded = extent->decoded;
	  return root_extent->create(
	    get_coll_context(t), cid, info.split_bits
	  ).si_then([=, this, &t](auto result) {
	    assert(result == CollectionNode::create_result_t::SUCCESS);
	    return tm.remove(t, extent->get_laddr());
	  }).si_then([] (auto) {
            return create_iertr::make_ready_future<>();
          });
        }).handle_error_interruptible(
	  crimson::ct_error::enospc::assert_failure{"unexpected enospc"},
	  create_iertr::pass_further{}
	);
      }
      case CollectionNode::create_result_t::SUCCESS: {
        return create_iertr::make_ready_future<>();
      }
      }
      __builtin_unreachable();
    });
  });
}

FlatCollectionManager::list_ret
FlatCollectionManager::list(const coll_root_t &coll_root, Transaction &t)
{
  logger().debug("FlatCollectionManager: {}", __func__);
  return get_coll_root(coll_root, t)
    .si_then([] (auto extent) {
    return extent->list();
  });
}

FlatCollectionManager::update_ret
FlatCollectionManager::update(const coll_root_t &coll_root, Transaction &t,
                              coll_t cid, coll_info_t info)
{
  logger().debug("FlatCollectionManager: {}", __func__);
  return get_coll_root(coll_root, t)
    .si_then([this, &t, cid, info] (auto extent) {
      return extent->update(get_coll_context(t), cid, info.split_bits);
  });
}

FlatCollectionManager::remove_ret
FlatCollectionManager::remove(const coll_root_t &coll_root, Transaction &t,
                              coll_t cid )
{
  logger().debug("FlatCollectionManager: {}", __func__);
  return get_coll_root(coll_root, t).si_then([this, &t, cid] (auto extent) {
    return extent->remove(get_coll_context(t), cid);
  });
}

}
