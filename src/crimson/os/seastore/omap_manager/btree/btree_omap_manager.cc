// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include "crimson/common/log.h"

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/omap_manager/btree/btree_omap_manager.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node_impl.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore);
  }
}

namespace crimson::os::seastore::omap_manager {

BtreeOMapManager::BtreeOMapManager(
  TransactionManager &tm)
  : tm(tm) {}

BtreeOMapManager::initialize_omap_ret
BtreeOMapManager::initialize_omap(Transaction &t)
{

  logger().debug("{}", __func__);
  return tm.alloc_extent<OMapLeafNode>(t, L_ADDR_MIN, OMAP_BLOCK_SIZE)
    .si_then([](auto&& root_extent) {
      root_extent->set_size(0);
      omap_node_meta_t meta{1};
      root_extent->set_meta(meta);
      omap_root_t omap_root;
      omap_root.update(root_extent->get_laddr(), 1);
      return initialize_omap_iertr::make_ready_future<omap_root_t>(omap_root);
  });
}

BtreeOMapManager::get_root_ret
BtreeOMapManager::get_omap_root(omap_context_t oc, const omap_root_t &omap_root)
{
  assert(omap_root.get_location() != L_ADDR_NULL);
  laddr_t laddr = omap_root.get_location();
  return omap_load_extent(oc, laddr, omap_root.get_depth());
}

BtreeOMapManager::handle_root_split_ret
BtreeOMapManager::handle_root_split(
  omap_context_t oc,
  omap_root_t &omap_root,
  const OMapNode::mutation_result_t& mresult)
{
  return oc.tm.alloc_extent<OMapInnerNode>(oc.t, L_ADDR_MIN, OMAP_BLOCK_SIZE)
    .si_then([&omap_root, mresult](auto&& nroot) -> handle_root_split_ret {
    auto [left, right, pivot] = *(mresult.split_tuple);
    omap_node_meta_t meta{omap_root.depth + 1};
    nroot->set_meta(meta);
    nroot->journal_inner_insert(nroot->iter_begin(), left->get_laddr(),
                                "", nroot->maybe_get_delta_buffer());
    nroot->journal_inner_insert(nroot->iter_begin() + 1, right->get_laddr(),
                                pivot, nroot->maybe_get_delta_buffer());
    omap_root.update(nroot->get_laddr(), omap_root.get_depth() + 1);
    return seastar::now();
  });
}

BtreeOMapManager::handle_root_merge_ret
BtreeOMapManager::handle_root_merge(
  omap_context_t oc,
  omap_root_t &omap_root, 
  OMapNode::mutation_result_t mresult)
{
  auto root = *(mresult.need_merge);
  auto iter = root->cast<OMapInnerNode>()->iter_begin();
  omap_root.update(
    iter->get_val(),
    omap_root.depth -= 1);
  return oc.tm.dec_ref(oc.t, root->get_laddr()
  ).si_then([](auto &&ret) -> handle_root_merge_ret {
    return seastar::now();
  }).handle_error_interruptible(
    handle_root_merge_iertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in handle_root_merge"
    }
  );
}

BtreeOMapManager::omap_get_value_ret
BtreeOMapManager::omap_get_value(
  const omap_root_t &omap_root,
  Transaction &t,
  const std::string &key)
{
  logger().debug("{}: {}", __func__, key);
  return get_omap_root(
    get_omap_context(t),
    omap_root
  ).si_then([this, &t, &key](auto&& extent) {
    return extent->get_value(get_omap_context(t), key);
  }).si_then([](auto &&e) {
    return omap_get_value_ret(
        interruptible::ready_future_marker{},
        std::move(e));
  });
}

BtreeOMapManager::omap_set_keys_ret
BtreeOMapManager::omap_set_keys(
  omap_root_t &omap_root,
  Transaction &t,
  std::map<std::string, ceph::bufferlist>&& keys)
{
  return seastar::do_with(std::move(keys), [&, this](auto& keys) {
    return trans_intr::do_for_each(
      keys.begin(),
      keys.end(),
      [&, this](auto &p) {
      return omap_set_key(omap_root, t, p.first, p.second);
    });
  });
}

BtreeOMapManager::omap_set_key_ret
BtreeOMapManager::omap_set_key(
  omap_root_t &omap_root,
  Transaction &t,
  const std::string &key,
  const ceph::bufferlist &value)
{
  logger().debug("{}: {} -> {}", __func__, key, value);
  return get_omap_root(
    get_omap_context(t),
    omap_root
  ).si_then([this, &t, &key, &value](auto root) {
    return root->insert(get_omap_context(t), key, value);
  }).si_then([this, &omap_root, &t](auto mresult) -> omap_set_key_ret {
    if (mresult.status == mutation_status_t::SUCCESS)
      return seastar::now();
    else if (mresult.status == mutation_status_t::WAS_SPLIT)
      return handle_root_split(get_omap_context(t), omap_root, mresult);
    else
      return seastar::now();
  });
}

BtreeOMapManager::omap_rm_key_ret
BtreeOMapManager::omap_rm_key(
  omap_root_t &omap_root,
  Transaction &t,
  const std::string &key)
{
  logger().debug("{}: {}", __func__, key);
  return get_omap_root(
    get_omap_context(t),
    omap_root
  ).si_then([this, &t, &key](auto root) {
    return root->rm_key(get_omap_context(t), key);
  }).si_then([this, &omap_root, &t](auto mresult) -> omap_rm_key_ret {
    if (mresult.status == mutation_status_t::SUCCESS) {
      return seastar::now();
    } else if (mresult.status == mutation_status_t::WAS_SPLIT) {
      return handle_root_split(get_omap_context(t), omap_root, mresult);
    } else if (mresult.status == mutation_status_t::NEED_MERGE) {
      auto root = *(mresult.need_merge);
      if (root->get_node_size() == 1 && omap_root.depth != 1) {
        return handle_root_merge(get_omap_context(t), omap_root, mresult);
      } else {
        return seastar::now(); 
      }
    } else {
      return seastar::now();
    }
  });

}

BtreeOMapManager::omap_list_ret
BtreeOMapManager::omap_list(
  const omap_root_t &omap_root,
  Transaction &t,
  const std::optional<std::string> &start,
  omap_list_config_t config)
{
  logger().debug("{}", __func__);
  return get_omap_root(
    get_omap_context(t),
    omap_root
  ).si_then([this, config, &t, &start](auto extent) {
    return extent->list(
      get_omap_context(t),
      start,
      config);
  });
}

BtreeOMapManager::omap_clear_ret
BtreeOMapManager::omap_clear(
  omap_root_t &omap_root,
  Transaction &t)
{
  logger().debug("{}", __func__);
  return get_omap_root(
    get_omap_context(t),
    omap_root
  ).si_then([this, &t](auto extent) {
    return extent->clear(get_omap_context(t));
  }).si_then([this, &omap_root, &t] {
    return tm.dec_ref(
      t, omap_root.get_location()
    ).si_then([&omap_root] (auto ret) {
      omap_root.update(
	L_ADDR_NULL,
	0);
      return omap_clear_iertr::now();
    });
  }).handle_error_interruptible(
    omap_clear_iertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in BtreeOMapManager::omap_clear"
    }
  );
}

}
