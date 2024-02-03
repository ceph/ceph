// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string.h>

#include "crimson/common/log.h"

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/omap_manager/btree/btree_omap_manager.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node_impl.h"

SET_SUBSYS(seastore_omap);

namespace crimson::os::seastore::omap_manager {

BtreeOMapManager::BtreeOMapManager(
  TransactionManager &tm)
  : tm(tm) {}

BtreeOMapManager::initialize_omap_ret
BtreeOMapManager::initialize_omap(Transaction &t, laddr_t hint)
{
  LOG_PREFIX(BtreeOMapManager::initialize_omap);
  DEBUGT("hint: {}", t, hint);
  return tm.alloc_extent<OMapLeafNode>(t, hint, OMAP_LEAF_BLOCK_SIZE)
    .si_then([hint, &t](auto&& root_extent) {
      root_extent->set_size(0);
      omap_node_meta_t meta{1};
      root_extent->set_meta(meta);
      omap_root_t omap_root;
      omap_root.update(root_extent->get_laddr(), 1, hint);
      t.get_omap_tree_stats().depth = 1u;
      t.get_omap_tree_stats().extents_num_delta++;
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
  LOG_PREFIX(BtreeOMapManager::handle_root_split);
  DEBUGT("{}", oc.t, omap_root);
  return oc.tm.alloc_extent<OMapInnerNode>(oc.t, omap_root.hint,
                                           OMAP_INNER_BLOCK_SIZE)
    .si_then([&omap_root, mresult, oc](auto&& nroot) -> handle_root_split_ret {
    auto [left, right, pivot] = *(mresult.split_tuple);
    omap_node_meta_t meta{omap_root.depth + 1};
    nroot->set_meta(meta);
    nroot->journal_inner_insert(nroot->iter_begin(), left->get_laddr(),
                                "", nroot->maybe_get_delta_buffer());
    nroot->journal_inner_insert(nroot->iter_begin() + 1, right->get_laddr(),
                                pivot, nroot->maybe_get_delta_buffer());
    omap_root.update(nroot->get_laddr(), omap_root.get_depth() + 1, omap_root.hint);
    oc.t.get_omap_tree_stats().depth = omap_root.depth;
    ++(oc.t.get_omap_tree_stats().extents_num_delta);
    return seastar::now();
  });
}

BtreeOMapManager::handle_root_merge_ret
BtreeOMapManager::handle_root_merge(
  omap_context_t oc,
  omap_root_t &omap_root, 
  OMapNode::mutation_result_t mresult)
{
  LOG_PREFIX(BtreeOMapManager::handle_root_merge);
  DEBUGT("{}", oc.t, omap_root);
  auto root = *(mresult.need_merge);
  auto iter = root->cast<OMapInnerNode>()->iter_begin();
  omap_root.update(
    iter->get_val(),
    omap_root.depth -= 1,
    omap_root.hint);
  oc.t.get_omap_tree_stats().depth = omap_root.depth;
  oc.t.get_omap_tree_stats().extents_num_delta--;
  return oc.tm.remove(oc.t, root->get_laddr()
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
  LOG_PREFIX(BtreeOMapManager::omap_get_value);
  DEBUGT("key={}", t, key);
  return get_omap_root(
    get_omap_context(t, omap_root.hint),
    omap_root
  ).si_then([this, &t, &key, &omap_root](auto&& extent) {
    return extent->get_value(get_omap_context(t, omap_root.hint), key);
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
  LOG_PREFIX(BtreeOMapManager::omap_set_key);
  DEBUGT("{} -> {}", t, key, value);
  return get_omap_root(
    get_omap_context(t, omap_root.hint),
    omap_root
  ).si_then([this, &t, &key, &value, &omap_root](auto root) {
    return root->insert(get_omap_context(t, omap_root.hint), key, value);
  }).si_then([this, &omap_root, &t](auto mresult) -> omap_set_key_ret {
    if (mresult.status == mutation_status_t::SUCCESS)
      return seastar::now();
    else if (mresult.status == mutation_status_t::WAS_SPLIT)
      return handle_root_split(get_omap_context(t, omap_root.hint), omap_root, mresult);
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
  LOG_PREFIX(BtreeOMapManager::omap_rm_key);
  DEBUGT("{}", t, key);
  return get_omap_root(
    get_omap_context(t, omap_root.hint),
    omap_root
  ).si_then([this, &t, &key, &omap_root](auto root) {
    return root->rm_key(get_omap_context(t, omap_root.hint), key);
  }).si_then([this, &omap_root, &t](auto mresult) -> omap_rm_key_ret {
    if (mresult.status == mutation_status_t::SUCCESS) {
      return seastar::now();
    } else if (mresult.status == mutation_status_t::WAS_SPLIT) {
      return handle_root_split(get_omap_context(t, omap_root.hint), omap_root, mresult);
    } else if (mresult.status == mutation_status_t::NEED_MERGE) {
      auto root = *(mresult.need_merge);
      if (root->get_node_size() == 1 && omap_root.depth != 1) {
        return handle_root_merge(get_omap_context(t, omap_root.hint), omap_root, mresult);
      } else {
        return seastar::now(); 
      }
    } else {
      return seastar::now();
    }
  });

}

BtreeOMapManager::omap_rm_key_range_ret
BtreeOMapManager::omap_rm_key_range(
  omap_root_t &omap_root,
  Transaction &t,
  const std::string &first,
  const std::string &last,
  omap_list_config_t config)
{
  LOG_PREFIX(BtreeOMapManager::omap_rm_key_range);
  DEBUGT("{} ~ {}", t, first, last);
  assert(first <= last);
  return seastar::do_with(
    std::make_optional<std::string>(first),
    std::make_optional<std::string>(last),
    [this, &omap_root, &t, config](auto &first, auto &last) {
    return omap_list(
      omap_root,
      t,
      first,
      last,
      config);
  }).si_then([this, &omap_root, &t](auto results) {
    LOG_PREFIX(BtreeOMapManager::omap_rm_key_range);
    auto &[complete, kvs] = results;
    std::vector<std::string> keys;
    for (const auto& [k, _] : kvs) {
      keys.push_back(k);
    }
    DEBUGT("total {} keys to remove", t, keys.size());
    return seastar::do_with(
      std::move(keys),
      [this, &omap_root, &t](auto& keys) {
      return trans_intr::do_for_each(
	keys.begin(),
	keys.end(),
	[this, &omap_root, &t](auto& key) {
	return omap_rm_key(omap_root, t, key);
      });
    });
  });
}

BtreeOMapManager::omap_list_ret
BtreeOMapManager::omap_list(
  const omap_root_t &omap_root,
  Transaction &t,
  const std::optional<std::string> &first,
  const std::optional<std::string> &last,
  omap_list_config_t config)
{
  LOG_PREFIX(BtreeOMapManager::omap_list);
  if (first && last) {
    DEBUGT("{}, first: {}, last: {}", t, omap_root, *first, *last);
    assert(last >= first);
  } else if (first) {
    DEBUGT("{}, first: {}", t, omap_root, *first);
  } else if (last) {
    DEBUGT("{}, last: {}", t, omap_root, *last);
  } else {
    DEBUGT("{}", t, omap_root);
  }

  return get_omap_root(
    get_omap_context(t, omap_root.hint),
    omap_root
  ).si_then([this, config, &t, &first, &last, &omap_root](auto extent) {
    return extent->list(
      get_omap_context(t, omap_root.hint),
      first,
      last,
      config);
  });
}

BtreeOMapManager::omap_clear_ret
BtreeOMapManager::omap_clear(
  omap_root_t &omap_root,
  Transaction &t)
{
  LOG_PREFIX(BtreeOMapManager::omap_clear);
  DEBUGT("{}", t, omap_root);
  return get_omap_root(
    get_omap_context(t, omap_root.hint),
    omap_root
  ).si_then([this, &t, &omap_root](auto extent) {
    return extent->clear(get_omap_context(t, omap_root.hint));
  }).si_then([this, &omap_root, &t] {
    return tm.remove(
      t, omap_root.get_location()
    ).si_then([&omap_root] (auto ret) {
      omap_root.update(
	L_ADDR_NULL,
	0, L_ADDR_MIN);
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
