// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "crimson/osd/exceptions.h"

#include "crimson/os/seastore/omap_manager.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"

namespace crimson::os::seastore::omap_manager {
/**
 * BtreeOMapManager
 *
 * Uses a btree to track :
 * string -> string mapping for each onode omap
 */

class BtreeOMapManager : public OMapManager {
  TransactionManager &tm;

  omap_context_t get_omap_context(
    Transaction &t, const omap_root_t &omap_root) {
    ceph_assert(omap_root.type < omap_type_t::NONE);
    return omap_context_t{tm, t, omap_root.hint, omap_root.type};
  }

  /* get_omap_root
   *
   * load omap tree root node
   */
  using get_root_iertr = base_iertr;
  using get_root_ret = get_root_iertr::future<OMapNodeRef>;
  static get_root_ret get_omap_root(
    omap_context_t c,
    const omap_root_t &omap_root);

  /* handle_root_split
   *
   * root has been split and needs to update omap_root_t
   */
  using handle_root_split_iertr = base_iertr;
  using handle_root_split_ret = handle_root_split_iertr::future<>;
  handle_root_split_ret handle_root_split(
    omap_context_t c,
    omap_root_t &omap_root,
    const OMapNode::mutation_result_t& mresult);

  /* handle_root_merge
   *
   * root node has only one item and it is not leaf node, need remove a layer
   */
  using handle_root_merge_iertr = base_iertr;
  using handle_root_merge_ret = handle_root_merge_iertr::future<>;
  handle_root_merge_ret handle_root_merge(
    omap_context_t oc,
    omap_root_t &omap_root, 
    OMapNode:: mutation_result_t mresult);

public:
  explicit BtreeOMapManager(TransactionManager &tm);

  initialize_omap_ret initialize_omap(Transaction &t, laddr_t hint,
    omap_type_t type) final;

  omap_get_value_ret omap_get_value(
    const omap_root_t &omap_root,
    Transaction &t,
    const std::string &key) final;

  omap_set_key_ret omap_set_key(
    omap_root_t &omap_root,
    Transaction &t,
    const std::string &key, const ceph::bufferlist &value) final;

  omap_set_keys_ret omap_set_keys(
    omap_root_t &omap_root,
    Transaction &t,
    std::map<std::string, ceph::bufferlist>&& keys) final;

  omap_rm_key_ret omap_rm_key(
    omap_root_t &omap_root,
    Transaction &t,
    const std::string &key) final;

  omap_rm_key_range_ret omap_rm_key_range(
    omap_root_t &omap_root,
    Transaction &t,
    const std::string &first,
    const std::string &last,
    omap_list_config_t config) final;

  omap_list_ret omap_list(
    const omap_root_t &omap_root,
    Transaction &t,
    const std::optional<std::string> &first,
    const std::optional<std::string> &last,
    omap_list_config_t config = omap_list_config_t()) final;

  omap_clear_ret omap_clear(
    omap_root_t &omap_root,
    Transaction &t) final;
};
using BtreeOMapManagerRef = std::unique_ptr<BtreeOMapManager>;

}
