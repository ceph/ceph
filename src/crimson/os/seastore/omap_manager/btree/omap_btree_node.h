// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <string>
#include <vector>

//#include <boost/iterator/counting_iterator.hpp>

#include "crimson/common/log.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/omap_manager.h"
#include "crimson/os/seastore/omap_manager/btree/omap_types.h"

namespace crimson::os::seastore::omap_manager{

struct omap_context_t {
  TransactionManager &tm;
  Transaction &t;
  laddr_t hint;
};

enum class mutation_status_t : uint8_t {
  SUCCESS = 0,
  WAS_SPLIT = 1,
  NEED_MERGE = 2,
  FAIL = 3
};

struct OMapNode : LogicalCachedExtent {
  using base_iertr = OMapManager::base_iertr;

  using OMapNodeRef = TCachedExtentRef<OMapNode>;

  struct mutation_result_t {
    mutation_status_t status;
    /// Only populated if WAS_SPLIT, indicates the newly created left and right nodes
    /// from splitting the target entry during insertion.
    std::optional<std::tuple<OMapNodeRef, OMapNodeRef, std::string>> split_tuple;
    /// only sopulated if need merged, indicate which entry need be doing merge in upper layer.
    std::optional<OMapNodeRef> need_merge;

    mutation_result_t(mutation_status_t s, std::optional<std::tuple<OMapNodeRef,
                      OMapNodeRef, std::string>> tuple, std::optional<OMapNodeRef> n_merge)
    : status(s),
      split_tuple(tuple),
      need_merge(n_merge) {}
  };

  explicit OMapNode(ceph::bufferptr &&ptr) : LogicalCachedExtent(std::move(ptr)) {}
  explicit OMapNode(extent_len_t length) : LogicalCachedExtent(length) {}
  OMapNode(const OMapNode &other)
  : LogicalCachedExtent(other) {}

  using get_value_iertr = base_iertr;
  using get_value_ret = OMapManager::omap_get_value_ret;
  virtual get_value_ret get_value(
    omap_context_t oc,
    const std::string &key) = 0;

  using insert_iertr = base_iertr;
  using insert_ret = insert_iertr::future<mutation_result_t>;
  virtual insert_ret insert(
    omap_context_t oc,
    const std::string &key,
    const ceph::bufferlist &value) = 0;

  using rm_key_iertr = base_iertr;
  using rm_key_ret = rm_key_iertr::future<mutation_result_t>;
  virtual rm_key_ret rm_key(
    omap_context_t oc,
    const std::string &key) = 0;

  using omap_list_config_t = OMapManager::omap_list_config_t;
  using list_iertr = base_iertr;
  using list_bare_ret = OMapManager::omap_list_bare_ret;
  using list_ret = OMapManager::omap_list_ret;
  virtual list_ret list(
    omap_context_t oc,
    const std::optional<std::string> &first,
    const std::optional<std::string> &last,
    omap_list_config_t config) = 0;

  using clear_iertr = base_iertr;
  using clear_ret = clear_iertr::future<>;
  virtual clear_ret clear(omap_context_t oc) = 0;

  using full_merge_iertr = base_iertr;
  using full_merge_ret = full_merge_iertr::future<OMapNodeRef>;
  virtual full_merge_ret make_full_merge(
    omap_context_t oc,
    OMapNodeRef right) = 0;

  using make_balanced_iertr = base_iertr;
  using make_balanced_ret = make_balanced_iertr::future
          <std::tuple<OMapNodeRef, OMapNodeRef, std::string>>;
  virtual make_balanced_ret make_balanced(
    omap_context_t oc,
    OMapNodeRef _right) = 0;

  virtual omap_node_meta_t get_node_meta() const = 0;
  virtual bool extent_will_overflow(
    size_t ksize,
    std::optional<size_t> vsize) const = 0;
  virtual bool can_merge(OMapNodeRef right) const = 0;
  virtual bool extent_is_below_min() const = 0;
  virtual uint32_t get_node_size() = 0;

  virtual ~OMapNode() = default;
};

using OMapNodeRef = OMapNode::OMapNodeRef;

using omap_load_extent_iertr = OMapNode::base_iertr;
omap_load_extent_iertr::future<OMapNodeRef>
omap_load_extent(omap_context_t oc, laddr_t laddr, depth_t depth);

}

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::omap_manager::OMapNode> : fmt::ostream_formatter {};
#endif
