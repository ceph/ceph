// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive/set.hpp>

#include "crimson/common/log.h"

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore {

template <typename node_key_t>
struct op_context_t {
  Cache &cache;
  Transaction &trans;
};

constexpr uint16_t MAX_FIXEDKVBTREE_DEPTH = 8;

template <typename bound_t>
struct fixed_kv_node_meta_t {
  bound_t begin = min_max_t<bound_t>::min;
  bound_t end = min_max_t<bound_t>::min;
  depth_t depth = 0;

  bool is_parent_of(const fixed_kv_node_meta_t &other) const {
    return (depth == other.depth + 1) &&
      (begin <= other.begin) &&
      (end > other.begin);
  }

  bool is_in_range(const bound_t key) const {
    return begin <= key && end > key;
  }

  std::pair<fixed_kv_node_meta_t, fixed_kv_node_meta_t> split_into(bound_t pivot) const {
    return std::make_pair(
      fixed_kv_node_meta_t{begin, pivot, depth},
      fixed_kv_node_meta_t{pivot, end, depth});
  }

  static fixed_kv_node_meta_t merge_from(
    const fixed_kv_node_meta_t &lhs, const fixed_kv_node_meta_t &rhs) {
    ceph_assert(lhs.depth == rhs.depth);
    return fixed_kv_node_meta_t{lhs.begin, rhs.end, lhs.depth};
  }

  static std::pair<fixed_kv_node_meta_t, fixed_kv_node_meta_t>
  rebalance(const fixed_kv_node_meta_t &lhs, const fixed_kv_node_meta_t &rhs, bound_t pivot) {
    ceph_assert(lhs.depth == rhs.depth);
    return std::make_pair(
      fixed_kv_node_meta_t{lhs.begin, pivot, lhs.depth},
      fixed_kv_node_meta_t{pivot, rhs.end, lhs.depth});
  }

  bool is_root() const {
    return begin == min_max_t<bound_t>::min && end == min_max_t<bound_t>::max;
  }
};

template <typename bound_t>
inline std::ostream &operator<<(
  std::ostream &lhs,
  const fixed_kv_node_meta_t<bound_t> &rhs)
{
  return lhs << "btree_node_meta_t("
	     << "begin=" << rhs.begin
	     << ", end=" << rhs.end
	     << ", depth=" << rhs.depth
	     << ")";
}

/**
 * fixed_kv_node_meta_le_t
 *
 * On disk layout for fixed_kv_node_meta_t
 */
template <typename bound_le_t>
struct fixed_kv_node_meta_le_t {
  bound_le_t begin = bound_le_t(0);
  bound_le_t end = bound_le_t(0);
  depth_le_t depth = init_depth_le(0);

  fixed_kv_node_meta_le_t() = default;
  fixed_kv_node_meta_le_t(
    const fixed_kv_node_meta_le_t<bound_le_t> &) = default;
  explicit fixed_kv_node_meta_le_t(
    const fixed_kv_node_meta_t<typename bound_le_t::orig_type> &val)
    : begin(val.begin),
      end(val.end),
      depth(init_depth_le(val.depth)) {}

  operator fixed_kv_node_meta_t<typename bound_le_t::orig_type>() const {
    return fixed_kv_node_meta_t<typename bound_le_t::orig_type>{
	    begin, end, depth };
  }
};

template <typename key_t, typename val_t>
class BtreeNodeMapping : public PhysicalNodeMapping<key_t, val_t> {
protected:
  op_context_t<key_t> ctx;
  /**
   * parent
   *
   * populated until link_extent is called to ensure cache residence
   * until add_pin is called.
   */
  CachedExtentRef parent;

  pladdr_t value;
  extent_len_t len = 0;
  fixed_kv_node_meta_t<key_t> range;
  uint16_t pos = std::numeric_limits<uint16_t>::max();

  virtual std::unique_ptr<BtreeNodeMapping> _duplicate(op_context_t<key_t>) const = 0;
  fixed_kv_node_meta_t<key_t> _get_pin_range() const {
    return range;
  }

public:
  using val_type = val_t;
  BtreeNodeMapping(op_context_t<key_t> ctx) : ctx(ctx) {}

  BtreeNodeMapping(
    op_context_t<key_t> ctx,
    CachedExtentRef parent,
    uint16_t pos,
    pladdr_t value,
    extent_len_t len,
    fixed_kv_node_meta_t<key_t> meta)
    : ctx(ctx),
      parent(parent),
      value(value),
      len(len),
      range(meta),
      pos(pos)
  {
    if (!parent->is_pending()) {
      this->child_pos = {parent, pos};
    }
  }

  CachedExtentRef get_parent() const final {
    return parent;
  }

  CachedExtentRef get_parent() {
    return parent;
  }

  uint16_t get_pos() const final {
    return pos;
  }

  extent_len_t get_length() const final {
    ceph_assert(range.end > range.begin);
    return len;
  }

  extent_types_t get_type() const override {
    ceph_abort("should never happen");
    return extent_types_t::ROOT;
  }

  val_t get_val() const final {
    if constexpr (std::is_same_v<val_t, paddr_t>) {
      return value.get_paddr();
    } else {
      static_assert(std::is_same_v<val_t, laddr_t>);
      return value.get_laddr();
    }
  }

  key_t get_key() const override {
    return range.begin;
  }

  PhysicalNodeMappingRef<key_t, val_t> duplicate() const final {
    auto ret = _duplicate(ctx);
    ret->range = range;
    ret->value = value;
    ret->parent = parent;
    ret->len = len;
    ret->pos = pos;
    return ret;
  }

  bool has_been_invalidated() const final {
    return parent->has_been_invalidated();
  }

  get_child_ret_t<LogicalCachedExtent> get_logical_extent(Transaction&) final;
  bool is_stable() const final;
};

}
