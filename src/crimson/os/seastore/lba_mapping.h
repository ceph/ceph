// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/cached_extent.h"
#include "crimson/os/seastore/btree/btree_range_pin.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node.h"
#include "crimson/os/seastore/logical_child_node.h"

namespace crimson::os::seastore {

class LBAMapping;
using LBAMappingRef = std::unique_ptr<LBAMapping>;

class LogicalCachedExtent;

class LBAMapping {
public:
  LBAMapping(op_context_t ctx)
    : ctx(ctx) {}
  LBAMapping(
    op_context_t ctx,
    CachedExtentRef parent,
    uint16_t pos,
    pladdr_t value,
    extent_len_t len,
    fixed_kv_node_meta_t<laddr_t> meta)
    : ctx(ctx),
      parent(parent),
      value(value),
      len(len),
      range(meta),
      pos(pos)
  {}

  CachedExtentRef get_parent() {
    return parent;
  }

  uint16_t get_pos() const {
    return pos;
  }

  extent_len_t get_length() const {
    ceph_assert(range.end > range.begin);
    return len;
  }

  paddr_t get_val() const {
    return value.get_paddr();
  }

  virtual laddr_t get_key() const {
    return range.begin;
  }

  bool has_been_invalidated() const {
    return parent->has_been_invalidated();
  }

  bool is_parent_viewable() const {
    ceph_assert(parent);
    return parent->is_viewable_by_trans(ctx.trans).first;
  }

  bool is_parent_valid() const {
    ceph_assert(parent);
    return parent->is_valid();
  }

  virtual void maybe_fix_pos() = 0;
  virtual bool parent_modified() const = 0;
  virtual uint32_t get_checksum() const = 0;

  // An lba pin may be indirect, see comments in lba_manager/btree/btree_lba_manager.h
  virtual bool is_indirect() const = 0;
  virtual laddr_t get_intermediate_key() const = 0;
  virtual laddr_t get_intermediate_base() const = 0;
  virtual extent_len_t get_intermediate_length() const = 0;
  // The start offset of the pin, must be 0 if the pin is not indirect
  virtual extent_len_t get_intermediate_offset() const = 0;

  virtual get_child_ret_t<lba_manager::btree::LBALeafNode, LogicalChildNode>
  get_logical_extent(Transaction &t) = 0;

  virtual LBAMappingRef refresh_with_pending_parent() = 0;

  // For reserved mappings, the return values are
  // undefined although it won't crash
  virtual bool is_stable() const = 0;
  virtual bool is_data_stable() const = 0;
  virtual bool is_clone() const = 0;
  bool is_zero_reserved() const {
    return get_val().is_zero();
  }

  LBAMappingRef duplicate() const;

  virtual ~LBAMapping() {}
protected:
  virtual LBAMappingRef _duplicate(op_context_t) const = 0;

  op_context_t ctx;
  CachedExtentRef parent;

  pladdr_t value;
  extent_len_t len = 0;
  fixed_kv_node_meta_t<laddr_t> range;
  uint16_t pos = std::numeric_limits<uint16_t>::max();
};

std::ostream &operator<<(std::ostream &out, const LBAMapping &rhs);
using lba_pin_list_t = std::list<LBAMappingRef>;

std::ostream &operator<<(std::ostream &out, const lba_pin_list_t &rhs);

} // namespace crimson::os::seastore

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::LBAMapping> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::lba_pin_list_t> : fmt::ostream_formatter {};
#endif
