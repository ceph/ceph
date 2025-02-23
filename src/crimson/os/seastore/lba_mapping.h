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

class LBAMapping : public BtreeNodeMapping<laddr_t, paddr_t> {
public:
  LBAMapping(op_context_t<laddr_t> ctx)
    : BtreeNodeMapping<laddr_t, paddr_t>(ctx) {}
  template <typename... T>
  LBAMapping(T&&... t)
    : BtreeNodeMapping<laddr_t, paddr_t>(std::forward<T>(t)...)
  {}

  // An lba pin may be indirect, see comments in lba_manager/btree/btree_lba_manager.h
  virtual bool is_indirect() const = 0;
  virtual laddr_t get_intermediate_key() const = 0;
  virtual laddr_t get_intermediate_base() const = 0;
  virtual extent_len_t get_intermediate_length() const = 0;
  // The start offset of the pin, must be 0 if the pin is not indirect
  virtual extent_len_t get_intermediate_offset() const = 0;

  virtual get_child_ret_t<lba_manager::btree::LBALeafNode, LogicalChildNode>
  get_logical_extent(Transaction &t) = 0;

  void link_child(LogicalChildNode *c) {
    ceph_assert(child_pos);
    child_pos->link_child(c);
  }
  virtual LBAMappingRef refresh_with_pending_parent() = 0;

  // For reserved mappings, the return values are
  // undefined although it won't crash
  virtual bool is_stable() const = 0;
  virtual bool is_data_stable() const = 0;
  virtual bool is_clone() const = 0;
  bool is_zero_reserved() const {
    return !get_val().is_real();
  }

  LBAMappingRef duplicate() const;

  virtual ~LBAMapping() {}
protected:
  virtual LBAMappingRef _duplicate(op_context_t<laddr_t>) const = 0;
  std::optional<child_pos_t<
    lba_manager::btree::LBALeafNode>> child_pos = std::nullopt;
};

std::ostream &operator<<(std::ostream &out, const LBAMapping &rhs);
using lba_pin_list_t = std::list<LBAMappingRef>;

std::ostream &operator<<(std::ostream &out, const lba_pin_list_t &rhs);

} // namespace crimson::os::seastore

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::LBAMapping> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::lba_pin_list_t> : fmt::ostream_formatter {};
#endif
