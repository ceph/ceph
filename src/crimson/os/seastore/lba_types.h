// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <boost/intrusive/set.hpp>

#include "crimson/common/log.h"

#include "crimson/os/seastore/seastore_types.h"
// todo: can't be included in header file due to transaction includes.
//       include from cc if needed.
//#include "crimson/os/seastore/logical_child_node.h"


namespace crimson::os::seastore {

namespace lba {

/**
 * lba_map_val_t
 *
 * struct representing a single lba mapping
 */
struct lba_map_val_t {
  extent_len_t len = 0;  ///< length of mapping
  pladdr_t pladdr;         ///< direct addr of mapping or
			   //	laddr of a direct lba mapping(see btree_lba_manager.h)
  extent_ref_count_t refcount = 0; ///< refcount
  checksum_t checksum = 0; ///< checksum of original block written at paddr (TODO)
  extent_types_t type = extent_types_t::NONE;

  lba_map_val_t() = default;
  lba_map_val_t(
    extent_len_t len,
    pladdr_t pladdr,
    extent_ref_count_t refcount,
    checksum_t checksum,
    extent_types_t type)
    : len(len), pladdr(pladdr), refcount(refcount),
      checksum(checksum), type(type) {}
  bool operator==(const lba_map_val_t&) const = default;
};

std::ostream& operator<<(std::ostream& out, const lba_map_val_t&);

/**
 * lba_map_val_le_t
 *
 * On disk layout for lba_map_val_t.
 */
struct __attribute__((packed)) lba_map_val_le_t {
  extent_len_le_t len = init_extent_len_le(0);
  pladdr_le_t pladdr;
  extent_ref_count_le_t refcount{0};
  checksum_le_t checksum{0};
  extent_types_le_t type = 0;

  lba_map_val_le_t() = default;
  lba_map_val_le_t(const lba_map_val_le_t &) = default;
  explicit lba_map_val_le_t(const lba_map_val_t &val)
    : len(init_extent_len_le(val.len)),
      pladdr(pladdr_le_t(val.pladdr)),
      refcount(val.refcount),
      checksum(val.checksum),
      type((extent_types_le_t)val.type) {}

  operator lba_map_val_t() const {
    return lba_map_val_t{
      len,
      pladdr,
      refcount,
      checksum,
      (extent_types_t)type};
  }
};

} // namespace lba

enum class op_type {
  mkfs,
  init_cached_extent,
  alloc_extent,
  alloc_extents,
  clone_mapping,
  reserve_region,
  rewrite_extent,
  get_physical_extent_if_live,
  update_refcount,
  update_mappings
};

using overlay_value_t = std::variant<
  std::monostate,
  extent_ref_count_t,
//  std::vector<LogicalChildNodeRef>,
  paddr_t
>;

template<typename T, typename Variant>
T& expect_value(Variant& v) {
  auto ptr = std::get_if<T>(&v);
  assert(ptr && "unexpected variant type");
  return *ptr;
}

// overlay_entry consists an operation type and
// all the relevant paramaters to be passed to lba_manager

struct overlay_entry {
  op_type op;
  overlay_value_t value;
};

struct LBAOverlayCursor {
private:
  laddr_t key;
  std::optional<lba::lba_map_val_t> val;

  // overlaid values
  std::optional<extent_ref_count_t> overlaid_refcount;
  std::optional<paddr_t> address;

  friend class LBAOverlayManager;

public:
// todo: until LBACursorRef is not part of lba tree...
//       use primtive ctor overload
/*
  LBAOverlayCursor(LBACursorRef base_cursor) 
     : key(base_cursor->key),
       val (base_cursor->val)
      {}
*/

  LBAOverlayCursor(laddr_t key, std::optional<lba::lba_map_val_t> val)
     : key(key),
       val(val)
      {}

  LBAOverlayCursor() = default;

  template<typename T>
  void set_overlay(std::optional<T> LBAOverlayCursor::*overlay_value, const T& value) {
      this->*overlay_value = value;
  }

  template<typename T>
  const T* get_overlay(std::optional<T> LBAOverlayCursor::*overlay_value) const {
    if ((this->*overlay_value).has_value()) {
      return &((this->*overlay_value).value());
    }
    return nullptr;
  }
};
} // namespace crimson::os::seastore
