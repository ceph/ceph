// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/btree/btree_types.h"

namespace crimson::os::seastore {

class BackrefMapping {
  op_context_t ctx;
  CachedExtentRef parent;
  fixed_kv_node_meta_t<paddr_t> range;
  laddr_t value;
  extent_len_t len = 0;
  uint16_t pos = std::numeric_limits<uint16_t>::max();
  extent_types_t type;
public:
  BackrefMapping(
    extent_types_t type,
    op_context_t ctx,
    CachedExtentRef parent,
    uint16_t pos,
    laddr_t value,
    extent_len_t len,
    fixed_kv_node_meta_t<paddr_t> meta)
      : ctx(ctx),
	parent(parent),
	range(meta),
	value(value),
	len(len),
	pos(pos),
	type(type)
  {}

  extent_len_t get_length() const {
    ceph_assert(range.end > range.begin);
    return len;
  }

  laddr_t get_val() const {
    return value;
  }

  paddr_t get_key() const {
    return range.begin;
  }

  extent_types_t get_type() const {
    return type;
  }
};

using BackrefMappingRef = std::unique_ptr<BackrefMapping>;
using backref_pin_list_t = std::list<BackrefMappingRef>;

} // namespace crimson::os::seastore
