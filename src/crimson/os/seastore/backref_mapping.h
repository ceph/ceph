// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/btree/btree_range_pin.h"

namespace crimson::os::seastore {

class BackrefMapping : public BtreeNodeMapping<paddr_t, laddr_t> {
  extent_types_t type;
public:
  BackrefMapping(op_context_t<paddr_t> ctx)
    : BtreeNodeMapping(ctx) {}
  template <typename... T>
  BackrefMapping(extent_types_t type, T&&... t)
    : BtreeNodeMapping(std::forward<T>(t)...),
      type(type) {}
  extent_types_t get_type() const {
    return type;
  }
};

using BackrefMappingRef = std::unique_ptr<BackrefMapping>;
using backref_pin_list_t = std::list<BackrefMappingRef>;

} // namespace crimson::os::seastore
