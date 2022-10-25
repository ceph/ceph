// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/btree/fixed_kv_node.h"

namespace crimson::os::seastore {

bool is_valid_child_ptr(ChildableCachedExtent* child) {
  return child != nullptr && child != RESERVATION_PTR;
}

} // namespace crimson::os::seastore
