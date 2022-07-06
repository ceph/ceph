// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/allocator.h"

namespace crimson::os::seastore {
std::ostream& operator<<(std::ostream& out, const allocator_spec_t& spec) {
  return out << "allocator_spec_t(capacity=" << spec.capacity
             << ", block_size=" << spec.block_size
             << ", min_alloc_size=" << spec.min_alloc_size
             << ", max_alloc_size=" << spec.max_alloc_size
             << ", device_id=" << spec.device_id << ")";
}

}
