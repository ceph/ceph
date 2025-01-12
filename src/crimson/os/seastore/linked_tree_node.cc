// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/linked_tree_node.h"

namespace crimson::os::seastore {

template <typename T>
std::ostream &operator<<(std::ostream &out, const parent_tracker_t<T> &tracker) {
  return out << "tracker_ptr=" << (void*)&tracker
	     << ", parent_ptr=" << (void*)tracker.get_parent().get();
}

} // crimson::os::seastore
