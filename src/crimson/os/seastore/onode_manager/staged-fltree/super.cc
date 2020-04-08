// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "super.h"
#include "node.h"

namespace crimson::os::seastore::onode {

Ref<Node> RootNodeTrackerIsolated::get_root(Transaction& t) const {
  auto iter = tracked_supers.find(&t);
  if (iter == tracked_supers.end()) {
    return nullptr;
  } else {
    return iter->second->get_p_root();
  }
}

Ref<Node> RootNodeTrackerShared::get_root(Transaction&) const {
  if (is_clean()) {
    return nullptr;
  } else {
    return tracked_super->get_p_root();
  }
}

}
