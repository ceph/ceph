// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_POOL_WATCHER_TYPES_H
#define CEPH_RBD_MIRROR_POOL_WATCHER_TYPES_H

#include "tools/rbd_mirror/Types.h"
#include <string>

namespace rbd {
namespace mirror {
namespace pool_watcher {

struct Listener {
  virtual ~Listener() {
  }

  virtual void handle_update(const std::string &mirror_uuid,
                             ImageIds &&added_image_ids,
                             ImageIds &&removed_image_ids) = 0;
};

} // namespace pool_watcher
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_POOL_WATCHER_TYPES_H
