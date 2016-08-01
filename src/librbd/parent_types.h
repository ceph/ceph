// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_PARENT_TYPES_H
#define CEPH_LIBRBD_PARENT_TYPES_H

#include "include/int_types.h"
#include "include/types.h"
#include <string>

namespace librbd {
  /** @brief Unique identification of a parent in clone relationship.
   * Cloning an image creates a child image that keeps a reference
   * to its parent. This allows copy-on-write images. */
  struct parent_spec {
    int64_t pool_id;
    std::string image_id;
    snapid_t snap_id;
    parent_spec() : pool_id(-1), snap_id(CEPH_NOSNAP) {}
    parent_spec(uint64_t pool_id, std::string image_id, snapid_t snap_id) :
      pool_id(pool_id), image_id(image_id), snap_id(snap_id) {}
    bool operator==(const parent_spec &other) {
      return ((this->pool_id == other.pool_id) &&
	      (this->image_id == other.image_id) &&
	      (this->snap_id == other.snap_id));
    }
    bool operator!=(const parent_spec &other) {
      return !(*this == other);
    }
  };

  /// Full information about an image's parent.
  struct parent_info {
    /// Identification of the parent.
    parent_spec spec;

    /** @brief Where the portion of data shared with the child image ends.
     * Since images can be resized multiple times, the portion of data shared
     * with the child image is not necessarily min(parent size, child size).
     * If the child image is first shrunk and then enlarged, the common portion
     * will be shorter. */
    uint64_t overlap;

    parent_info() : overlap(0) {}
  };
}

enum {
  RBD_PROTECTION_STATUS_UNPROTECTED  = 0,
  RBD_PROTECTION_STATUS_UNPROTECTING = 1,
  RBD_PROTECTION_STATUS_PROTECTED    = 2,
  RBD_PROTECTION_STATUS_LAST         = 3
};

#endif
