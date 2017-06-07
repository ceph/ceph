// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRBD_TYPES_H
#define LIBRBD_TYPES_H

#include "include/types.h"
#include "cls/rbd/cls_rbd_types.h"
#include <string>

namespace librbd {

/** @brief Unique identification of a parent in clone relationship.
 * Cloning an image creates a child image that keeps a reference
 * to its parent. This allows copy-on-write images. */
struct ParentSpec {
  int64_t pool_id;
  std::string image_id;
  snapid_t snap_id;

  ParentSpec() : pool_id(-1), snap_id(CEPH_NOSNAP) {
  }
  ParentSpec(int64_t pool_id, std::string image_id, snapid_t snap_id)
    : pool_id(pool_id), image_id(image_id), snap_id(snap_id) {
  }

  bool operator==(const ParentSpec &other) {
    return ((this->pool_id == other.pool_id) &&
            (this->image_id == other.image_id) &&
            (this->snap_id == other.snap_id));
  }
  bool operator!=(const ParentSpec &other) {
    return !(*this == other);
  }
};

/// Full information about an image's parent.
struct ParentInfo {
  /// Identification of the parent.
  ParentSpec spec;

  /** @brief Where the portion of data shared with the child image ends.
   * Since images can be resized multiple times, the portion of data shared
   * with the child image is not necessarily min(parent size, child size).
   * If the child image is first shrunk and then enlarged, the common portion
   * will be shorter. */
  uint64_t overlap;

  ParentInfo() : overlap(0) {
  }
};

struct SnapInfo {
  std::string name;
  cls::rbd::SnapshotNamespace snap_namespace;
  uint64_t size;
  ParentInfo parent;
  uint8_t protection_status;
  uint64_t flags;
  utime_t timestamp;
  SnapInfo(std::string _name,
           const cls::rbd::SnapshotNamespace &_snap_namespace,
           uint64_t _size, const ParentInfo &_parent,
           uint8_t _protection_status, uint64_t _flags, utime_t _timestamp)
    : name(_name), snap_namespace(_snap_namespace), size(_size),
      parent(_parent), protection_status(_protection_status), flags(_flags),
      timestamp(_timestamp) {
  }
};

} // namespace librbd

#endif // LIBRBD_TYPES_H
