// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRBD_TYPES_H
#define LIBRBD_TYPES_H

#include "include/types.h"
#include "cls/rbd/cls_rbd_types.h"
#include "deep_copy/Types.h"
#include <map>
#include <string>

namespace librbd {

// Performance counters
enum {
  l_librbd_first = 26000,

  l_librbd_rd,               // read ops
  l_librbd_rd_bytes,         // bytes read
  l_librbd_rd_latency,       // average latency
  l_librbd_wr,
  l_librbd_wr_bytes,
  l_librbd_wr_latency,
  l_librbd_discard,
  l_librbd_discard_bytes,
  l_librbd_discard_latency,
  l_librbd_flush,
  l_librbd_flush_latency,

  l_librbd_ws,
  l_librbd_ws_bytes,
  l_librbd_ws_latency,

  l_librbd_cmp,
  l_librbd_cmp_bytes,
  l_librbd_cmp_latency,

  l_librbd_snap_create,
  l_librbd_snap_remove,
  l_librbd_snap_rollback,
  l_librbd_snap_rename,

  l_librbd_notify,
  l_librbd_resize,

  l_librbd_readahead,
  l_librbd_readahead_bytes,

  l_librbd_invalidate_cache,

  l_librbd_opened_time,
  l_librbd_lock_acquired_time,

  l_librbd_last,
};

typedef std::map<uint64_t, uint64_t> SnapSeqs;

/// Full information about an image's parent.
struct ParentImageInfo {
  /// Identification of the parent.
  cls::rbd::ParentImageSpec spec;

  /** @brief Where the portion of data shared with the child image ends.
   * Since images can be resized multiple times, the portion of data shared
   * with the child image is not necessarily min(parent size, child size).
   * If the child image is first shrunk and then enlarged, the common portion
   * will be shorter. */
  uint64_t overlap = 0;
};

struct SnapInfo {
  std::string name;
  cls::rbd::SnapshotNamespace snap_namespace;
  uint64_t size;
  ParentImageInfo parent;
  uint8_t protection_status;
  uint64_t flags;
  utime_t timestamp;
  SnapInfo(std::string _name,
           const cls::rbd::SnapshotNamespace &_snap_namespace,
           uint64_t _size, const ParentImageInfo &_parent,
           uint8_t _protection_status, uint64_t _flags, utime_t _timestamp)
    : name(_name), snap_namespace(_snap_namespace), size(_size),
      parent(_parent), protection_status(_protection_status), flags(_flags),
      timestamp(_timestamp) {
  }
};

enum {
  OPEN_FLAG_SKIP_OPEN_PARENT = 1 << 0,
  OPEN_FLAG_OLD_FORMAT       = 1 << 1,
  OPEN_FLAG_IGNORE_MIGRATING = 1 << 2
};

enum ImageReadOnlyFlag {
  IMAGE_READ_ONLY_FLAG_USER        = 1 << 0,
  IMAGE_READ_ONLY_FLAG_NON_PRIMARY = 1 << 1,
};

enum SnapCreateFlag {
  SNAP_CREATE_FLAG_SKIP_OBJECT_MAP             = 1 << 0,
  SNAP_CREATE_FLAG_SKIP_NOTIFY_QUIESCE         = 1 << 1,
  SNAP_CREATE_FLAG_IGNORE_NOTIFY_QUIESCE_ERROR = 1 << 2,
};

struct MigrationInfo {
  int64_t pool_id = -1;
  std::string pool_namespace;
  std::string image_name;
  std::string image_id;
  deep_copy::SnapMap snap_map;
  uint64_t overlap = 0;
  bool flatten = false;

  MigrationInfo() {
  }
  MigrationInfo(int64_t pool_id, const std::string& pool_namespace,
                const std::string& image_name, const std::string& image_id,
                const deep_copy::SnapMap &snap_map, uint64_t overlap,
                bool flatten)
    : pool_id(pool_id), pool_namespace(pool_namespace), image_name(image_name),
      image_id(image_id), snap_map(snap_map), overlap(overlap),
      flatten(flatten) {
  }

  bool empty() const {
    return pool_id == -1;
  }
};

} // namespace librbd

#endif // LIBRBD_TYPES_H
