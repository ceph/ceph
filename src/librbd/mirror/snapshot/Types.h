// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_TYPES_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_TYPES_H

#include "cls/rbd/cls_rbd_types.h"
#include "include/buffer.h"
#include "include/types.h"

#include <map>
#include <string>

namespace librbd {
namespace mirror {
namespace snapshot {

enum CreatePrimaryFlags {
  CREATE_PRIMARY_FLAG_IGNORE_EMPTY_PEERS = (1 << 0),
  CREATE_PRIMARY_FLAG_DEMOTED            = (1 << 1),
  CREATE_PRIMARY_FLAG_FORCE              = (1 << 2)
};

struct ImageStateHeader {
  uint32_t object_count = 0;

  ImageStateHeader() {
  }
  ImageStateHeader(uint32_t object_count) : object_count(object_count) {
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator &it);
};

WRITE_CLASS_ENCODER(ImageStateHeader);

struct SnapState {
  cls::rbd::SnapshotNamespace snap_namespace;
  std::string name;
  uint8_t protection_status = 0;

  SnapState() {
  }
  SnapState(const cls::rbd::SnapshotNamespace &snap_namespace,
            const std::string &name, uint8_t protection_status)
    : snap_namespace(snap_namespace), name(name),
      protection_status(protection_status) {
  }

  bool operator==(const SnapState& rhs) const {
    return snap_namespace == rhs.snap_namespace &&
           name == rhs.name && protection_status == rhs.protection_status;
  }

  bool operator<(const SnapState& rhs) const {
    if (snap_namespace != rhs.snap_namespace) {
      return snap_namespace < rhs.snap_namespace;
    }
    if (name != rhs.name) {
      return name < rhs.name;
    }
    return protection_status < rhs.protection_status;
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator &it);
  void dump(Formatter *f) const;
};

std::ostream& operator<<(std::ostream& os, const SnapState& snap_state);

WRITE_CLASS_ENCODER(SnapState);

struct ImageState {
  std::string name;
  uint64_t features = 0;
  uint64_t snap_limit = 0;
  std::map<uint64_t, SnapState> snapshots;
  std::map<std::string, bufferlist> metadata;

  ImageState() {
  }
  ImageState(const std::string &name, uint64_t features, uint64_t snap_limit,
             const std::map<uint64_t, SnapState> &snapshots,
             const std::map<std::string, bufferlist> &metadata)
    : name(name), features(features), snap_limit(snap_limit),
      snapshots(snapshots), metadata(metadata) {
  }

  bool operator==(const ImageState& rhs) const {
    return name == rhs.name && features == rhs.features &&
           snap_limit == rhs.snap_limit && snapshots == rhs.snapshots;
  }

  bool operator<(const ImageState& rhs) const {
    if (name != rhs.name) {
      return name < rhs.name;
    }
    if (features != rhs.features) {
      return features < rhs.features;
    }
    if (snap_limit != rhs.snap_limit) {
      return snap_limit < rhs.snap_limit;
    }
    return snapshots < rhs.snapshots;
  }

  void encode(bufferlist &bl) const;
  void decode(bufferlist::const_iterator &it);
  void dump(Formatter *f) const;
};

std::ostream& operator<<(std::ostream& os, const ImageState& image_state);

WRITE_CLASS_ENCODER(ImageState);

} // namespace snapshot
} // namespace mirror
} // namespace librbd

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_TYPES_H
