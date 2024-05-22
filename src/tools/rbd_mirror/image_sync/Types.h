// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_SYNC_TYPES_H
#define RBD_MIRROR_IMAGE_SYNC_TYPES_H

#include "cls/rbd/cls_rbd_types.h"
#include "librbd/Types.h"
#include <list>
#include <string>
#include <boost/optional.hpp>

struct Context;

namespace rbd {
namespace mirror {
namespace image_sync {

struct SyncPoint {
  typedef boost::optional<uint64_t> ObjectNumber;

  SyncPoint() {
  }
  SyncPoint(const cls::rbd::SnapshotNamespace& snap_namespace,
            const std::string& snap_name,
            const std::string& from_snap_name,
            const ObjectNumber& object_number)
    : snap_namespace(snap_namespace), snap_name(snap_name),
      from_snap_name(from_snap_name), object_number(object_number) {
  }

  cls::rbd::SnapshotNamespace snap_namespace =
    {cls::rbd::UserSnapshotNamespace{}};
  std::string snap_name;
  std::string from_snap_name;
  ObjectNumber object_number = boost::none;

  bool operator==(const SyncPoint& rhs) const {
    return (snap_namespace == rhs.snap_namespace &&
            snap_name == rhs.snap_name &&
            from_snap_name == rhs.from_snap_name &&
            object_number == rhs.object_number);
  }
};

typedef std::list<SyncPoint> SyncPoints;

struct SyncPointHandler {
public:
  SyncPointHandler(const SyncPointHandler&) = delete;
  SyncPointHandler& operator=(const SyncPointHandler&) = delete;

  virtual ~SyncPointHandler() {}
  virtual void destroy() {
    delete this;
  }

  virtual SyncPoints get_sync_points() const = 0;
  virtual librbd::SnapSeqs get_snap_seqs() const = 0;

  virtual void update_sync_points(const librbd::SnapSeqs& snap_seq,
                                  const SyncPoints& sync_points,
                                  bool sync_complete,
                                  Context* on_finish) = 0;

protected:
  SyncPointHandler() {}
};

} // namespace image_sync
} // namespace mirror
} // namespace rbd

#endif // RBD_MIRROR_IMAGE_SYNC_TYPES_H
