// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OBJECT_MAP_TYPES_H
#define CEPH_LIBRBD_OBJECT_MAP_TYPES_H

namespace librbd {
namespace object_map {

enum DiffState {
  DIFF_STATE_NONE    = 0,
  DIFF_STATE_UPDATED = 1,
  DIFF_STATE_HOLE    = 2
};

} // namespace object_map
} // namespace librbd

#endif // CEPH_LIBRBD_OBJECT_MAP_TYPES_H
