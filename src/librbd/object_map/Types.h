// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OBJECT_MAP_TYPES_H
#define CEPH_LIBRBD_OBJECT_MAP_TYPES_H

namespace librbd {
namespace object_map {

enum DiffState {
  DIFF_STATE_HOLE         = 0, /* unchanged hole */
  DIFF_STATE_DATA         = 1, /* unchanged data */
  DIFF_STATE_HOLE_UPDATED = 2, /* new hole */
  DIFF_STATE_DATA_UPDATED = 3  /* new data */
};

} // namespace object_map
} // namespace librbd

#endif // CEPH_LIBRBD_OBJECT_MAP_TYPES_H
