// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OBJECT_MAP_TYPES_H
#define CEPH_LIBRBD_OBJECT_MAP_TYPES_H

namespace librbd {
namespace object_map {

enum DiffState {
  // diff-iterate: hole with or without data captured in intermediate snapshot
  // deep-copy: hole without data captured in intermediate snapshot
  DIFF_STATE_HOLE         = 0,
  // diff-iterate, deep-copy: unchanged data
  DIFF_STATE_DATA         = 1,
  // diff-iterate: new hole (data -> hole)
  // deep-copy: new hole (data -> hole) or hole with data captured in
  //            intermediate snapshot
  DIFF_STATE_HOLE_UPDATED = 2,
  // diff-iterate, deep-copy: new data (hole -> data) or changed data
  DIFF_STATE_DATA_UPDATED = 3
};

} // namespace object_map
} // namespace librbd

#endif // CEPH_LIBRBD_OBJECT_MAP_TYPES_H
