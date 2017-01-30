// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_SNAPINFO_H
#define CEPH_LIBRBD_SNAPINFO_H

#include "cls/rbd/cls_rbd_types.h"
#include "include/int_types.h"

#include "librbd/parent_types.h"

namespace librbd {

  struct SnapInfo {
    std::string name;
    cls::rbd::SnapshotNamespace snap_namespace;
    uint64_t size;
    parent_info parent;
    uint8_t protection_status;
    uint64_t flags;
    utime_t timestamp;
    SnapInfo(std::string _name, const cls::rbd::SnapshotNamespace &_snap_namespace,
	     uint64_t _size, parent_info _parent,
             uint8_t _protection_status, uint64_t _flags, utime_t _timestamp)
      : name(_name), snap_namespace(_snap_namespace), size(_size), parent(_parent),
	protection_status(_protection_status), flags(_flags), timestamp(_timestamp) {}
  };
}

#endif
