// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_PARENT_TYPES_H
#define CEPH_LIBRBD_PARENT_TYPES_H

// parent_spec uniquely identifies a parent in the clone relationship
// (clone(parent) creates child, then parent_spec <-> child_imageid)

namespace librbd {
  struct parent_spec {
    int64_t pool_id;
    string image_id;
    snapid_t snap_id;
    parent_spec() : pool_id(-1), snap_id(CEPH_NOSNAP) {}
    parent_spec(uint64_t pool_id, string image_id, snapid_t snap_id) :
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

  struct parent_info {
    parent_spec spec;
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
