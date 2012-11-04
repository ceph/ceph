// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_SNAPINFO_H
#define CEPH_LIBRBD_SNAPINFO_H

#include <inttypes.h>

#include "include/rados/librados.hpp"

#include "cls/rbd/cls_rbd_client.h"
#include "librbd/parent_types.h"

namespace librbd {

  struct SnapInfo {
    librados::snap_t id;
    uint64_t size;
    uint64_t features;
    parent_info parent;
    uint8_t protection_status;
    SnapInfo(librados::snap_t _id, uint64_t _size, uint64_t _features,
	     parent_info _parent, uint8_t _protection_status) :
      id(_id), size(_size), features(_features), parent(_parent),
      protection_status(_protection_status) {}
  };
}

#endif
