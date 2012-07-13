// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_SNAPINFO_H
#define CEPH_LIBRBD_SNAPINFO_H

#include <inttypes.h>

#include "include/rados/librados.hpp"

#include "librbd/cls_rbd_client.h"

namespace librbd {

  struct SnapInfo {
    librados::snap_t id;
    uint64_t size;
    uint64_t features;
    cls_client::parent_info parent;
    SnapInfo(librados::snap_t _id, uint64_t _size, uint64_t _features,
	     cls_client::parent_info _parent) :
      id(_id), size(_size), features(_features), parent(_parent) {}
  };
}

#endif
