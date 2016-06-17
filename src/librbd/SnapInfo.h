// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_SNAPINFO_H
#define CEPH_LIBRBD_SNAPINFO_H

#include "include/int_types.h"

#include "librbd/parent_types.h"

namespace librbd {

  struct SnapInfo {
    std::string name;
    uint64_t size;
    parent_info parent;
    uint8_t protection_status;
    uint64_t flags;
    SnapInfo(std::string _name, uint64_t _size, parent_info _parent,
             uint8_t _protection_status, uint64_t _flags)
      : name(_name), size(_size), parent(_parent),
	protection_status(_protection_status), flags(_flags) {}
  };
}

#endif
