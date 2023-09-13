// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_DEEP_COPY_TYPES_H
#define CEPH_LIBRBD_DEEP_COPY_TYPES_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include <boost/optional.hpp>

namespace librbd {
namespace deep_copy {

enum {
  OBJECT_COPY_REQUEST_FLAG_FLATTEN      = 1U << 0,
  OBJECT_COPY_REQUEST_FLAG_MIGRATION    = 1U << 1,
  OBJECT_COPY_REQUEST_FLAG_EXISTS_CLEAN = 1U << 2,
};

typedef std::vector<librados::snap_t> SnapIds;
typedef std::map<librados::snap_t, SnapIds> SnapMap;

typedef boost::optional<uint64_t> ObjectNumber;

} // namespace deep_copy
} // namespace librbd

#endif // CEPH_LIBRBD_DEEP_COPY_TYPES_H
