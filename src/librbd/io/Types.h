// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_TYPES_H
#define CEPH_LIBRBD_IO_TYPES_H

#include "include/int_types.h"
#include <map>
#include <vector>

namespace librbd {
namespace io {

typedef enum {
  AIO_TYPE_NONE = 0,
  AIO_TYPE_GENERIC,
  AIO_TYPE_OPEN,
  AIO_TYPE_CLOSE,
  AIO_TYPE_READ,
  AIO_TYPE_WRITE,
  AIO_TYPE_DISCARD,
  AIO_TYPE_FLUSH,
  AIO_TYPE_WRITESAME,
} aio_type_t;

enum Direction {
  DIRECTION_READ,
  DIRECTION_WRITE,
  DIRECTION_BOTH
};

typedef std::vector<std::pair<uint64_t, uint64_t> > Extents;
typedef std::map<uint64_t, uint64_t> ExtentMap;

} // namespace io
} // namespace librbd

#endif // CEPH_LIBRBD_IO_TYPES_H

