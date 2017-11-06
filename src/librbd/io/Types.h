// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_TYPES_H
#define CEPH_LIBRBD_IO_TYPES_H

#include "include/int_types.h"
#include <map>
#include <vector>

namespace librbd {
namespace io {

#define RBD_QOS_IOPS_THROTTLE				1 << 0
#define RBD_QOS_BPS_THROTTLE				1 << 1
#define RBD_QOS_READ_IOPS_THROTTLE			1 << 2
#define RBD_QOS_WRITE_IOPS_THROTTLE			1 << 3
#define RBD_QOS_READ_BPS_THROTTLE			1 << 4
#define RBD_QOS_WRITE_BPS_THROTTLE			1 << 5

#define RBD_QOS_BPS_MASK	(RBD_QOS_BPS_THROTTLE | RBD_QOS_READ_BPS_THROTTLE | RBD_QOS_WRITE_BPS_THROTTLE)
#define RBD_QOS_IOPS_MASK	(RBD_QOS_IOPS_THROTTLE | RBD_QOS_READ_IOPS_THROTTLE | RBD_QOS_WRITE_IOPS_THROTTLE)
#define RBD_QOS_READ_MASK	(RBD_QOS_READ_BPS_THROTTLE | RBD_QOS_READ_IOPS_THROTTLE)
#define RBD_QOS_WRITE_MASK	(RBD_QOS_WRITE_BPS_THROTTLE | RBD_QOS_WRITE_IOPS_THROTTLE)

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
  AIO_TYPE_COMPARE_AND_WRITE,
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

