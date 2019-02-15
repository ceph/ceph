// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_UTILS_H
#define CEPH_LIBRBD_IO_UTILS_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include <map>

class ObjectExtent;

namespace librbd {
namespace io {
namespace util {

bool assemble_write_same_extent(const ObjectExtent &object_extent,
                                const ceph::bufferlist& data,
                                ceph::bufferlist *ws_data,
                                bool force_write);

} // namespace util
} // namespace io
} // namespace librbd

#endif // CEPH_LIBRBD_IO_UTILS_H
