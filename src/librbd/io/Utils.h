// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_UTILS_H
#define CEPH_LIBRBD_IO_UTILS_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/rados/rados_types.hpp"
#include "common/zipkin_trace.h"
#include "librbd/io/Types.h"
#include <map>

class ObjectExtent;

namespace neorados { struct Op; }

namespace librbd {

struct ImageCtx;

namespace io {
namespace util {

void apply_op_flags(uint32_t op_flags, uint32_t flags, neorados::Op* op);

bool assemble_write_same_extent(const LightweightObjectExtent &object_extent,
                                const ceph::bufferlist& data,
                                ceph::bufferlist *ws_data,
                                bool force_write);

template <typename ImageCtxT = librbd::ImageCtx>
void read_parent(ImageCtxT *image_ctx, uint64_t object_no, uint64_t off,
                 uint64_t len, librados::snap_t snap_id,
                 const ZTracer::Trace &trace, ceph::bufferlist* data,
                 Context* on_finish);

} // namespace util
} // namespace io
} // namespace librbd

#endif // CEPH_LIBRBD_IO_UTILS_H
