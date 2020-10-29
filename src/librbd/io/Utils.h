// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_UTILS_H
#define CEPH_LIBRBD_IO_UTILS_H

#include "include/int_types.h"
#include "include/buffer_fwd.h"
#include "include/rados/rados_types.hpp"
#include "common/zipkin_trace.h"
#include "librbd/Types.h"
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
void read_parent(ImageCtxT *image_ctx, uint64_t object_no,
                 ReadExtents* extents, librados::snap_t snap_id,
                 const ZTracer::Trace &trace, Context* on_finish);

template <typename ImageCtxT = librbd::ImageCtx>
int clip_request(ImageCtxT *image_ctx, Extents *image_extents);

inline uint64_t get_extents_length(const Extents &extents) {
  uint64_t total_bytes = 0;
  for (auto [_, extent_length] : extents) {
    total_bytes += extent_length;
  }
  return total_bytes;
}

void unsparsify(CephContext* cct, ceph::bufferlist* bl,
                const Extents& extent_map, uint64_t bl_off,
                uint64_t out_bl_len);

template <typename ImageCtxT = librbd::ImageCtx>
bool trigger_copyup(ImageCtxT *image_ctx, uint64_t object_no,
                    IOContext io_context, Context* on_finish);

} // namespace util
} // namespace io
} // namespace librbd

#endif // CEPH_LIBRBD_IO_UTILS_H
