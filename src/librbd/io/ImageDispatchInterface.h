// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_IMAGE_DISPATCH_INTERFACE_H
#define CEPH_LIBRBD_IO_IMAGE_DISPATCH_INTERFACE_H

#include "include/int_types.h"
#include "include/buffer.h"
#include "common/zipkin_trace.h"
#include "librbd/Types.h"
#include "librbd/io/ReadResult.h"
#include "librbd/io/Types.h"
#include <atomic>

struct Context;

namespace librbd {
namespace io {

struct AioCompletion;
struct ImageDispatchSpec;

struct ImageDispatchInterface {
  typedef ImageDispatchLayer DispatchLayer;
  typedef ImageDispatchSpec DispatchSpec;

  virtual ~ImageDispatchInterface() {
  }

  virtual ImageDispatchLayer get_dispatch_layer() const = 0;

  virtual void shut_down(Context* on_finish) = 0;

  virtual bool read(
      AioCompletion* aio_comp, Extents &&image_extents,
      ReadResult &&read_result, IOContext io_context, int op_flags,
      int read_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) = 0;
  virtual bool write(
      AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
      int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) = 0;
  virtual bool discard(
      AioCompletion* aio_comp, Extents &&image_extents,
      uint32_t discard_granularity_bytes, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) = 0;
  virtual bool write_same(
      AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
      int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) = 0;
  virtual bool compare_and_write(
      AioCompletion* aio_comp, Extents &&image_extents,
      bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
      int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) = 0;
  virtual bool flush(
      AioCompletion* aio_comp, FlushSource flush_source,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) = 0;

  virtual bool list_snaps(
      AioCompletion* aio_comp, Extents&& image_extents, SnapIds&& snap_ids,
      int list_snaps_flags, SnapshotDelta* snapshot_delta,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) = 0;

  virtual bool invalidate_cache(Context* on_finish) = 0;
};

} // namespace io
} // namespace librbd

#endif // CEPH_LIBRBD_IO_IMAGE_DISPATCH_INTERFACE_H
