// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_IO_IMAGE_DISPATCH_H
#define CEPH_TEST_LIBRBD_MOCK_IO_IMAGE_DISPATCH_H

#include "gmock/gmock.h"
#include "include/Context.h"
#include "librbd/io/ImageDispatchInterface.h"
#include "librbd/io/Types.h"

class Context;

namespace librbd {
namespace io {

struct MockImageDispatch : public ImageDispatchInterface {
public:
  MOCK_CONST_METHOD0(get_dispatch_layer, ImageDispatchLayer());

  MOCK_METHOD1(shut_down, void(Context*));

  bool read(
      AioCompletion* aio_comp, Extents &&image_extents,
      ReadResult &&read_result, IOContext io_context, int op_flags,
      int read_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override {
    return false;
  }

  bool write(
      AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
      int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override {
    return false;
  }

  bool discard(
      AioCompletion* aio_comp, Extents &&image_extents,
      uint32_t discard_granularity_bytes, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override {
    return false;
  }

  bool write_same(
      AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
      int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override {
    return false;
  }

  bool compare_and_write(
      AioCompletion* aio_comp, Extents &&image_extents,
      bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
      int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override {
    return false;
  }

  bool flush(
      AioCompletion* aio_comp, FlushSource flush_source,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override {
    return false;
  }

  bool list_snaps(
      AioCompletion* aio_comp, Extents&& image_extents, SnapIds&& snap_ids,
      int list_snaps_flags, SnapshotDelta* snapshot_delta,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override {
    return false;
  }

  bool invalidate_cache(Context* on_finish) override {
    return false;
  }

};

} // namespace io
} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_IO_IMAGE_DISPATCH_H
