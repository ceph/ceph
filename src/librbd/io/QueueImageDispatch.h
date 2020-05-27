// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IO_QUEUE_IMAGE_DISPATCH_H
#define CEPH_LIBRBD_IO_QUEUE_IMAGE_DISPATCH_H

#include "librbd/io/ImageDispatchInterface.h"
#include "include/int_types.h"
#include "include/buffer.h"
#include "common/zipkin_trace.h"
#include "common/Throttle.h"
#include "librbd/io/ReadResult.h"
#include "librbd/io/Types.h"
#include <list>
#include <set>

struct Context;

namespace librbd {

struct ImageCtx;

namespace io {

struct AioCompletion;

template <typename ImageCtxT>
class QueueImageDispatch : public ImageDispatchInterface {
public:
  QueueImageDispatch(ImageCtxT* image_ctx);

  ImageDispatchLayer get_dispatch_layer() const override {
    return IMAGE_DISPATCH_LAYER_QUEUE;
  }

  void shut_down(Context* on_finish) override;

  bool read(
      AioCompletion* aio_comp, Extents &&image_extents,
      ReadResult &&read_result, int op_flags,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context* on_dispatched) override;
  bool write(
      AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
      int op_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context* on_dispatched) override;
  bool discard(
      AioCompletion* aio_comp, Extents &&image_extents,
      uint32_t discard_granularity_bytes,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context* on_dispatched) override;
  bool write_same(
      AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
      int op_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context* on_dispatched) override;
  bool compare_and_write(
      AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&cmp_bl,
      bufferlist &&bl, uint64_t *mismatch_offset, int op_flags,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context* on_dispatched) override;
  bool flush(
      AioCompletion* aio_comp, FlushSource flush_source,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      DispatchResult* dispatch_result, Context* on_dispatched) override;

  void handle_finished(int r, uint64_t tid) override {
  }

private:
  ImageCtxT* m_image_ctx;

  bool enqueue(DispatchResult* dispatch_result, Context* on_dispatched);

};

} // namespace io
} // namespace librbd

extern template class librbd::io::QueueImageDispatch<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IO_QUEUE_IMAGE_DISPATCH_H
