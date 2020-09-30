// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_PWL_IMAGE_DISPATCH_H
#define CEPH_LIBRBD_PWL_IMAGE_DISPATCH_H

#include "librbd/io/ImageDispatchInterface.h"
#include "include/int_types.h"
#include "include/buffer.h"
#include "common/zipkin_trace.h"
#include "librbd/io/ReadResult.h"
#include "librbd/io/Types.h"

struct Context;

namespace librbd {

struct ImageCtx;

namespace cache {

template<typename>
class WriteLogCache;

namespace pwl {

template <typename ImageCtxT>
class ImageDispatch : public io::ImageDispatchInterface {
public:
  ImageDispatch(ImageCtxT* image_ctx,
                   WriteLogCache<ImageCtx> *image_cache) :
    m_image_ctx(image_ctx), m_image_cache(image_cache) {
  }

  io::ImageDispatchLayer get_dispatch_layer() const override {
    return io::IMAGE_DISPATCH_LAYER_WRITEBACK_CACHE;
  }

  void shut_down(Context* on_finish) override;

  bool read(
      io::AioCompletion* aio_comp, io::Extents &&image_extents,
      io::ReadResult &&read_result, IOContext io_context,
      int op_flags, int read_flags,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
    bool write(
      io::AioCompletion* aio_comp, io::Extents &&image_extents, bufferlist &&bl,
      IOContext io_context, int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) override;
  bool discard(
      io::AioCompletion* aio_comp, io::Extents &&image_extents,
      uint32_t discard_granularity_bytes, IOContext io_context,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) override;
  bool write_same(
      io::AioCompletion* aio_comp, io::Extents &&image_extents, bufferlist &&bl,
      IOContext io_context, int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) override;
  bool compare_and_write(
      io::AioCompletion* aio_comp, io::Extents &&image_extents,
      bufferlist &&cmp_bl,
      bufferlist &&bl, uint64_t *mismatch_offset,
      IOContext io_context, int op_flags,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) override;
  bool flush(
      io::AioCompletion* aio_comp, io::FlushSource flush_source,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result,
      Context** on_finish, Context* on_dispatched) override;
  bool list_snaps(
    io::AioCompletion* aio_comp, io::Extents&& image_extents,
    io::SnapIds&& snap_ids, int list_snaps_flags,
    io::SnapshotDelta* snapshot_delta,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) override;

private:
  ImageCtxT* m_image_ctx;
  cache::WriteLogCache<ImageCtx> *m_image_cache;

  bool preprocess_length(
      io::AioCompletion* aio_comp, io::Extents &image_extents) const;
};

} // namespace pwl
} // namespace cache
} // namespace librbd

extern template class librbd::cache::pwl::ImageDispatch<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_PWL_IMAGE_DISPATCH_H
