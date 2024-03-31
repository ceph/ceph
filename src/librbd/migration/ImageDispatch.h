// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIGRATION_IMAGE_DISPATCH_H
#define CEPH_LIBRBD_MIGRATION_IMAGE_DISPATCH_H

#include "librbd/io/ImageDispatchInterface.h"
#include <memory>

struct Context;

namespace librbd {

struct ImageCtx;

namespace migration {

struct FormatInterface;

template <typename ImageCtxT>
class ImageDispatch : public io::ImageDispatchInterface {
public:
  static ImageDispatch* create(ImageCtxT* image_ctx,
                               std::unique_ptr<FormatInterface> source) {
    return new ImageDispatch(image_ctx, std::move(source));
  }

  ImageDispatch(ImageCtxT* image_ctx, std::unique_ptr<FormatInterface> source);

  void shut_down(Context* on_finish) override;

  io::ImageDispatchLayer get_dispatch_layer() const override {
    return io::IMAGE_DISPATCH_LAYER_MIGRATION;
  }

  bool read(
      io::AioCompletion* aio_comp, io::Extents &&image_extents,
      io::ReadResult &&read_result, IOContext io_context, int op_flags,
      int read_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
  bool write(
      io::AioCompletion* aio_comp, io::Extents &&image_extents, bufferlist &&bl,
      int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
  bool discard(
      io::AioCompletion* aio_comp, io::Extents &&image_extents,
      uint32_t discard_granularity_bytes, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
  bool write_same(
      io::AioCompletion* aio_comp, io::Extents &&image_extents, bufferlist &&bl,
      int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
  bool compare_and_write(
      io::AioCompletion* aio_comp, io::Extents &&image_extents,
      bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
      int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;
  bool flush(
      io::AioCompletion* aio_comp, io::FlushSource flush_source,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;

  bool list_snaps(
      io::AioCompletion* aio_comp, io::Extents&& image_extents,
      io::SnapIds&& snap_ids, int list_snaps_flags,
      io::SnapshotDelta* snapshot_delta, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;

  bool invalidate_cache(Context* on_finish) override {
    return false;
  }

private:
  ImageCtxT* m_image_ctx;
  std::unique_ptr<FormatInterface> m_format;

  void fail_io(int r, io::AioCompletion* aio_comp,
               io::DispatchResult* dispatch_result);

};

} // namespace migration
} // namespace librbd

extern template class librbd::migration::ImageDispatch<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIGRATION_IMAGE_DISPATCH_H
