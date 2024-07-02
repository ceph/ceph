// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CRYPTO_CRYPTO_IMAGE_DISPATCH_H
#define CEPH_LIBRBD_CRYPTO_CRYPTO_IMAGE_DISPATCH_H

#include "librbd/crypto/CryptoInterface.h"
#include "librbd/io/ImageDispatchInterface.h"

namespace librbd {

struct ImageCtx;

namespace crypto {

template <typename ImageCtxT = librbd::ImageCtx>
class CryptoImageDispatch : public io::ImageDispatchInterface {
public:
  static CryptoImageDispatch* create(
          ImageCtxT* image_ctx, CryptoInterface* crypto) {
    return new CryptoImageDispatch(image_ctx, crypto);
  }
  CryptoImageDispatch(ImageCtxT* image_ctx,
                      CryptoInterface* crypto);

  io::ImageDispatchLayer get_dispatch_layer() const override {
    return io::IMAGE_DISPATCH_LAYER_CRYPTO;
  }

  void shut_down(Context* on_finish) override {
    on_finish->complete(0);
  }

  bool read(
      io::AioCompletion* aio_comp, io::Extents &&image_extents,
      io::ReadResult &&read_result, IOContext io_context, int op_flags,
      int read_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;

  bool write(
      io::AioCompletion* aio_comp, io::Extents &&image_extents,
      bufferlist &&bl, int op_flags, const ZTracer::Trace &parent_trace,
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
      io::AioCompletion* aio_comp, io::Extents &&image_extents,
      bufferlist &&bl, int op_flags, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;

  bool compare_and_write(
      io::AioCompletion* aio_comp, io::Extents &&image_extents,
      bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
      int op_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override;

  bool flush(
      io::AioCompletion* aio_comp, io::FlushSource flush_source,
      const ZTracer::Trace &parent_trace, uint64_t tid,
      std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override {
    return false;
  }

  bool list_snaps(
      io::AioCompletion* aio_comp, io::Extents&& image_extents,
      io::SnapIds&& snap_ids, int list_snaps_flags,
      io::SnapshotDelta* snapshot_delta, const ZTracer::Trace &parent_trace,
      uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
      io::DispatchResult* dispatch_result, Context** on_finish,
      Context* on_dispatched) override {
    return false;
  }

  bool invalidate_cache(Context* on_finish) override {
    return false;
  }

private:
  void fail_io(int r, io::AioCompletion* aio_comp,
               io::DispatchResult* dispatch_result);

  ImageCtxT* m_image_ctx;
  CryptoInterface* m_crypto;
};

} // namespace crypto
} // namespace librbd

extern template class librbd::crypto::CryptoImageDispatch<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CRYPTO_CRYPTO_IMAGE_DISPATCH_H
