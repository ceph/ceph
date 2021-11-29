// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/crypto/CryptoImageDispatch.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ReadResult.h"
#include "librbd/io/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::crypto::CryptoImageDispatch: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace crypto {

using librbd::util::create_context_callback;

template <typename I>
struct C_ReadRequest : public Context {
    I* image_ctx;
    CryptoInterface* crypto;
    io::Extents aligned_extents;
    ceph::bufferlist bl;
    io::ImageDispatchSpec* req;
    io::ReadResult::C_ImageReadRequest* read_ctx;

    C_ReadRequest(I* image_ctx, CryptoInterface* crypto,
                  io::AioCompletion* aio_comp, io::Extents &&image_extents,
                  io::ReadResult &&read_result, IOContext io_context,
                  int op_flags, int read_flags,
                  const ZTracer::Trace &parent_trace
                  ) : image_ctx(image_ctx), crypto(crypto) {
      if (!aio_comp->async_op.started()) {
        aio_comp->start_op();
      }

      aio_comp->read_result = std::move(read_result);
      aio_comp->read_result.set_image_extents(image_extents);
      aio_comp->set_request_count(1);

      read_ctx = new io::ReadResult::C_ImageReadRequest(
              aio_comp, 0, image_extents);

      auto ctx = create_context_callback<C_ReadRequest<I>,
                                         &C_ReadRequest<I>::handle_read>(this);
      auto backing_aio_comp = io::AioCompletion::create_and_start(
              ctx, util::get_image_ctx(image_ctx), io::AIO_TYPE_READ);

      crypto->align_image_extents(image_extents, &aligned_extents);

      auto aligned_extents_copy = aligned_extents;
      req = io::ImageDispatchSpec::create_read(
              *image_ctx, io::IMAGE_DISPATCH_LAYER_CRYPTO, backing_aio_comp,
              std::move(aligned_extents_copy), io::ImageArea::DATA, {&bl},
              io_context, op_flags, read_flags, parent_trace);
    }

    void send() {
      req->send();
    }

    void handle_read(int r) {
      ldout(image_ctx->cct, 20) << "r=" << r << dendl;

      if (r < 0) {
        complete(r);
        return;
      }

      remove_alignment_data();
    }

    void remove_alignment_data() {
      int r = 0;
      for (uint64_t i = 0; i < aligned_extents.size(); ++i) {
        auto& extent = read_ctx->image_extents[i];
        auto& aligned_extent = aligned_extents[i];
        ceph::bufferlist aligned_extent_bl;
        bl.splice(0, aligned_extent.second, &aligned_extent_bl);
        r = crypto->decrypt(&aligned_extent_bl, aligned_extent.first);
        if (r < 0) {
          break;
        }
        uint64_t cut_offset = extent.first - aligned_extent.first;
        aligned_extent_bl.splice(cut_offset, extent.second, &read_ctx->bl);
      }

      complete(r);
    }

    void finish(int r) override {
      ldout(image_ctx->cct, 20) << "r=" << r << dendl;
      if (r < 0) {
        read_ctx->complete(r);
        return;
      }

      r = 0;
      for (auto& extent: read_ctx->image_extents) {
        r += extent.second;
      }
      read_ctx->complete(r);
    }
};

template <typename I>
CryptoImageDispatch<I>::CryptoImageDispatch(
    I* image_ctx, CryptoInterface* crypto)
  : m_image_ctx(image_ctx), m_crypto(crypto) {
}

template <typename I>
bool CryptoImageDispatch<I>::read(
    io::AioCompletion* aio_comp, io::Extents &&image_extents,
    io::ReadResult &&read_result, IOContext io_context, int op_flags,
    int read_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  auto req = new C_ReadRequest<I>(m_image_ctx, m_crypto, aio_comp,
                                  std::move(image_extents),
                                  std::move(read_result), io_context, op_flags,
                                  read_flags, parent_trace);
  req->send();

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
  return true;
}

template <typename I>
bool CryptoImageDispatch<I>::write(
    io::AioCompletion* aio_comp, io::Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  lderr(cct) << dendl;

  fail_io(-EROFS, aio_comp, dispatch_result);
  return true;
}

template <typename I>
bool CryptoImageDispatch<I>::discard(
    io::AioCompletion* aio_comp, io::Extents &&image_extents,
    uint32_t discard_granularity_bytes, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  lderr(cct) << dendl;

  fail_io(-EROFS, aio_comp, dispatch_result);
  return true;
}

template <typename I>
bool CryptoImageDispatch<I>::write_same(
    io::AioCompletion* aio_comp, io::Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  lderr(cct) << dendl;

  fail_io(-EROFS, aio_comp, dispatch_result);
  return true;
}

template <typename I>
bool CryptoImageDispatch<I>::compare_and_write(
    io::AioCompletion* aio_comp, io::Extents &&image_extents,
    bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
    int op_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  lderr(cct) << dendl;

  fail_io(-EROFS, aio_comp, dispatch_result);
  return true;
}

template <typename I>
void CryptoImageDispatch<I>::fail_io(int r, io::AioCompletion* aio_comp,
                                     io::DispatchResult* dispatch_result) {
  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
  aio_comp->fail(r);
}

} // namespace crypto
} // namespace librbd

template class librbd::crypto::CryptoImageDispatch<librbd::ImageCtx>;
