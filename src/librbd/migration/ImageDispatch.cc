// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/ImageDispatch.h"
#include "include/neorados/RADOS.hpp"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/crypto/CryptoInterface.h"
#include "librbd/crypto/EncryptionFormat.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/Utils.h"
#include "librbd/migration/FormatInterface.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::ImageDispatch: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace migration {

namespace {

struct C_DecryptData : public io::ReadResult::C_ImageReadRequest {
  crypto::CryptoInterface* crypto;

  C_DecryptData(io::AioCompletion* aio_comp, const io::Extents& image_extents,
                crypto::CryptoInterface* crypto)
      : C_ImageReadRequest(aio_comp, 0, image_extents), crypto(crypto) {}

  void finish(int r) override {
    if (r < 0) {
      C_ImageReadRequest::finish(r);
      return;
    }

    auto ciphertext_bl = std::move(bl);
    for (const auto& extent : image_extents) {
      ceph::bufferlist tmp;
      ciphertext_bl.splice(0, extent.second, &tmp);
      int r = crypto->decrypt(&tmp, extent.first);
      if (r < 0) {
        C_ImageReadRequest::finish(r);
        return;
      }
      bl.claim_append(tmp);
    }

    C_ImageReadRequest::finish(0);
  }
};

template <typename I>
struct C_MapSnapshotDelta : public io::C_AioRequest {
  io::SnapshotDelta* snapshot_delta;
  I* image_ctx;

  C_MapSnapshotDelta(io::AioCompletion* aio_comp,
                     io::SnapshotDelta* snapshot_delta, I* image_ctx)
      : C_AioRequest(aio_comp), snapshot_delta(snapshot_delta),
        image_ctx(image_ctx) {}

  void finish(int r) override {
    if (r < 0) {
      C_AioRequest::finish(r);
      return;
    }

    auto raw_snapshot_delta = std::move(*snapshot_delta);
    for (const auto& [key, raw_sparse_extents] : raw_snapshot_delta) {
      auto& sparse_extents = (*snapshot_delta)[key];
      for (const auto& raw_sparse_extent : raw_sparse_extents) {
        auto off = io::util::raw_to_area_offset(*image_ctx,
                                                raw_sparse_extent.get_off());
        ceph_assert(off.second == io::ImageArea::DATA);
        sparse_extents.insert(off.first, raw_sparse_extent.get_len(),
                              {raw_sparse_extent.get_val().state,
                               raw_sparse_extent.get_len()});
      }
    }

    C_AioRequest::finish(0);
  }
};

} // anonymous namespace

template <typename I>
ImageDispatch<I>::ImageDispatch(I* image_ctx,
                                std::unique_ptr<FormatInterface> format)
  : m_image_ctx(image_ctx), m_format(std::move(format)) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << "ictx=" << image_ctx << dendl;
}

template <typename I>
void ImageDispatch<I>::shut_down(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 10) << dendl;

  on_finish->complete(0);
}

template <typename I>
bool ImageDispatch<I>::read(
    io::AioCompletion* aio_comp, io::Extents &&image_extents,
    io::ReadResult &&read_result, IOContext io_context, int op_flags,
    int read_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  // let io::ImageDispatch layer (IMAGE_DISPATCH_LAYER_CORE) handle
  // native format
  if (!m_format) {
    return false;
  }

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;

  if (m_image_ctx->encryption_format != nullptr &&
      (*image_dispatch_flags & io::IMAGE_DISPATCH_FLAG_CRYPTO_HEADER) == 0) {
    auto crypto = m_image_ctx->encryption_format->get_crypto();

    // alignment should be performed on the destination image (see
    // C_UnalignedObjectReadRequest in CryptoObjectDispatch)
    for (const auto& extent : image_extents) {
      ceph_assert(crypto->is_aligned(extent.first, extent.second));
    }

    aio_comp->read_result = std::move(read_result);
    aio_comp->read_result.set_image_extents(image_extents);

    aio_comp->set_request_count(1);
    auto ctx = new C_DecryptData(aio_comp, image_extents, crypto);
    aio_comp = io::AioCompletion::create_and_start<Context>(
        ctx, util::get_image_ctx(m_image_ctx), io::AIO_TYPE_READ);
    read_result = io::ReadResult(&ctx->bl);

    // map to raw image extents _after_ DATA area extents are captured
    for (auto& extent : image_extents) {
      extent.first = io::util::area_to_raw_offset(*m_image_ctx, extent.first,
                                                  io::ImageArea::DATA);
    }
  }

  m_format->read(aio_comp, io_context->get_read_snap(),
                 std::move(image_extents), std::move(read_result),
                 op_flags, read_flags, parent_trace);
  return true;
}

template <typename I>
bool ImageDispatch<I>::write(
    io::AioCompletion* aio_comp, io::Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  lderr(cct) << dendl;

  fail_io(-EROFS, aio_comp, dispatch_result);
  return true;
}

template <typename I>
bool ImageDispatch<I>::discard(
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
bool ImageDispatch<I>::write_same(
    io::AioCompletion* aio_comp, io::Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  lderr(cct) << dendl;

  fail_io(-EROFS, aio_comp, dispatch_result);
  return true;
}

template <typename I>
bool ImageDispatch<I>::compare_and_write(
    io::AioCompletion* aio_comp, io::Extents &&image_extents,
    bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  lderr(cct) << dendl;

  fail_io(-EROFS, aio_comp, dispatch_result);
  return true;
}

template <typename I>
bool ImageDispatch<I>::flush(
    io::AioCompletion* aio_comp, io::FlushSource flush_source,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
  aio_comp->set_request_count(0);
  return true;
}

template <typename I>
bool ImageDispatch<I>::list_snaps(
    io::AioCompletion* aio_comp, io::Extents&& image_extents,
    io::SnapIds&& snap_ids, int list_snaps_flags,
    io::SnapshotDelta* snapshot_delta, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  // let io::ImageDispatch layer (IMAGE_DISPATCH_LAYER_CORE) handle
  // native format
  if (!m_format) {
    return false;
  }

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;

  aio_comp->set_request_count(1);
  Context* ctx;

  if (m_image_ctx->encryption_format != nullptr &&
      (*image_dispatch_flags & io::IMAGE_DISPATCH_FLAG_CRYPTO_HEADER) == 0) {
    // map to raw image extents
    for (auto& extent : image_extents) {
      extent.first = io::util::area_to_raw_offset(*m_image_ctx, extent.first,
                                                  io::ImageArea::DATA);
    }
    // ... and back on completion
    ctx = new C_MapSnapshotDelta(aio_comp, snapshot_delta, m_image_ctx);
  } else {
    ctx = new io::C_AioRequest(aio_comp);
  }

  m_format->list_snaps(std::move(image_extents), std::move(snap_ids),
                       list_snaps_flags, snapshot_delta, parent_trace,
                       ctx);
  return true;
}

template <typename I>
void ImageDispatch<I>::fail_io(int r, io::AioCompletion* aio_comp,
                               io::DispatchResult* dispatch_result) {
  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
  aio_comp->fail(r);
}

} // namespace migration
} // namespace librbd

template class librbd::migration::ImageDispatch<librbd::ImageCtx>;
