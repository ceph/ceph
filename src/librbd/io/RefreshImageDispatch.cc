// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/RefreshImageDispatch.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include <map>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::RefreshImageDispatch: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace io {

template <typename I>
RefreshImageDispatch<I>::RefreshImageDispatch(I* image_ctx)
  : m_image_ctx(image_ctx) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << "ictx=" << image_ctx << dendl;
}

template <typename I>
void RefreshImageDispatch<I>::shut_down(Context* on_finish) {
  on_finish->complete(0);
}

template <typename I>
bool RefreshImageDispatch<I>::read(
    AioCompletion* aio_comp, Extents &&image_extents, ReadResult &&read_result,
    IOContext io_context, int op_flags, int read_flags,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << ", image_extents=" << image_extents
                 << dendl;

  if (needs_refresh(dispatch_result, on_dispatched)) {
    return true;
  }

  return false;
}

template <typename I>
bool RefreshImageDispatch<I>::write(
    AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << ", image_extents=" << image_extents
                 << dendl;

  if (needs_refresh(dispatch_result, on_dispatched)) {
    return true;
  }

  return false;
}

template <typename I>
bool RefreshImageDispatch<I>::discard(
    AioCompletion* aio_comp, Extents &&image_extents,
    uint32_t discard_granularity_bytes, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << ", image_extents=" << image_extents
                 << dendl;

  if (needs_refresh(dispatch_result, on_dispatched)) {
    return true;
  }

  return false;
}

template <typename I>
bool RefreshImageDispatch<I>::write_same(
    AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << ", image_extents=" << image_extents
                 << dendl;

  if (needs_refresh(dispatch_result, on_dispatched)) {
    return true;
  }

  return false;
}

template <typename I>
bool RefreshImageDispatch<I>::compare_and_write(
    AioCompletion* aio_comp, Extents &&image_extents,
    bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << ", image_extents=" << image_extents
                 << dendl;

  if (needs_refresh(dispatch_result, on_dispatched)) {
    return true;
  }

  return false;
}

template <typename I>
bool RefreshImageDispatch<I>::flush(
    AioCompletion* aio_comp, FlushSource flush_source,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  // The refresh state machine can initiate a flush and it can
  // enable the exclusive-lock which will also attempt to flush.
  if (flush_source == FLUSH_SOURCE_REFRESH ||
      flush_source == FLUSH_SOURCE_EXCLUSIVE_LOCK_SKIP_REFRESH ||
      flush_source == FLUSH_SOURCE_SHUTDOWN) {
    return false;
  }

  if (needs_refresh(dispatch_result, on_dispatched)) {
    return true;
  }

  return false;
}

template <typename I>
bool RefreshImageDispatch<I>::needs_refresh(
    DispatchResult* dispatch_result, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;

  if (m_image_ctx->state->is_refresh_required()) {
    ldout(cct, 15) << "on_dispatched=" << on_dispatched << dendl;

    *dispatch_result = DISPATCH_RESULT_CONTINUE;
    m_image_ctx->state->refresh(on_dispatched);
    return true;
  }

  return false;
}

} // namespace io
} // namespace librbd

template class librbd::io::RefreshImageDispatch<librbd::ImageCtx>;
