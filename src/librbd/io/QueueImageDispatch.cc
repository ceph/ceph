// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/QueueImageDispatch.h"
#include "common/dout.h"
#include "common/Cond.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatchSpec.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::QueueImageDispatch: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace io {

template <typename I>
QueueImageDispatch<I>::QueueImageDispatch(I* image_ctx)
  : m_image_ctx(image_ctx) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << "ictx=" << image_ctx << dendl;
}

template <typename I>
void QueueImageDispatch<I>::shut_down(Context* on_finish) {
  on_finish->complete(0);
}

template <typename I>
bool QueueImageDispatch<I>::read(
    AioCompletion* aio_comp, Extents &&image_extents, ReadResult &&read_result,
    int op_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  return enqueue(dispatch_result, on_dispatched);
}

template <typename I>
bool QueueImageDispatch<I>::write(
    AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  return enqueue(dispatch_result, on_dispatched);
}

template <typename I>
bool QueueImageDispatch<I>::discard(
    AioCompletion* aio_comp, Extents &&image_extents,
    uint32_t discard_granularity_bytes, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  return enqueue(dispatch_result, on_dispatched);
}

template <typename I>
bool QueueImageDispatch<I>::write_same(
    AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  return enqueue(dispatch_result, on_dispatched);
}

template <typename I>
bool QueueImageDispatch<I>::compare_and_write(
    AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&cmp_bl,
    bufferlist &&bl, uint64_t *mismatch_offset, int op_flags,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  return enqueue(dispatch_result, on_dispatched);
}

template <typename I>
bool QueueImageDispatch<I>::flush(
    AioCompletion* aio_comp, FlushSource flush_source,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  if (flush_source != FLUSH_SOURCE_USER) {
    return false;
  }

  return enqueue(dispatch_result, on_dispatched);
}

template <typename I>
bool QueueImageDispatch<I>::enqueue(
    DispatchResult* dispatch_result, Context* on_dispatched) {
  if (!m_image_ctx->non_blocking_aio) {
    return false;
  }

  *dispatch_result = DISPATCH_RESULT_CONTINUE;
  m_image_ctx->op_work_queue->queue(on_dispatched, 0);
  return true;
}

} // namespace io
} // namespace librbd

template class librbd::io::QueueImageDispatch<librbd::ImageCtx>;
