// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/QueueImageDispatch.h"
#include "common/dout.h"
#include "common/Cond.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/FlushTracker.h"
#include "librbd/io/ImageDispatchSpec.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::QueueImageDispatch: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace io {

template <typename I>
QueueImageDispatch<I>::QueueImageDispatch(I* image_ctx)
  : m_image_ctx(image_ctx), m_flush_tracker(new FlushTracker<I>(image_ctx)) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << "ictx=" << image_ctx << dendl;
}

template <typename I>
QueueImageDispatch<I>::~QueueImageDispatch() {
  delete m_flush_tracker;
}

template <typename I>
void QueueImageDispatch<I>::shut_down(Context* on_finish) {
  m_flush_tracker->shut_down();
  on_finish->complete(0);
}

template <typename I>
bool QueueImageDispatch<I>::read(
    AioCompletion* aio_comp, Extents &&image_extents, ReadResult &&read_result,
    IOContext io_context, int op_flags, int read_flags,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  return enqueue(true, tid, dispatch_result, on_finish, on_dispatched);
}

template <typename I>
bool QueueImageDispatch<I>::write(
    AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  return enqueue(false, tid, dispatch_result, on_finish, on_dispatched);
}

template <typename I>
bool QueueImageDispatch<I>::discard(
    AioCompletion* aio_comp, Extents &&image_extents,
    uint32_t discard_granularity_bytes, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  return enqueue(false, tid, dispatch_result, on_finish, on_dispatched);
}

template <typename I>
bool QueueImageDispatch<I>::write_same(
    AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  return enqueue(false, tid, dispatch_result, on_finish, on_dispatched);
}

template <typename I>
bool QueueImageDispatch<I>::compare_and_write(
    AioCompletion* aio_comp, Extents &&image_extents,
    bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  return enqueue(false, tid, dispatch_result, on_finish, on_dispatched);
}

template <typename I>
bool QueueImageDispatch<I>::flush(
    AioCompletion* aio_comp, FlushSource flush_source,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  *dispatch_result = DISPATCH_RESULT_CONTINUE;
  m_flush_tracker->flush(on_dispatched);
  return true;
}

template <typename I>
void QueueImageDispatch<I>::handle_finished(int r, uint64_t tid) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  m_flush_tracker->finish_io(tid);
}

template <typename I>
bool QueueImageDispatch<I>::enqueue(
    bool read_op, uint64_t tid, DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  if (!m_image_ctx->non_blocking_aio) {
    return false;
  }

  if (!read_op) {
    m_flush_tracker->start_io(tid);
    *on_finish = new LambdaContext([this, tid, on_finish=*on_finish](int r) {
        handle_finished(r, tid);
        on_finish->complete(r);
      });
  }

  *dispatch_result = DISPATCH_RESULT_CONTINUE;
  m_image_ctx->asio_engine->post(on_dispatched, 0);
  return true;
}

} // namespace io
} // namespace librbd

template class librbd::io::QueueImageDispatch<librbd::ImageCtx>;
