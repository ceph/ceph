// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/migration/ImageDispatch.h"
#include "include/neorados/RADOS.hpp"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/migration/FormatInterface.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::migration::ImageDispatch: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace migration {

template <typename I>
ImageDispatch<I>::ImageDispatch(I* image_ctx,
                                std::unique_ptr<FormatInterface<I>> format)
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

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
  return m_format->read(aio_comp, io_context->get_read_snap(),
                        std::move(image_extents), std::move(read_result),
                        op_flags, read_flags, parent_trace);
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

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;

  aio_comp->set_request_count(1);
  auto ctx = new io::C_AioRequest(aio_comp);

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
