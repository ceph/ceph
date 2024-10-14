// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/ImageDispatch.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ObjectDispatcherInterface.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::ImageDispatch: " << this << " " \
                           << __func__ << ": "

namespace librbd {
namespace io {

namespace {

void start_in_flight_io(AioCompletion* aio_comp) {
  // TODO remove AsyncOperation from AioCompletion
  if (!aio_comp->async_op.started()) {
    aio_comp->start_op();
  }
}

ImageArea get_area(const std::atomic<uint32_t>* image_dispatch_flags) {
  return (*image_dispatch_flags & IMAGE_DISPATCH_FLAG_CRYPTO_HEADER ?
      ImageArea::CRYPTO_HEADER : ImageArea::DATA);
}

} // anonymous namespace

template <typename I>
void ImageDispatch<I>::shut_down(Context* on_finish) {
  on_finish->complete(0);
}

template <typename I>
bool ImageDispatch<I>::read(
    AioCompletion* aio_comp, Extents &&image_extents, ReadResult &&read_result,
    IOContext io_context, int op_flags, int read_flags,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  auto area = get_area(image_dispatch_flags);
  ldout(cct, 20) << "image_extents=" << image_extents
                 << " area=" << area << dendl;

  start_in_flight_io(aio_comp);

  *dispatch_result = DISPATCH_RESULT_COMPLETE;
  ImageRequest<I>::aio_read(m_image_ctx, aio_comp, std::move(image_extents),
                            area, std::move(read_result), io_context, op_flags,
                            read_flags, parent_trace);
  return true;
}

template <typename I>
bool ImageDispatch<I>::write(
    AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  auto area = get_area(image_dispatch_flags);
  ldout(cct, 20) << "image_extents=" << image_extents
                 << " area=" << area << dendl;

  start_in_flight_io(aio_comp);

  *dispatch_result = DISPATCH_RESULT_COMPLETE;
  ImageRequest<I>::aio_write(m_image_ctx, aio_comp, std::move(image_extents),
                             area, std::move(bl), op_flags, parent_trace);
  return true;
}

template <typename I>
bool ImageDispatch<I>::discard(
    AioCompletion* aio_comp, Extents &&image_extents,
    uint32_t discard_granularity_bytes, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  auto area = get_area(image_dispatch_flags);
  ldout(cct, 20) << "image_extents=" << image_extents
                 << " area=" << area << dendl;

  start_in_flight_io(aio_comp);

  *dispatch_result = DISPATCH_RESULT_COMPLETE;
  ImageRequest<I>::aio_discard(m_image_ctx, aio_comp, std::move(image_extents),
                               area, discard_granularity_bytes, parent_trace);
  return true;
}

template <typename I>
bool ImageDispatch<I>::write_same(
    AioCompletion* aio_comp, Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  auto area = get_area(image_dispatch_flags);
  ldout(cct, 20) << "image_extents=" << image_extents
                 << " area=" << area << dendl;

  start_in_flight_io(aio_comp);

  *dispatch_result = DISPATCH_RESULT_COMPLETE;
  ImageRequest<I>::aio_writesame(m_image_ctx, aio_comp,
                                 std::move(image_extents), area, std::move(bl),
                                 op_flags, parent_trace);
  return true;
}

template <typename I>
bool ImageDispatch<I>::compare_and_write(
    AioCompletion* aio_comp, Extents &&image_extents,
    bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  auto area = get_area(image_dispatch_flags);
  ldout(cct, 20) << "image_extents=" << image_extents
                 << " area=" << area << dendl;

  start_in_flight_io(aio_comp);

  *dispatch_result = DISPATCH_RESULT_COMPLETE;
  ImageRequest<I>::aio_compare_and_write(m_image_ctx, aio_comp,
                                         std::move(image_extents), area,
                                         std::move(cmp_bl), std::move(bl),
                                         mismatch_offset, op_flags,
                                         parent_trace);
  return true;
}

template <typename I>
bool ImageDispatch<I>::flush(
    AioCompletion* aio_comp, FlushSource flush_source,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  start_in_flight_io(aio_comp);

  *dispatch_result = DISPATCH_RESULT_COMPLETE;
  ImageRequest<I>::aio_flush(m_image_ctx, aio_comp, flush_source, parent_trace);
  return true;
}

template <typename I>
bool ImageDispatch<I>::list_snaps(
    AioCompletion* aio_comp, Extents&& image_extents, SnapIds&& snap_ids,
    int list_snaps_flags, SnapshotDelta* snapshot_delta,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  auto area = get_area(image_dispatch_flags);
  ldout(cct, 20) << "image_extents=" << image_extents
                 << " area=" << area << dendl;

  start_in_flight_io(aio_comp);

  *dispatch_result = DISPATCH_RESULT_COMPLETE;
  ImageListSnapsRequest<I> req(*m_image_ctx, aio_comp, std::move(image_extents),
                               area, std::move(snap_ids), list_snaps_flags,
                               snapshot_delta, parent_trace);
  req.send();
  return true;
}

template <typename I>
bool ImageDispatch<I>::invalidate_cache(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  std::shared_lock owner_lock{m_image_ctx->owner_lock};
  m_image_ctx->io_object_dispatcher->invalidate_cache(on_finish);
  return true;
}

} // namespace io
} // namespace librbd

template class librbd::io::ImageDispatch<librbd::ImageCtx>;
