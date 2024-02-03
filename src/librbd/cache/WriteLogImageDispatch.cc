// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/dout.h"
#include "include/neorados/RADOS.hpp"
#include "librbd/cache/pwl/AbstractWriteLog.h"
#include "librbd/cache/pwl/ShutdownRequest.h"
#include "librbd/cache/WriteLogImageDispatch.h"
#include "librbd/ImageCtx.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/Utils.h"

#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::WriteLogImageDispatch: " << this << " " \
                           << __func__ << ": "

namespace librbd {
namespace cache {

template <typename I>
void WriteLogImageDispatch<I>::shut_down(Context* on_finish) {
  ceph_assert(m_image_cache != nullptr);

  Context* ctx = new LambdaContext(
      [this, on_finish](int r) {
        m_image_cache = nullptr;
	on_finish->complete(r);
      });

  cache::pwl::ShutdownRequest<I> *req = cache::pwl::ShutdownRequest<I>::create(
    *m_image_ctx, m_image_cache, m_plugin_api, ctx);
  req->send();
}

template <typename I>
bool WriteLogImageDispatch<I>::read(
    io::AioCompletion* aio_comp, io::Extents &&image_extents,
    io::ReadResult &&read_result, IOContext io_context,
    int op_flags, int read_flags,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  if (*image_dispatch_flags & io::IMAGE_DISPATCH_FLAG_CRYPTO_HEADER) {
    return false;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "image_extents=" << image_extents << dendl;

  if (io_context->get_read_snap() != CEPH_NOSNAP) {
    return false;
  }

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
  if (preprocess_length(aio_comp, image_extents)) {
    return true;
  }

  m_plugin_api.update_aio_comp(aio_comp, 1, read_result, image_extents);

  auto *req_comp = m_plugin_api.create_image_read_request(aio_comp, 0, image_extents);

  m_image_cache->read(std::move(image_extents),
                      &req_comp->bl, op_flags,
                      req_comp);
  return true;
}

template <typename I>
bool WriteLogImageDispatch<I>::write(
    io::AioCompletion* aio_comp, io::Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  if (*image_dispatch_flags & io::IMAGE_DISPATCH_FLAG_CRYPTO_HEADER) {
    return false;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "image_extents=" << image_extents << dendl;

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
  if (preprocess_length(aio_comp, image_extents)) {
    return true;
  }

  m_plugin_api.update_aio_comp(aio_comp, 1);
  io::C_AioRequest *req_comp = m_plugin_api.create_aio_request(aio_comp);
  m_image_cache->write(std::move(image_extents),
                       std::move(bl), op_flags, req_comp);
  return true;
}

template <typename I>
bool WriteLogImageDispatch<I>::discard(
    io::AioCompletion* aio_comp, io::Extents &&image_extents,
    uint32_t discard_granularity_bytes, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  if (*image_dispatch_flags & io::IMAGE_DISPATCH_FLAG_CRYPTO_HEADER) {
    return false;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "image_extents=" << image_extents << dendl;

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
  if (preprocess_length(aio_comp, image_extents)) {
    return true;
  }

  m_plugin_api.update_aio_comp(aio_comp, image_extents.size());
  for (auto &extent : image_extents) {
    io::C_AioRequest *req_comp = m_plugin_api.create_aio_request(aio_comp);
    m_image_cache->discard(extent.first, extent.second,
                           discard_granularity_bytes,
                           req_comp);
  }
  return true;
}

template <typename I>
bool WriteLogImageDispatch<I>::write_same(
    io::AioCompletion* aio_comp, io::Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  if (*image_dispatch_flags & io::IMAGE_DISPATCH_FLAG_CRYPTO_HEADER) {
    return false;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "image_extents=" << image_extents << dendl;

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
  if (preprocess_length(aio_comp, image_extents)) {
    return true;
  }

  m_plugin_api.update_aio_comp(aio_comp, image_extents.size());
  for (auto &extent : image_extents) {
    io::C_AioRequest *req_comp = m_plugin_api.create_aio_request(aio_comp);
    m_image_cache->writesame(extent.first, extent.second,
                             std::move(bl), op_flags,
                             req_comp);
  }
  return true;
}

template <typename I>
bool WriteLogImageDispatch<I>::compare_and_write(
    io::AioCompletion* aio_comp, io::Extents &&image_extents,
    bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
    int op_flags, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  if (*image_dispatch_flags & io::IMAGE_DISPATCH_FLAG_CRYPTO_HEADER) {
    return false;
  }

  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "image_extents=" << image_extents << dendl;

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;
  if (preprocess_length(aio_comp, image_extents)) {
    return true;
  }

  m_plugin_api.update_aio_comp(aio_comp, 1);
  io::C_AioRequest *req_comp = m_plugin_api.create_aio_request(aio_comp);
  m_image_cache->compare_and_write(
    std::move(image_extents), std::move(cmp_bl), std::move(bl),
    mismatch_offset, op_flags, req_comp);
  return true;
}

template <typename I>
bool WriteLogImageDispatch<I>::flush(
    io::AioCompletion* aio_comp, io::FlushSource flush_source,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result,
    Context** on_finish, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  *dispatch_result = io::DISPATCH_RESULT_COMPLETE;

  m_plugin_api.update_aio_comp(aio_comp, 1);

  io::C_AioRequest *req_comp = m_plugin_api.create_aio_request(aio_comp);
  m_image_cache->flush(flush_source, req_comp);

  return true;
}

template <typename I>
bool WriteLogImageDispatch<I>::list_snaps(
    io::AioCompletion* aio_comp, io::Extents&& image_extents,
    io::SnapIds&& snap_ids,
    int list_snaps_flags, io::SnapshotDelta* snapshot_delta,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result, Context** on_finish,
    Context* on_dispatched) {
  return false;
}

template <typename I>
bool WriteLogImageDispatch<I>::preprocess_length(
    io::AioCompletion* aio_comp, io::Extents &image_extents) const {
  auto total_bytes = io::util::get_extents_length(image_extents);
  if (total_bytes == 0) {
    m_plugin_api.update_aio_comp(aio_comp, 0);
    return true;
  }
  return false;
}

template <typename I>
bool WriteLogImageDispatch<I>::invalidate_cache(Context* on_finish) {
  m_image_cache->invalidate(on_finish);
  return true;
}

} // namespace io
} // namespace librbd

template class librbd::cache::WriteLogImageDispatch<librbd::ImageCtx>;
