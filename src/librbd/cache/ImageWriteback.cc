// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ImageWriteback.h"
#include "include/buffer.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ReadResult.h"

#undef dout_subsys
#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ImageWriteback: " << __func__ << ": "

namespace librbd {
namespace cache {

template <typename I>
ImageWriteback<I>::ImageWriteback(I &image_ctx) : m_image_ctx(image_ctx) {
}

template <typename I>
void ImageWriteback<I>::aio_read(Extents &&image_extents, bufferlist *bl,
                                 int fadvise_flags, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "image_extents=" << image_extents << ", "
                 << "on_finish=" << on_finish << dendl;

  ImageCtx *image_ctx = util::get_image_ctx(&m_image_ctx);
  auto aio_comp = io::AioCompletion::create_and_start(
      on_finish, image_ctx, io::AIO_TYPE_READ);
  ZTracer::Trace trace;
  auto req = io::ImageDispatchSpec::create_read(
    *image_ctx, io::IMAGE_DISPATCH_LAYER_WRITEBACK_CACHE, aio_comp,
    std::move(image_extents), io::ImageArea::DATA, io::ReadResult{bl},
    image_ctx->get_data_io_context(), fadvise_flags, 0, trace);
  req->send();
}

template <typename I>
void ImageWriteback<I>::aio_write(Extents &&image_extents,
                                  ceph::bufferlist&& bl,
                                  int fadvise_flags, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "image_extents=" << image_extents << ", "
                 << "on_finish=" << on_finish << dendl;

  ImageCtx *image_ctx = util::get_image_ctx(&m_image_ctx);
  auto aio_comp = io::AioCompletion::create_and_start(
      on_finish, image_ctx, io::AIO_TYPE_WRITE);
  ZTracer::Trace trace;
  auto req = io::ImageDispatchSpec::create_write(
    *image_ctx, io::IMAGE_DISPATCH_LAYER_WRITEBACK_CACHE, aio_comp,
    std::move(image_extents), io::ImageArea::DATA, std::move(bl),
    fadvise_flags, trace);
  req->send();
}

template <typename I>
void ImageWriteback<I>::aio_discard(uint64_t offset, uint64_t length,
                                    uint32_t discard_granularity_bytes,
                                    Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "offset=" << offset << ", "
                 << "length=" << length << ", "
                << "on_finish=" << on_finish << dendl;

  ImageCtx *image_ctx = util::get_image_ctx(&m_image_ctx);
  auto aio_comp = io::AioCompletion::create_and_start(
      on_finish, image_ctx, io::AIO_TYPE_DISCARD);
  ZTracer::Trace trace;
  auto req = io::ImageDispatchSpec::create_discard(
    *image_ctx, io::IMAGE_DISPATCH_LAYER_WRITEBACK_CACHE, aio_comp,
    {{offset, length}}, io::ImageArea::DATA, discard_granularity_bytes, trace);
  req->send();
}

template <typename I>
void ImageWriteback<I>::aio_flush(io::FlushSource flush_source,
                                  Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "on_finish=" << on_finish << dendl;

  ImageCtx *image_ctx = util::get_image_ctx(&m_image_ctx);
  auto aio_comp = io::AioCompletion::create_and_start(
      on_finish, image_ctx, io::AIO_TYPE_FLUSH);

  ZTracer::Trace trace;
  auto req = io::ImageDispatchSpec::create_flush(
    *image_ctx, io::IMAGE_DISPATCH_LAYER_WRITEBACK_CACHE, aio_comp,
    flush_source, trace);
  req->send();
}

template <typename I>
void ImageWriteback<I>::aio_writesame(uint64_t offset, uint64_t length,
                                      ceph::bufferlist&& bl,
                                      int fadvise_flags, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "offset=" << offset << ", "
                 << "length=" << length << ", "
                 << "data_len=" << bl.length() << ", "
                 << "on_finish=" << on_finish << dendl;

  ImageCtx *image_ctx = util::get_image_ctx(&m_image_ctx);
  auto aio_comp = io::AioCompletion::create_and_start(
      on_finish, image_ctx, io::AIO_TYPE_WRITESAME);
  ZTracer::Trace trace;
  auto req = io::ImageDispatchSpec::create_write_same(
    *image_ctx, io::IMAGE_DISPATCH_LAYER_WRITEBACK_CACHE, aio_comp,
    {{offset, length}}, io::ImageArea::DATA, std::move(bl),
    fadvise_flags, trace);
  req->send();
}

template <typename I>
void ImageWriteback<I>::aio_compare_and_write(Extents &&image_extents,
                                              ceph::bufferlist&& cmp_bl,
                                              ceph::bufferlist&& bl,
                                              uint64_t *mismatch_offset,
                                              int fadvise_flags,
                                              Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "image_extents=" << image_extents << ", "
                 << "on_finish=" << on_finish << dendl;

  ImageCtx *image_ctx = util::get_image_ctx(&m_image_ctx);
  auto aio_comp = io::AioCompletion::create_and_start(
      on_finish, image_ctx, io::AIO_TYPE_COMPARE_AND_WRITE);
  ZTracer::Trace trace;
  auto req = io::ImageDispatchSpec::create_compare_and_write(
    *image_ctx, io::IMAGE_DISPATCH_LAYER_WRITEBACK_CACHE, aio_comp,
    std::move(image_extents), io::ImageArea::DATA, std::move(cmp_bl),
    std::move(bl), mismatch_offset, fadvise_flags, trace);
  req->send();
}

} // namespace cache
} // namespace librbd

template class librbd::cache::ImageWriteback<librbd::ImageCtx>;

