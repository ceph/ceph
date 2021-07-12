// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FlattenRequest.h"

#include "common/dout.h"
#include "common/errno.h"
#include "librbd/Utils.h"
#include "librbd/crypto/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ReadResult.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::crypto::luks::FlattenRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace crypto {
namespace luks {

using librbd::util::create_context_callback;

template <typename I>
FlattenRequest<I>::FlattenRequest(
        I* image_ctx, Context* on_finish) : m_image_ctx(image_ctx),
                                            m_on_finish(on_finish),
                                            m_header(image_ctx->cct) {
}

template <typename I>
void FlattenRequest<I>::send() {
  // setup interface with libcryptsetup
  auto r = m_header.init();
  if (r < 0) {
    finish(r);
    return;
  }

  read_header();
}

template <typename I>
void FlattenRequest<I>::read_header() {
  auto ctx = create_context_callback<
        FlattenRequest<I>, &FlattenRequest<I>::handle_read_header>(this);

  auto aio_comp = io::AioCompletion::create_and_start(
          ctx, librbd::util::get_image_ctx(m_image_ctx), io::AIO_TYPE_READ);
  ZTracer::Trace trace;
  auto req = io::ImageDispatchSpec::create_read(
          *m_image_ctx, io::IMAGE_DISPATCH_LAYER_API_START, aio_comp,
          {{0, MAXIMUM_HEADER_SIZE}}, io::ReadResult{&m_bl},
          m_image_ctx->get_data_io_context(), 0, 0, trace);
  req->send();
}

template <typename I>
void FlattenRequest<I>::handle_read_header(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_image_ctx->cct) << "error reading from image: " << cpp_strerror(r)
                            << dendl;
    finish(r);
    return;
  }

  // write header to libcryptsetup interface
  r = m_header.write(m_bl);
  if (r < 0) {
    finish(r);
    return;
  }
  m_bl.clear();

  // switch magic
  int max_replace_offset = m_header.replace_magic(
          Header::RBD_CLONE_MAGIC, Header::LUKS_MAGIC);
  if (max_replace_offset < 0) {
    lderr(m_image_ctx->cct) << "unable to restore header magic: "
                            << cpp_strerror(max_replace_offset) << dendl;
    finish(max_replace_offset);
    return;
  } else if (max_replace_offset == 0) {
    // magic already fixed
    finish(0);
    return;
  }

  r = m_header.read(&m_bl, max_replace_offset);
  if (r < 0) {
    finish(r);
    return;
  }

  write_header();
}

template <typename I>
void FlattenRequest<I>::write_header() {
  // write header to offset 0 of the image
  auto ctx = create_context_callback<
          FlattenRequest<I>, &FlattenRequest<I>::handle_write_header>(this);
  auto aio_comp = io::AioCompletion::create_and_start(
          ctx, librbd::util::get_image_ctx(m_image_ctx), io::AIO_TYPE_WRITE);

  ZTracer::Trace trace;
  auto req = io::ImageDispatchSpec::create_write(
          *m_image_ctx, io::IMAGE_DISPATCH_LAYER_API_START, aio_comp,
          {{0, m_bl.length()}}, std::move(m_bl),
          m_image_ctx->get_data_io_context(), 0, trace);
  req->send();
}

template <typename I>
void FlattenRequest<I>::handle_write_header(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_image_ctx->cct) << "error writing header to image: "
                            << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  flush();
}

template <typename I>
void FlattenRequest<I>::flush() {
  auto ctx = create_context_callback<
          FlattenRequest<I>, &FlattenRequest<I>::handle_flush>(this);
  auto aio_comp = io::AioCompletion::create_and_start(
    ctx, librbd::util::get_image_ctx(m_image_ctx), io::AIO_TYPE_FLUSH);
  auto req = io::ImageDispatchSpec::create_flush(
    *m_image_ctx, io::IMAGE_DISPATCH_LAYER_INTERNAL_START, aio_comp,
    io::FLUSH_SOURCE_INTERNAL, {});
  req->send();
}

template <typename I>
void FlattenRequest<I>::handle_flush(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_image_ctx->cct) << "unable to flush image: " << cpp_strerror(r)
                            << dendl;
  }

  finish(r);
}

template <typename I>
void FlattenRequest<I>::finish(int r) {
  m_on_finish->complete(r);
  delete this;
}

} // namespace luks
} // namespace crypto
} // namespace librbd

template class librbd::crypto::luks::FlattenRequest<librbd::ImageCtx>;
