// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FlattenRequest.h"

#include "common/dout.h"
#include "common/errno.h"
#include "librbd/Utils.h"
#include "librbd/crypto/EncryptionFormat.h"
#include "librbd/crypto/ShutDownCryptoRequest.h"
#include "librbd/crypto/Utils.h"
#include "librbd/crypto/luks/LoadRequest.h"
#include "librbd/crypto/luks/Magic.h"
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
                                            m_on_finish(on_finish) {
  ceph_assert(m_image_ctx->encryption_format.get() != nullptr);
}

template <typename I>
void FlattenRequest<I>::send() {
  shutdown_crypto();
}

template <typename I>
void FlattenRequest<I>::shutdown_crypto() {
  auto ctx = create_context_callback<
        FlattenRequest<I>, &FlattenRequest<I>::handle_shutdown_crypto>(this);

  auto *req = ShutDownCryptoRequest<I>::create(
          m_image_ctx, &m_encryption_format, ctx);
  req->send();
}

template <typename I>
void FlattenRequest<I>::handle_shutdown_crypto(int r) {
  if (r < 0) {
    lderr(m_image_ctx->cct) << "error shutting down crypto: "
                            << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  ceph_assert(m_encryption_format.get() != nullptr);

  read_header();
}

template <typename I>
void FlattenRequest<I>::read_header() {
  auto ctx = create_context_callback<
        FlattenRequest<I>, &FlattenRequest<I>::handle_read_header>(this);

  uint64_t data_offset = m_encryption_format->get_crypto()->get_data_offset();

  auto aio_comp = io::AioCompletion::create_and_start(
          ctx, librbd::util::get_image_ctx(m_image_ctx), io::AIO_TYPE_READ);
  ZTracer::Trace trace;
  auto req = io::ImageDispatchSpec::create_read(
          *m_image_ctx, io::IMAGE_DISPATCH_LAYER_API_START, aio_comp,
          {{0, data_offset}}, io::ImageArea::CRYPTO_HEADER,
          io::ReadResult{&m_bl}, m_image_ctx->get_data_io_context(), 0, 0,
          trace);
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

  r = Magic::is_rbd_clone(m_bl);
  if (r < 0) {
    lderr(m_image_ctx->cct) << "unable to determine encryption header magic: "
                            << cpp_strerror(r) << dendl;
    finish(r);
    return;
  } else if (r > 0) {
    // switch magic
    r = Magic::replace_magic(m_image_ctx->cct, m_bl);
    if (r < 0) {
      lderr(m_image_ctx->cct) << "unable to restore header magic: "
                              << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }
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
          {{0, m_bl.length()}}, io::ImageArea::CRYPTO_HEADER,
          std::move(m_bl), m_image_ctx->get_data_io_context(), 0, trace);
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
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  // restore crypto to image context
  if (m_encryption_format.get() != nullptr) {
    util::set_crypto(m_image_ctx, std::move(m_encryption_format));
  }

  m_on_finish->complete(r);
  delete this;
}

} // namespace luks
} // namespace crypto
} // namespace librbd

template class librbd::crypto::luks::FlattenRequest<librbd::ImageCtx>;
