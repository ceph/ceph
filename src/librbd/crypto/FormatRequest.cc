// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FormatRequest.h"

#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/crypto/EncryptionFormat.h"
#include "librbd/crypto/ShutDownCryptoRequest.h"
#include "librbd/crypto/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ObjectDispatcherInterface.h"
#include "librbd/io/Types.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::crypto::FormatRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace crypto {

using librbd::util::create_context_callback;

template <typename I>
FormatRequest<I>::FormatRequest(
        I* image_ctx, EncryptionFormat format,
        Context* on_finish) : m_image_ctx(image_ctx),
                              m_format(std::move(format)),
                              m_on_finish(on_finish) {
}

template <typename I>
void FormatRequest<I>::send() {
  if (m_image_ctx->test_features(RBD_FEATURE_JOURNALING)) {
    lderr(m_image_ctx->cct) << "cannot use encryption with journal" << dendl;
    finish(-ENOTSUP);
    return;
  }
  
  if (m_image_ctx->encryption_format.get() == nullptr) {
    format();
    return;
  } else if (m_image_ctx->parent != nullptr) {
    lderr(m_image_ctx->cct) << "cannot format a cloned image "
                               "while encryption is loaded"
                            << dendl;
    finish(-EINVAL);
    return;
  }

  auto ctx = create_context_callback<
          FormatRequest<I>, &FormatRequest<I>::handle_shutdown_crypto>(this);
  auto *req = ShutDownCryptoRequest<I>::create(m_image_ctx, nullptr, ctx);
  req->send();
}

template <typename I>
void FormatRequest<I>::handle_shutdown_crypto(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r != 0) {
    lderr(m_image_ctx->cct) << "unable to unload existing crypto: "
                            << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  format();
}

template <typename I>
void FormatRequest<I>::format() {
  auto ctx = create_context_callback<
          FormatRequest<I>, &FormatRequest<I>::handle_format>(this);
  m_format->format(m_image_ctx, ctx);
}

template <typename I>
void FormatRequest<I>::handle_format(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r != 0) {
    lderr(m_image_ctx->cct) << "unable to format image: " << cpp_strerror(r)
                            << dendl;
    finish(r);
    return;
  }

  flush();
}

template <typename I>
void FormatRequest<I>::flush() {
  auto ctx = create_context_callback<
          FormatRequest<I>, &FormatRequest<I>::handle_flush>(this);
  auto aio_comp = io::AioCompletion::create_and_start(
    ctx, librbd::util::get_image_ctx(m_image_ctx), io::AIO_TYPE_FLUSH);
  auto req = io::ImageDispatchSpec::create_flush(
    *m_image_ctx, io::IMAGE_DISPATCH_LAYER_INTERNAL_START, aio_comp,
    io::FLUSH_SOURCE_INTERNAL, {});
  req->send();
}

template <typename I>
void FormatRequest<I>::handle_flush(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r != 0) {
    lderr(m_image_ctx->cct) << "unable to flush image: " << cpp_strerror(r)
                            << dendl;
  }

  finish(r);
}

template <typename I>
void FormatRequest<I>::finish(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r == 0 && m_image_ctx->parent == nullptr) {
    // only load on flat images, to avoid a case where encryption
    // is wrongfully loaded only on the child image
    util::set_crypto(m_image_ctx, std::move(m_format));
  }
  m_on_finish->complete(r);
  delete this;
}

} // namespace crypto
} // namespace librbd

template class librbd::crypto::FormatRequest<librbd::ImageCtx>;
