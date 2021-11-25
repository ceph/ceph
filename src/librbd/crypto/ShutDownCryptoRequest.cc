// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ShutDownCryptoRequest.h"

#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/crypto/CryptoImageDispatch.h"
#include "librbd/crypto/CryptoObjectDispatch.h"
#include "librbd/io/ImageDispatcherInterface.h"
#include "librbd/io/ObjectDispatcherInterface.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::crypto::ShutDownCryptoRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace crypto {

using librbd::util::create_context_callback;

template <typename I>
ShutDownCryptoRequest<I>::ShutDownCryptoRequest(
        I* image_ctx, Context* on_finish) : m_image_ctx(image_ctx),
                                            m_on_finish(on_finish) {
}

template <typename I>
void ShutDownCryptoRequest<I>::send() {
  shut_down_object_dispatch();
}

template <typename I>
void ShutDownCryptoRequest<I>::shut_down_object_dispatch() {
  if (!m_image_ctx->io_object_dispatcher->exists(
          io::OBJECT_DISPATCH_LAYER_CRYPTO)) {
    finish(0);
    return;
  }

  auto ctx = create_context_callback<
          ShutDownCryptoRequest<I>,
          &ShutDownCryptoRequest<I>::handle_shut_down_object_dispatch>(this);

  m_image_ctx->io_object_dispatcher->shut_down_dispatch(
          io::OBJECT_DISPATCH_LAYER_CRYPTO, ctx);
}

template <typename I>
void ShutDownCryptoRequest<I>::handle_shut_down_object_dispatch(int r) {
  if (r < 0) {
    lderr(m_image_ctx->cct) << "failed to shut down object dispatch: "
                            << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  shut_down_image_dispatch();
}

template <typename I>
void ShutDownCryptoRequest<I>::shut_down_image_dispatch() {
  if (!m_image_ctx->io_image_dispatcher->exists(
          io::IMAGE_DISPATCH_LAYER_CRYPTO)) {
    finish(0);
    return;
  }

  auto ctx = create_context_callback<
        ShutDownCryptoRequest<I>,
        &ShutDownCryptoRequest<I>::handle_shut_down_image_dispatch>(this);
  m_image_ctx->io_image_dispatcher->shut_down_dispatch(
          io::IMAGE_DISPATCH_LAYER_CRYPTO, ctx);
}

template <typename I>
void ShutDownCryptoRequest<I>::handle_shut_down_image_dispatch(int r) {
  if (r < 0) {
    lderr(m_image_ctx->cct) << "failed to shut down image dispatch: "
                            << cpp_strerror(r) << dendl;
  }
  finish(r);
}

template <typename I>
void ShutDownCryptoRequest<I>::finish(int r) {
  if (r == 0) {
    std::unique_lock image_locker{m_image_ctx->image_lock};
    m_image_ctx->crypto = nullptr;
  }

  m_on_finish->complete(r);
  delete this;
}

} // namespace crypto
} // namespace librbd

template class librbd::crypto::ShutDownCryptoRequest<librbd::ImageCtx>;
