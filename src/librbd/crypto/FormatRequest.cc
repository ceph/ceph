// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "FormatRequest.h"

#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/io/ObjectDispatcherInterface.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::crypto::FormatRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace crypto {

using librbd::util::create_context_callback;

template <typename I>
FormatRequest<I>::FormatRequest(
        I* image_ctx, std::unique_ptr<EncryptionFormat<I>> format,
        Context* on_finish) : m_image_ctx(image_ctx),
                              m_format(std::move(format)),
                              m_on_finish(on_finish) {
}

template <typename I>
void FormatRequest<I>::send() {
  if (m_image_ctx->io_object_dispatcher->exists(
          io::OBJECT_DISPATCH_LAYER_CRYPTO)) {
    lderr(m_image_ctx->cct) << "cannot format with already loaded encryption"
                            << dendl;
    finish(-EEXIST);
    return;
  }

  if (m_image_ctx->parent != nullptr) {
    lderr(m_image_ctx->cct) << "cannot format a cloned image" << dendl;
    finish(-ENOTSUP);
    return;
  }

  if (m_image_ctx->test_features(RBD_FEATURE_JOURNALING)) {
    lderr(m_image_ctx->cct) << "cannot use encryption with journal" << dendl;
    finish(-ENOTSUP);
    return;
  }

  auto ctx = create_context_callback<
          FormatRequest<I>, &FormatRequest<I>::finish>(this);
  m_format->format(m_image_ctx, ctx);
}

template <typename I>
void FormatRequest<I>::finish(int r) {
  m_on_finish->complete(r);
  delete this;
}

} // namespace crypto
} // namespace librbd

template class librbd::crypto::FormatRequest<librbd::ImageCtx>;
