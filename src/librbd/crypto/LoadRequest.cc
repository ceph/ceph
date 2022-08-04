// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LoadRequest.h"

#include "common/dout.h"
#include "common/errno.h"
#include "librbd/Utils.h"
#include "librbd/ImageCtx.h"
#include "librbd/crypto/EncryptionFormat.h"
#include "librbd/crypto/Utils.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatcherInterface.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/Types.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::crypto::LoadRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace crypto {

using librbd::util::create_context_callback;

template <typename I>
LoadRequest<I>::LoadRequest(
        I* image_ctx, EncryptionFormat format,
        Context* on_finish) : m_image_ctx(image_ctx),
                              m_on_finish(on_finish),
                              m_format_idx(0) {
  m_formats.push_back(std::move(format));
}

template <typename I>
void LoadRequest<I>::send() {
  if (m_image_ctx->encryption_format.get() != nullptr) {
    lderr(m_image_ctx->cct) << "encryption already loaded" << dendl;
    finish(-EEXIST);
    return;
  }

  auto ictx = m_image_ctx;
  while (ictx != nullptr) {
    if (ictx->test_features(RBD_FEATURE_JOURNALING)) {
      lderr(m_image_ctx->cct) << "cannot use encryption with journal."
                              << " image name: " << ictx->name << dendl;
      finish(-ENOTSUP);
      return;
    }
    ictx = ictx->parent;
  }

  m_current_image_ctx = m_image_ctx;
  flush();
}

template <typename I>
void LoadRequest<I>::flush() {
  auto ctx = create_context_callback<
          LoadRequest<I>, &LoadRequest<I>::handle_flush>(this);
  auto aio_comp = io::AioCompletion::create_and_start(
    ctx, librbd::util::get_image_ctx(m_image_ctx), io::AIO_TYPE_FLUSH);
  auto req = io::ImageDispatchSpec::create_flush(
    *m_image_ctx, io::IMAGE_DISPATCH_LAYER_INTERNAL_START, aio_comp,
    io::FLUSH_SOURCE_INTERNAL, {});
  req->send();
}

template <typename I>
void LoadRequest<I>::handle_flush(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_image_ctx->cct) << "failed to flush image" << dendl;
    finish(r);
    return;
  }

  load();
}

template <typename I>
void LoadRequest<I>::load() {
  ldout(m_image_ctx->cct, 20) << "format_idx=" << m_format_idx << dendl;

  auto ctx = create_context_callback<
          LoadRequest<I>, &LoadRequest<I>::handle_load>(this);
  m_formats[m_format_idx]->load(m_current_image_ctx, ctx);
}

template <typename I>
void LoadRequest<I>::handle_load(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << ". image name: "
                              << m_current_image_ctx->name << dendl;

  if (r < 0) {
    lderr(m_image_ctx->cct) << "failed to load encryption. image name: "
                            << m_current_image_ctx->name << dendl;
    finish(r);
    return;
  }

  m_current_image_ctx = m_current_image_ctx->parent;
  if (m_current_image_ctx != nullptr) {
    // move on to loading parent
    m_format_idx++;
    if (m_format_idx >= m_formats.size()) {
      // try to load next ancestor using the same format
      m_formats.push_back(m_formats[m_formats.size() - 1]->clone());
    }

    load();
  } else {
    invalidate_cache();
  }
}

template <typename I>
void LoadRequest<I>::invalidate_cache() {
  auto ctx = create_context_callback<
          LoadRequest<I>, &LoadRequest<I>::handle_invalidate_cache>(this);
  m_image_ctx->io_image_dispatcher->invalidate_cache(ctx);
}

template <typename I>
void LoadRequest<I>::handle_invalidate_cache(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_image_ctx->cct) << "failed to invalidate image cache" << dendl;
  }

  finish(r);
}

template <typename I>
void LoadRequest<I>::finish(int r) {
  ldout(m_image_ctx->cct, 20) << "r=" << r << dendl;

  if (r == 0) {
    auto ictx = m_image_ctx;
    for (auto& format : m_formats) {
      util::set_crypto(ictx, std::move(format));
      ictx = ictx->parent;
    }
  }

  m_on_finish->complete(r);
  delete this;
}

} // namespace crypto
} // namespace librbd

template class librbd::crypto::LoadRequest<librbd::ImageCtx>;
