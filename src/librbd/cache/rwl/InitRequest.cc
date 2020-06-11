// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/rwl/InitRequest.h"
#include "librbd/Utils.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/asio/ContextWQ.h"

#if defined(WITH_RBD_RWL)
#include "librbd/cache/rwl/ImageCacheState.h"
#include "librbd/cache/ReplicatedWriteLog.h"
#endif // WITH_RBD_RWL

#include "librbd/cache/Utils.h"
#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd_rwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::rwl:InitRequest " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace cache {
namespace rwl {

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;

template <typename I>
InitRequest<I>* InitRequest<I>::create(I &image_ctx,
                                       Context *on_finish) {
  return new InitRequest(image_ctx, on_finish);
}

template <typename I>
InitRequest<I>::InitRequest(I &image_ctx, Context *on_finish)
  : m_image_ctx(image_ctx),
    m_on_finish(create_async_context_callback(image_ctx, on_finish)),
    m_error_result(0) {
}

template <typename I>
void InitRequest<I>::send() {
#if defined(WITH_RBD_RWL)
  get_image_cache_state();
#else
  finish();
#endif // WITH_RBD_RWL
}

#if defined(WITH_RBD_RWL)
template <typename I>
void InitRequest<I>::get_image_cache_state() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  int r;
  auto cache_state = ImageCacheState<I>::get_image_cache_state(&m_image_ctx, r);

  if (r < 0 || !cache_state) {
    save_result(r);
    finish();
    return;
  } else if (!cache_state->is_valid()) {
    delete cache_state;
    cache_state = nullptr;
    lderr(cct) << "failed to get image cache state: " << cpp_strerror(r)
               << dendl;
    save_result(-ENOENT);
    finish();
    return;
  }

  auto cache_type = cache_state->get_image_cache_type();
  switch(cache_type) {
    case cache::IMAGE_CACHE_TYPE_RWL:
      m_image_ctx.image_cache =
        new librbd::cache::ReplicatedWriteLog<I>(m_image_ctx,
                                                 cache_state);
      break;
    default:
      delete cache_state;
      cache_state = nullptr;
      save_result(-ENOENT);
      finish();
      return;
  }

  init_image_cache();
}

template <typename I>
void InitRequest<I>::init_image_cache() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  using klass = InitRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_init_image_cache>(
    this);
  m_image_ctx.image_cache->init(ctx);
}

template <typename I>
void InitRequest<I>::handle_init_image_cache(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  if (r < 0) {
    lderr(cct) << "failed to init image cache: " << cpp_strerror(r)
               << dendl;
    delete m_image_ctx.image_cache;
    m_image_ctx.image_cache = nullptr;
    save_result(r);
    finish();
    return;
  }
  set_feature_bit();
}

template <typename I>
void InitRequest<I>::set_feature_bit() {
  CephContext *cct = m_image_ctx.cct;

  uint64_t new_features = m_image_ctx.features | RBD_FEATURE_DIRTY_CACHE;
  uint64_t features_mask = RBD_FEATURE_DIRTY_CACHE;
  ldout(cct, 10) << "old_features=" << m_image_ctx.features
                 << ", new_features=" << new_features
                 << ", features_mask=" << features_mask
                 << dendl;

  int r = librbd::cls_client::set_features(&m_image_ctx.md_ctx,
                                           m_image_ctx.header_oid,
                                           new_features, features_mask);
  m_image_ctx.features |= RBD_FEATURE_DIRTY_CACHE;
  using klass = InitRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_set_feature_bit>(
    this);
  ctx->complete(r);
}

template <typename I>
void InitRequest<I>::handle_set_feature_bit(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to set feature bit: " << cpp_strerror(r)
               << dendl;
    save_result(r);
  } else if (m_image_ctx.discard_granularity_bytes) {
    ldout(cct, 1) << "RWL image cache is enabled and "
                  << "set discard_granularity_bytes = 0." << dendl;
    m_image_ctx.discard_granularity_bytes = 0;
  }
  finish();
}

#endif // WITH_RBD_RWL

template <typename I>
void InitRequest<I>::finish() {
  m_on_finish->complete(m_error_result);
  delete this;
}

} // namespace rwl
} // namespace cache
} // namespace librbd

template class librbd::cache::rwl::InitRequest<librbd::ImageCtx>;
