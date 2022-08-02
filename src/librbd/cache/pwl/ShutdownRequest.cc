// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/cache/pwl/ShutdownRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/Operations.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/cache/Types.h"

#include "librbd/cache/pwl/AbstractWriteLog.h"
#include "librbd/plugin/Api.h"

#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl:ShutdownRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace cache {
namespace pwl {

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;

template <typename I>
ShutdownRequest<I>* ShutdownRequest<I>::create(
    I &image_ctx,
    AbstractWriteLog<I> *image_cache,
    plugin::Api<I>& plugin_api,
    Context *on_finish) {
  return new ShutdownRequest(image_ctx, image_cache, plugin_api, on_finish);
}

template <typename I>
ShutdownRequest<I>::ShutdownRequest(
    I &image_ctx,
    AbstractWriteLog<I> *image_cache,
    plugin::Api<I>& plugin_api,
    Context *on_finish)
  : m_image_ctx(image_ctx),
    m_image_cache(image_cache),
    m_plugin_api(plugin_api),
    m_on_finish(create_async_context_callback(image_ctx, on_finish)),
    m_error_result(0) {
}

template <typename I>
void ShutdownRequest<I>::send() {
  send_shutdown_image_cache();
}

template <typename I>
void ShutdownRequest<I>::send_shutdown_image_cache() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  if (m_image_cache == nullptr) {
    finish();
    return;
  }

  using klass = ShutdownRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_shutdown_image_cache>(
    this);

  m_image_cache->shut_down(ctx);
}

template <typename I>
void ShutdownRequest<I>::handle_shutdown_image_cache(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  if (r < 0) {
    lderr(cct) << "failed to shut down the image cache: " << cpp_strerror(r)
               << dendl;
    save_result(r);
    finish();
    return;
  } else {
    delete m_image_cache;
    m_image_cache = nullptr;
  }
  send_remove_feature_bit();
}

template <typename I>
void ShutdownRequest<I>::send_remove_feature_bit() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  uint64_t new_features = m_image_ctx.features & ~RBD_FEATURE_DIRTY_CACHE;
  uint64_t features_mask = RBD_FEATURE_DIRTY_CACHE;
  ldout(cct, 10) << "old_features=" << m_image_ctx.features
                 << ", new_features=" << new_features
                 << ", features_mask=" << features_mask
                 << dendl;

  int r = librbd::cls_client::set_features(&m_image_ctx.md_ctx, m_image_ctx.header_oid,
                                           new_features, features_mask);
  m_image_ctx.features &= ~RBD_FEATURE_DIRTY_CACHE;
  using klass = ShutdownRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_remove_feature_bit>(
    this);
  ctx->complete(r);
}

template <typename I>
void ShutdownRequest<I>::handle_remove_feature_bit(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  if (r < 0) {
    lderr(cct) << "failed to remove the feature bit: " << cpp_strerror(r)
               << dendl;
    save_result(r);
    finish();
    return;
  }
  send_remove_image_cache_state();
}

template <typename I>
void ShutdownRequest<I>::send_remove_image_cache_state() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  using klass = ShutdownRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_remove_image_cache_state>(
    this);
  std::shared_lock owner_lock{m_image_ctx.owner_lock};
  m_plugin_api.execute_image_metadata_remove(&m_image_ctx, PERSISTENT_CACHE_STATE, ctx);
}

template <typename I>
void ShutdownRequest<I>::handle_remove_image_cache_state(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  if (r < 0) {
    lderr(cct) << "failed to remove the image cache state: " << cpp_strerror(r)
               << dendl;
    save_result(r);
  }
  finish();
}

template <typename I>
void ShutdownRequest<I>::finish() {
  m_on_finish->complete(m_error_result);
  delete this;
}

} // namespace pwl
} // namespace cache
} // namespace librbd

template class librbd::cache::pwl::ShutdownRequest<librbd::ImageCtx>;
