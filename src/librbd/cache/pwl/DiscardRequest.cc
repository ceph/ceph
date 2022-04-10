// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/dout.h"
#include "common/errno.h"
#include "common/hostname.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/cache/pwl/DiscardRequest.h"

#if __has_include(<filesystem>)
#include <filesystem>
namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#endif

#include "librbd/cache/pwl/ImageCacheState.h"

#include "librbd/cache/Types.h"
#include "librbd/io/ImageDispatcherInterface.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"


#define dout_subsys ceph_subsys_rbd_pwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::pwl:DiscardRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace cache {
namespace pwl {

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;

template <typename I>
DiscardRequest<I>* DiscardRequest<I>::create(
    I &image_ctx,
    plugin::Api<I>& plugin_api,
    Context *on_finish) {
  return new DiscardRequest(image_ctx, plugin_api, on_finish);
}

template <typename I>
DiscardRequest<I>::DiscardRequest(
    I &image_ctx,
    plugin::Api<I>& plugin_api,
    Context *on_finish)
  : m_image_ctx(image_ctx),
    m_plugin_api(plugin_api),
    m_on_finish(create_async_context_callback(image_ctx, on_finish)),
    m_error_result(0) {
}

template <typename I>
void DiscardRequest<I>::send() {
  delete_image_cache_file();
}

template <typename I>
void DiscardRequest<I>::delete_image_cache_file() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  m_cache_state = ImageCacheState<I>::get_image_cache_state(&m_image_ctx, m_plugin_api);
  if (!m_cache_state) {
    remove_feature_bit();
    return;
  }
  if (m_cache_state->present &&
      !m_cache_state->host.compare(ceph_get_short_hostname()) &&
      fs::exists(m_cache_state->path)) {
    std::error_code ec;
    fs::remove(m_cache_state->path, ec);
    if (ec) {
      lderr(cct) << "failed to remove persistent cache file: " << ec.message()
                 << dendl;
      // not fatal
    }
  }

  remove_image_cache_state();
}

template <typename I>
void DiscardRequest<I>::remove_image_cache_state() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  using klass = DiscardRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_remove_image_cache_state>(
    this);

  m_cache_state->clear_image_cache_state(ctx);
}

template <typename I>
void DiscardRequest<I>::handle_remove_image_cache_state(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  if (r < 0) {
    lderr(cct) << "failed to remove the image cache state: " << cpp_strerror(r)
               << dendl;
    save_result(r);
    finish();
    return;
  }

  remove_feature_bit();
}

template <typename I>
void DiscardRequest<I>::remove_feature_bit() {
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
  using klass = DiscardRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_remove_feature_bit>(
    this);
  ctx->complete(r);
}

template <typename I>
void DiscardRequest<I>::handle_remove_feature_bit(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  if (r < 0) {
    lderr(cct) << "failed to remove the feature bit: " << cpp_strerror(r)
               << dendl;
    save_result(r);
  }
  finish();
}

template <typename I>
void DiscardRequest<I>::finish() {
  if (m_cache_state) {
    delete m_cache_state;
    m_cache_state = nullptr;
  }

  m_on_finish->complete(m_error_result);
  delete this;
}

} // namespace pwl
} // namespace cache
} // namespace librbd

template class librbd::cache::pwl::DiscardRequest<librbd::ImageCtx>;
