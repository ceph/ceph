// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/plugin/ParentCache.h"
#include "ceph_ver.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/PluginRegistry.h"
#include "librbd/ImageCtx.h"
#include "librbd/cache/ParentCacheObjectDispatch.h"

extern "C" {

const char *__ceph_plugin_version() {
  return CEPH_GIT_NICE_VER;
}

int __ceph_plugin_init(CephContext *cct, const std::string& type,
                       const std::string& name) {
  auto plugin_registry = cct->get_plugin_registry();
  return plugin_registry->add(
    type, name, new librbd::plugin::ParentCache<librbd::ImageCtx>(cct));
}

} // extern "C"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::plugin::ParentCache: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace plugin {

template <typename I>
void ParentCache<I>::init(I* image_ctx, Api<I>& api,
                          cache::ImageWritebackInterface& image_writeback,
                          PluginHookPoints& hook_points_list,
                          Context* on_finish) {
  bool parent_cache_enabled = image_ctx->config.template get_val<bool>(
    "rbd_parent_cache_enabled");
  if (image_ctx->child == nullptr || !parent_cache_enabled ||
      !image_ctx->data_ctx.is_valid()) {
    on_finish->complete(0);
    return;
  }

  auto cct = image_ctx->cct;
  ldout(cct, 5) << dendl;

  auto parent_cache = cache::ParentCacheObjectDispatch<I>::create(
    image_ctx, api);
  on_finish = new LambdaContext([this, on_finish, parent_cache](int r) {
      if (r < 0) {
        // the object dispatcher will handle cleanup if successfully initialized
        delete parent_cache;
      }

      handle_init_parent_cache(r, on_finish);
    });
  parent_cache->init(on_finish);
}

template <typename I>
void ParentCache<I>::handle_init_parent_cache(int r, Context* on_finish) {
  ldout(cct, 5) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "Failed to initialize parent cache object dispatch layer: "
               << cpp_strerror(r) << dendl;
    on_finish->complete(r);
    return;
  }

  on_finish->complete(0);
}

} // namespace plugin
} // namespace librbd

template class librbd::plugin::ParentCache<librbd::ImageCtx>;
