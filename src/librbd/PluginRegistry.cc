// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/PluginRegistry.h"
#include "include/Context.h"
#include "common/dout.h"
#include "librbd/cache/ImageWriteback.h"
#include "librbd/ImageCtx.h"
#include "librbd/plugin/Api.h"
#include <boost/tokenizer.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::PluginRegistry: " \
                           << this << " " << __func__ << ": "

namespace librbd {

template <typename I>
PluginRegistry<I>::PluginRegistry(I* image_ctx)
  : m_image_ctx(image_ctx), m_plugin_api(std::make_unique<plugin::Api<I>>()),
    m_image_writeback(std::make_unique<cache::ImageWriteback<I>>(*image_ctx)) {
}

template <typename I>
PluginRegistry<I>::~PluginRegistry() {
}

template <typename I>
void PluginRegistry<I>::init(const std::string& plugins, Context* on_finish) {
  auto cct = m_image_ctx->cct;
  auto plugin_registry = cct->get_plugin_registry();

  auto gather_ctx = new C_Gather(cct, on_finish);

  boost::tokenizer<boost::escaped_list_separator<char>> tokenizer(plugins);
  for (auto token : tokenizer) {
    ldout(cct, 5) << "attempting to load plugin: " << token << dendl;

    auto ctx = gather_ctx->new_sub();

    auto plugin = dynamic_cast<plugin::Interface<I>*>(
      plugin_registry->get_with_load("librbd", "librbd_" + token));
    if (plugin == nullptr) {
      lderr(cct) << "failed to load plugin: " << token << dendl;
      ctx->complete(-ENOSYS);
      break;
    }

    plugin->init(
	m_image_ctx, *m_plugin_api, *m_image_writeback, m_plugin_hook_points, ctx);
  }

  gather_ctx->activate();
}

template <typename I>
void PluginRegistry<I>::acquired_exclusive_lock(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  auto gather_ctx = new C_Gather(cct, on_finish);

  for (auto &hook : m_plugin_hook_points) {
    auto ctx = gather_ctx->new_sub();
    hook->acquired_exclusive_lock(ctx);
  }
  gather_ctx->activate();
}

template <typename I>
void PluginRegistry<I>::prerelease_exclusive_lock(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  auto gather_ctx = new C_Gather(cct, on_finish);

  for (auto &hook : m_plugin_hook_points) {
    auto ctx = gather_ctx->new_sub();
    hook->prerelease_exclusive_lock(ctx);
  }
  gather_ctx->activate();
}

template <typename I>
void PluginRegistry<I>::discard(Context* on_finish) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << dendl;

  auto gather_ctx = new C_Gather(cct, on_finish);

  for (auto &hook : m_plugin_hook_points) {
    auto ctx = gather_ctx->new_sub();
    hook->discard(ctx);
  }
  gather_ctx->activate();
}

} // namespace librbd

template class librbd::PluginRegistry<librbd::ImageCtx>;
