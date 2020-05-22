// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/PluginRegistry.h"
#include "include/Context.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include <boost/tokenizer.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::PluginRegistry: " \
                           << this << " " << __func__ << ": "

namespace librbd {

template <typename I>
PluginRegistry<I>::PluginRegistry(I* image_ctx) : m_image_ctx(image_ctx) {
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
      ctx->complete(-ENOENT);
      break;
    }

    m_plugin_hook_points.emplace_back();
    auto hook_points = &m_plugin_hook_points.back();
    plugin->init(m_image_ctx, hook_points, ctx);
  }

  gather_ctx->activate();
}

} // namespace librbd

template class librbd::PluginRegistry<librbd::ImageCtx>;
