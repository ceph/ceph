// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_PLUGIN_REGISTRY_H
#define CEPH_LIBRBD_PLUGIN_REGISTRY_H

#include "librbd/plugin/Types.h"
#include <memory>
#include <string>
#include <list>

struct Context;

namespace librbd {

struct ImageCtx;

namespace plugin { template <typename> struct Api; }

template <typename ImageCtxT>
class PluginRegistry {
public:
  PluginRegistry(ImageCtxT* image_ctx);
  ~PluginRegistry();

  void init(const std::string& plugins, Context* on_finish);

private:
  typedef std::list<plugin::HookPoints> PluginHookPoints;

  ImageCtxT* m_image_ctx;
  std::unique_ptr<plugin::Api<ImageCtxT>> m_plugin_api;

  std::string m_plugins;

  PluginHookPoints m_plugin_hook_points;

};

} // namespace librbd

extern template class librbd::PluginRegistry<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_PLUGIN_REGISTRY_H
