// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_PLUGIN_PARENT_CACHE_H
#define CEPH_LIBRBD_PLUGIN_PARENT_CACHE_H

#include "librbd/plugin/Types.h"
#include "include/Context.h"

namespace librbd {

struct ImageCtx;

namespace plugin {

template <typename ImageCtxT>
class ParentCache : public Interface<ImageCtxT> {
public:
  ParentCache(CephContext* cct) : Interface<ImageCtxT>(cct) {
  }

  void init(ImageCtxT* image_ctx, Api<ImageCtxT>& api,
            cache::ImageWritebackInterface& image_writeback,
            PluginHookPoints& hook_points_list,
            Context* on_finish) override;

private:
  void handle_init_parent_cache(int r, Context* on_finish);
  using ceph::Plugin::cct;

};

} // namespace plugin
} // namespace librbd

extern template class librbd::plugin::ParentCache<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_PLUGIN_PARENT_CACHE_H
