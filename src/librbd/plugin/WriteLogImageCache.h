// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_PLUGIN_WRITELOG_IMAGE_CACHE_H
#define CEPH_LIBRBD_PLUGIN_WRITELOG_IMAGE_CACHE_H

#include "librbd/plugin/Types.h"
#include "include/Context.h"

namespace librbd {

struct ImageCtx;

namespace plugin {

template <typename ImageCtxT>
class WriteLogImageCache : public Interface<ImageCtxT> {
public:
  WriteLogImageCache(CephContext* cct) : Interface<ImageCtxT>(cct) {
  }

  ~WriteLogImageCache() override;

  void init(ImageCtxT* image_ctx, Api<ImageCtxT>& api,
            cache::ImageWritebackInterface& image_writeback,
            PluginHookPoints& hook_points_list,
            Context* on_finish) override;

  class HookPoints : public plugin::HookPoints {
  public:
    HookPoints(ImageCtxT* image_ctx,
	       cache::ImageWritebackInterface& image_writeback,
	       plugin::Api<ImageCtxT>& plugin_api);
    ~HookPoints() override;

    void acquired_exclusive_lock(Context* on_finish) override;
    void prerelease_exclusive_lock(Context* on_finish) override;
    void discard(Context* on_finish) override;

  private:
    ImageCtxT* m_image_ctx;
    cache::ImageWritebackInterface& m_image_writeback;
    plugin::Api<ImageCtxT>& m_plugin_api;
  };

};

} // namespace plugin
} // namespace librbd

extern template class librbd::plugin::WriteLogImageCache<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_PLUGIN_WRITELOG_IMAGE_CACHE_H
