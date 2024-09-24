// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_PLUGIN_TYPES_H
#define CEPH_LIBRBD_PLUGIN_TYPES_H

#include "include/common_fwd.h"
#include "include/Context.h"
#include "common/PluginRegistry.h"
#include "librbd/cache/ImageWriteback.h"

namespace librbd {
namespace plugin {

template <typename> struct Api;

struct HookPoints {
  virtual ~HookPoints() {
  }
  virtual void acquired_exclusive_lock(Context* on_finish) = 0;
  virtual void prerelease_exclusive_lock(Context* on_finish) = 0;
  virtual void discard(Context* on_finish) {
    on_finish->complete(0);
  }
};

typedef std::list<std::unique_ptr<HookPoints>> PluginHookPoints;

template <typename ImageCtxT>
struct Interface : public ceph::Plugin {
  Interface(CephContext* cct) : Plugin(cct) {
  }

  virtual ~Interface() {
  }

  virtual void init(ImageCtxT* image_ctx, Api<ImageCtxT>& api,
		    librbd::cache::ImageWritebackInterface& image_writeback,
                    PluginHookPoints& hook_points_list, Context* on_finish) = 0;
};

} // namespace plugin
} // namespace librbd

#endif // CEPH_LIBRBD_PLUGIN_TYPES_H
