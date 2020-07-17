// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_PLUGIN_TYPES_H
#define CEPH_LIBRBD_PLUGIN_TYPES_H

#include "include/common_fwd.h"
#include "common/PluginRegistry.h"

struct Context;

namespace librbd {
namespace plugin {

struct HookPoints {
  // TODO later commits will add support for exclusive-lock hook points
};

template <typename ImageCtxT>
struct Interface : public ceph::Plugin {
  Interface(CephContext* cct) : Plugin(cct) {
  }

  virtual void init(ImageCtxT* image_ctx, HookPoints* hook_points,
                    Context* on_finish) = 0;
};

} // namespace plugin
} // namespace librbd

#endif // CEPH_LIBRBD_PLUGIN_TYPES_H
