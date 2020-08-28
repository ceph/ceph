// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_PLUGIN_API_H
#define CEPH_LIBRBD_PLUGIN_API_H

#include "include/common_fwd.h"
#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "librbd/io/Types.h"

namespace ZTracer { struct Trace; }

namespace librbd {

struct ImageCtx;

namespace plugin {

template <typename ImageCtxT>
struct Api {
  using Extents = librbd::io::Extents;

  Api() {}
  virtual ~Api() {}

  virtual void read_parent(
      ImageCtxT *image_ctx, uint64_t object_no, const Extents &extents,
      librados::snap_t snap_id, const ZTracer::Trace &trace,
      ceph::bufferlist* data, Context* on_finish);

};

} // namespace plugin
} // namespace librbd

extern template class librbd::plugin::Api<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_PLUGIN_API_H
