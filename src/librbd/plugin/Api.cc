// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/plugin/Api.h"
#include "librbd/ImageCtx.h"
#include "librbd/io/Utils.h"

namespace librbd {
namespace plugin {

template <typename I>
void Api<I>::read_parent(
    I *image_ctx, uint64_t object_no, const Extents &extents,
    librados::snap_t snap_id, const ZTracer::Trace &trace,
    ceph::bufferlist* data, Context* on_finish) {
  io::util::read_parent<I>(image_ctx, object_no, extents, snap_id, trace, data,
                           on_finish);
}

} // namespace plugin
} // namespace librbd

template class librbd::plugin::Api<librbd::ImageCtx>;
