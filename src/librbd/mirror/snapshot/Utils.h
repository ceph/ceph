// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_UTILS_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_UTILS_H

#include "include/stringify.h"

namespace librbd {
namespace mirror {
namespace snapshot {
namespace util {

const std::string IMAGE_STATE_OBJECT_PREFIX = "rbd_mirror_snapshot.";

template <typename ImageCtxT = librbd::ImageCtx>
std::string image_state_object_name(ImageCtxT *image_ctx, uint64_t snap_id,
                                    uint64_t index) {
  return IMAGE_STATE_OBJECT_PREFIX + image_ctx->id + "." +
    stringify(snap_id) + "." + stringify(index);
}

} // namespace util
} // namespace snapshot
} // namespace mirror
} // namespace librbd

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_UTILS_H
