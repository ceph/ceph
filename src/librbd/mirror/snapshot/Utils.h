// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_UTILS_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_UTILS_H

#include "include/stringify.h"

namespace librbd {

struct ImageCtx;

namespace mirror {
namespace snapshot {
namespace util {

template <typename ImageCtxT = librbd::ImageCtx>
bool can_create_primary_snapshot(ImageCtxT *image_ctx, bool demoted, bool force,
                                 uint64_t *rollback_snap_id);

template <typename ImageCtxT = librbd::ImageCtx>
bool can_create_non_primary_snapshot(ImageCtxT *image_ctx);

template <typename ImageCtxT = librbd::ImageCtx>
std::string image_state_object_name(ImageCtxT *image_ctx, uint64_t snap_id,
                                    uint64_t index);

} // namespace util
} // namespace snapshot
} // namespace mirror
} // namespace librbd

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_UTILS_H
