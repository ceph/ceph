// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_SNAPSHOT_UTILS_H
#define RBD_MIRROR_IMAGE_REPLAYER_SNAPSHOT_UTILS_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "common/ceph_mutex.h"
#include "librbd/Types.h"
#include <map>

namespace rbd {
namespace mirror {
namespace image_replayer {
namespace snapshot {
namespace util {

uint64_t compute_remote_snap_id(
    const ceph::shared_mutex& local_image_lock,
    const std::map<librados::snap_t, librbd::SnapInfo>& local_snap_infos,
    uint64_t local_snap_id, const std::string& remote_mirror_uuid);

} // namespace util
} // namespace snapshot
} // namespace image_replayer
} // namespace mirror
} // namespace rbd

#endif // RBD_MIRROR_IMAGE_REPLAYER_SNAPSHOT_UTILS_H
