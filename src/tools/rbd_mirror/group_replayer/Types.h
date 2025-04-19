// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_RBD_MIRROR_GROUP_REPLAYER_TYPES_H
#define CEPH_RBD_MIRROR_GROUP_REPLAYER_TYPES_H

namespace rbd {
namespace mirror {
namespace group_replayer {

enum HealthState {
  HEALTH_STATE_OK,
  HEALTH_STATE_WARNING,
  HEALTH_STATE_ERROR
};

typedef std::pair<int64_t /*pool_id*/, std::string /*global_image_id*/> GlobalImageId;

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_GROUP_REPLAYER_TYPES_H
