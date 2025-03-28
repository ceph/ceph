// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

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

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_GROUP_REPLAYER_TYPES_H
