// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_TYPES_H
#define CEPH_LIBRBD_MIRROR_TYPES_H

namespace librbd {
namespace mirror {

enum PromotionState {
  PROMOTION_STATE_PRIMARY,
  PROMOTION_STATE_NON_PRIMARY,
  PROMOTION_STATE_ORPHAN
};

} // namespace mirror
} // namespace librbd

#endif // CEPH_LIBRBD_MIRROR_TYPES_H

