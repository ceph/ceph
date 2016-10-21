// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MANAGED_LOCK_NORELEASE_POLICY_H
#define CEPH_LIBRBD_MANAGED_LOCK_NORELEASE_POLICY_H

#include "librbd/managed_lock/Policy.h"

namespace librbd {

namespace managed_lock {

class NoReleasePolicy : public Policy {
public:
  virtual int lock_requested(bool force) {
    return -EROFS;
  }
};

} // namespace managed_lock
} // namespace librbd

#endif // CEPH_LIBRBD_MANAGED_LOCK_NORELEASED_POLICY_H
