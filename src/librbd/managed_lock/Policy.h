// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_LOCK_POLICY_H
#define CEPH_LIBRBD_LOCK_POLICY_H

namespace librbd {
namespace managed_lock {

struct Policy {
  virtual ~Policy() {
  }

  virtual int lock_requested(bool force) = 0;
};

} // namespace managed_lock
} // namespace librbd

#endif // CEPH_LIBRBD_LOCK_POLICY_H
