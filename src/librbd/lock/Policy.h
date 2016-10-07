// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_LOCK_POLICY_H
#define CEPH_LIBRBD_LOCK_POLICY_H

namespace librbd {
namespace lock {

struct Policy {
  virtual ~Policy() {
  }

  virtual bool may_auto_request_lock() = 0;
  virtual int lock_requested(bool force) = 0;
};

} // namespace lock
} // namespace librbd

#endif // CEPH_LIBRBD_LOCK_POLICY_H
