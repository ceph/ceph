// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_POLICY_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_POLICY_H

namespace librbd {
namespace exclusive_lock {

enum OperationRequestType {
  OPERATION_REQUEST_TYPE_GENERAL           = 0,
  OPERATION_REQUEST_TYPE_TRASH_SNAP_REMOVE = 1,
};

struct Policy {
  virtual ~Policy() {
  }

  virtual bool may_auto_request_lock() = 0;
  virtual int lock_requested(bool force) = 0;

  virtual bool accept_blocked_request(OperationRequestType) {
    return false;
  }
};

} // namespace exclusive_lock
} // namespace librbd

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_POLICY_H
