// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_AUTOMATIC_POLICY_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_AUTOMATIC_POLICY_H

#include "librbd/exclusive_lock/Policy.h"

namespace librbd {

struct ImageCtx;

namespace exclusive_lock {

class AutomaticPolicy : public Policy {
public:
  AutomaticPolicy(ImageCtx *image_ctx) : m_image_ctx(image_ctx) {
  }

  virtual bool may_auto_request_lock() {
    return true;
  }

  virtual int lock_requested(bool force);

private:
  ImageCtx *m_image_ctx;

};

} // namespace exclusive_lock
} // namespace librbd

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_AUTOMATIC_POLICY_H
