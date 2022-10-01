// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_STANDARD_POLICY_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_STANDARD_POLICY_H

#include "librbd/exclusive_lock/Policy.h"

namespace librbd {

struct ImageCtx;

namespace exclusive_lock {

template <typename ImageCtxT = ImageCtx>
class StandardPolicy : public Policy {
public:
  StandardPolicy(ImageCtxT* image_ctx) : m_image_ctx(image_ctx) {
  }

  bool may_auto_request_lock() override {
    return false;
  }

  int lock_requested(bool force) override;

private:
  ImageCtxT* m_image_ctx;

};

} // namespace exclusive_lock
} // namespace librbd

extern template class librbd::exclusive_lock::StandardPolicy<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_STANDARD_POLICY_H
