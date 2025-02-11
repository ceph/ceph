// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_STANDARD_POLICY_H
#define CEPH_LIBRBD_JOURNAL_STANDARD_POLICY_H

#include "librbd/journal/Policy.h"

namespace librbd {

struct ImageCtx;

namespace journal {

template<typename ImageCtxT = ImageCtx>
class StandardPolicy : public Policy {
public:
  StandardPolicy(ImageCtxT *image_ctx) : m_image_ctx(image_ctx) {
  }

  bool append_disabled() const override {
    return false;
  }
  bool journal_disabled() const override {
    return false;
  }
  void allocate_tag_on_lock(Context *on_finish) override;

private:
  ImageCtxT *m_image_ctx;
};

} // namespace journal
} // namespace librbd

extern template class librbd::journal::StandardPolicy<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_JOURNAL_STANDARD_POLICY_H
