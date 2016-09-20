// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_STANDARD_POLICY_H
#define CEPH_LIBRBD_JOURNAL_STANDARD_POLICY_H

#include "librbd/journal/Policy.h"

namespace librbd {

struct ImageCtx;

namespace journal {

class StandardPolicy : public Policy {
public:
  StandardPolicy(ImageCtx *image_ctx) : m_image_ctx(image_ctx) {
  }

  virtual bool append_disabled() const {
    return false;
  }
  virtual bool journal_disabled() const {
    return false;
  }
  virtual void allocate_tag_on_lock(Context *on_finish);

private:
  ImageCtx *m_image_ctx;
};

} // namespace journal
} // namespace librbd

#endif // CEPH_LIBRBD_JOURNAL_STANDARD_POLICY_H
