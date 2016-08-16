// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_DISABLED_POLICY_H
#define CEPH_LIBRBD_JOURNAL_DISABLED_POLICY_H

#include "librbd/journal/Policy.h"

namespace librbd {

struct ImageCtx;

namespace journal {

class DisabledPolicy : public Policy {
public:
  virtual bool append_disabled() const {
    assert(false);
    return false;
  }
  virtual bool journal_disabled() const {
    return true;
  }
  virtual void allocate_tag_on_lock(Context *on_finish) {
    assert(false);
  }
};

} // namespace journal
} // namespace librbd

#endif // CEPH_LIBRBD_JOURNAL_DISABLED_POLICY_H
