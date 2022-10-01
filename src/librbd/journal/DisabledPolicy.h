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
  bool append_disabled() const override {
    return true;
  }
  bool journal_disabled() const override {
    return true;
  }
  void allocate_tag_on_lock(Context *on_finish) override {
    ceph_abort();
  }
};

} // namespace journal
} // namespace librbd

#endif // CEPH_LIBRBD_JOURNAL_DISABLED_POLICY_H
