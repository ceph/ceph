// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_JOURNAL_POLICY_H
#define CEPH_LIBRBD_JOURNAL_POLICY_H

class Context;

namespace librbd {

namespace journal {

struct Policy {
  virtual ~Policy() {
  }

  virtual bool append_disabled() const = 0;
  virtual bool journal_disabled() const = 0;
  virtual void allocate_tag_on_lock(Context *on_finish) = 0;
};

} // namespace journal
} // namespace librbd

#endif // CEPH_LIBRBD_JOURNAL_POLICY_H
