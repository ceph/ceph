// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_JOURNAL_ASYNC_OP_TRACKER_H
#define CEPH_JOURNAL_ASYNC_OP_TRACKER_H

#include "include/int_types.h"
#include "common/Cond.h"
#include "common/Mutex.h"

namespace journal {

class AsyncOpTracker {
public:
  AsyncOpTracker();
  ~AsyncOpTracker();

  void start_op();
  void finish_op();

  void wait_for_ops();

private:
  Mutex m_lock;
  Cond m_cond;
  uint32_t m_pending_ops;

};

} // namespace journal

#endif // CEPH_JOURNAL_ASYNC_OP_TRACKER_H
