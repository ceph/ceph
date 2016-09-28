// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_ASYNC_OP_TRACKER_H
#define CEPH_ASYNC_OP_TRACKER_H

#include "include/int_types.h"
#include "common/Mutex.h"

struct Context;

class AsyncOpTracker {
public:
  AsyncOpTracker();
  ~AsyncOpTracker();

  void start_op();
  void finish_op();

  void wait_for_ops(Context *on_finish);

  bool empty();

private:
  Mutex m_lock;
  uint32_t m_pending_ops = 0;
  Context *m_on_finish = nullptr;

};

#endif // CEPH_ASYNC_OP_TRACKER_H
