// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_ASYNC_OP_TRACKER_H
#define CEPH_ASYNC_OP_TRACKER_H

#include "common/ceph_mutex.h"
#include "include/Context.h"

class AsyncOpTracker {
public:
  AsyncOpTracker();
  ~AsyncOpTracker();

  void start_op();
  void finish_op();

  void wait_for_ops(Context *on_finish, Context *on_finish_locked = nullptr);

  bool empty();

private:
  ceph::mutex m_lock = ceph::make_mutex("AsyncOpTracker::m_lock");
  uint32_t m_pending_ops = 0;
  Context *m_on_finish = nullptr;
  Context* m_on_finish_locked = nullptr;
};

class C_TrackedOp : public Context {
public:
  C_TrackedOp(AsyncOpTracker& async_op_tracker, Context* on_finish)
    : m_async_op_tracker(async_op_tracker), m_on_finish(on_finish) {
    m_async_op_tracker.start_op();
  }

  void finish(int r) override {
    if (m_on_finish != nullptr) {
      m_on_finish->complete(r);
    }
    m_async_op_tracker.finish_op();
  }

private:
  AsyncOpTracker& m_async_op_tracker;
  Context* m_on_finish;
};

#endif // CEPH_ASYNC_OP_TRACKER_H
