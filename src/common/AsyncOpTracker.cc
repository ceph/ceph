// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/AsyncOpTracker.h"
#include "include/assert.h"
#include "include/Context.h"

AsyncOpTracker::AsyncOpTracker()
  : m_lock("AsyncOpTracker::m_lock", false, false) {
}

AsyncOpTracker::~AsyncOpTracker() {
  Mutex::Locker locker(m_lock);
  assert(m_pending_ops == 0);
}

void AsyncOpTracker::start_op() {
  Mutex::Locker locker(m_lock);
  ++m_pending_ops;
}

void AsyncOpTracker::finish_op() {
  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);
    assert(m_pending_ops > 0);
    if (--m_pending_ops == 0) {
      std::swap(on_finish, m_on_finish);
    }
  }

  if (on_finish != nullptr) {
    on_finish->complete(0);
  }
}

void AsyncOpTracker::wait_for_ops(Context *on_finish) {
  {
    Mutex::Locker locker(m_lock);
    assert(m_on_finish == nullptr);
    if (m_pending_ops > 0) {
      m_on_finish = on_finish;
      return;
    }
  }
  on_finish->complete(0);
}

bool AsyncOpTracker::empty() {
  Mutex::Locker locker(m_lock);
  return (m_pending_ops == 0);
}

