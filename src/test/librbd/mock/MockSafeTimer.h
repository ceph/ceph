// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MOCK_SAFE_TIMER_H
#define CEPH_MOCK_SAFE_TIMER_H

#include <gmock/gmock.h>

struct Context;

struct MockSafeTimer {
  virtual ~MockSafeTimer() {
  }

  MOCK_METHOD2(add_event_after, Context*(double, Context *));
  MOCK_METHOD2(add_event_at, Context*(utime_t, Context *));
  MOCK_METHOD1(cancel_event, bool(Context *));
};

#endif // CEPH_MOCK_SAFE_TIMER_H
