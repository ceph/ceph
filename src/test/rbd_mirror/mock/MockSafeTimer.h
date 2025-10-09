// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_MOCK_SAFE_TIMER_H
#define CEPH_MOCK_SAFE_TIMER_H

#include <gmock/gmock.h>

struct Context;

struct MockSafeTimer {
  MOCK_METHOD2(add_event_after, Context*(double, Context*));
  MOCK_METHOD1(cancel_event, bool(Context *));
};

#endif // CEPH_MOCK_SAFE_TIMER_H
