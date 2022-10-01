// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MOCK_CONTEXT_WQ_H
#define CEPH_MOCK_CONTEXT_WQ_H

#include <gmock/gmock.h>

struct Context;

struct MockContextWQ {
  void queue(Context *ctx) {
    queue(ctx, 0);
  }
  MOCK_METHOD2(queue, void(Context *, int));
};

#endif // CEPH_MOCK_CONTEXT_WQ_H
