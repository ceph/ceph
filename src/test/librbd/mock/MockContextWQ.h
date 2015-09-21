// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_CONTEXT_WQ_H
#define CEPH_TEST_LIBRBD_MOCK_CONTEXT_WQ_H

#include "gmock/gmock.h"

namespace librbd {

struct MockContextWQ {
  MOCK_METHOD2(queue, void(Context *, int r));
};

} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_CONTEXT_WQ_H
