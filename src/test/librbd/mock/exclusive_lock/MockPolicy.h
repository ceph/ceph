// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_EXCLUSIVE_LOCK_POLICY_H
#define CEPH_TEST_LIBRBD_MOCK_EXCLUSIVE_LOCK_POLICY_H

#include "librbd/exclusive_lock/Policy.h"
#include <gmock/gmock.h>

namespace librbd {
namespace exclusive_lock {

struct MockPolicy : public Policy {

  MOCK_METHOD0(may_auto_request_lock, bool());
  MOCK_METHOD1(lock_requested, int(bool));

};

} // namespace exclusive_lock
} // librbd

#endif
