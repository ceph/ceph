// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_EXCLUSIVE_LOCK_H
#define CEPH_TEST_LIBRBD_MOCK_EXCLUSIVE_LOCK_H

#include "common/RefCountedObj.h"
#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "librbd/exclusive_lock/Policy.h"
#include "librbd/io/Types.h"
#include "gmock/gmock.h"

class Context;

namespace librbd {

struct MockExclusiveLock {
  MOCK_CONST_METHOD0(is_lock_owner, bool());

  MOCK_METHOD2(init, void(uint64_t features, Context*));
  MOCK_METHOD1(shut_down, void(Context*));

  MOCK_METHOD1(reacquire_lock, void(Context*));
  MOCK_METHOD1(try_acquire_lock, void(Context*));

  MOCK_METHOD1(block_requests, void(int));
  MOCK_METHOD0(unblock_requests, void());

  MOCK_METHOD1(acquire_lock, void(Context *));
  MOCK_METHOD1(release_lock, void(Context *));

  MOCK_METHOD2(accept_request, bool(exclusive_lock::OperationRequestType,
                                    int *));
  MOCK_METHOD0(accept_ops, bool());
  MOCK_METHOD0(get_unlocked_op_error, int());

  MOCK_METHOD3(set_require_lock, void(bool init_shutdown, io::Direction,
                                      Context*));
  MOCK_METHOD1(unset_require_lock, void(io::Direction));

  MOCK_METHOD1(start_op, Context*(int*));

  void get() {}
  void put() {}
};

} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_EXCLUSIVE_LOCK_H
