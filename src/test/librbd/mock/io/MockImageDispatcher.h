// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_IO_IMAGE_DISPATCHER_H
#define CEPH_TEST_LIBRBD_MOCK_IO_IMAGE_DISPATCHER_H

#include "gmock/gmock.h"
#include "include/Context.h"
#include "librbd/io/ImageDispatcher.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/Types.h"

class Context;

namespace librbd {
namespace io {

struct ImageDispatchInterface;

struct MockImageDispatcher : public ImageDispatcherInterface {
public:
  MOCK_METHOD1(shut_down, void(Context*));

  MOCK_METHOD1(register_dispatch, void(ImageDispatchInterface*));
  MOCK_METHOD1(exists, bool(ImageDispatchLayer));
  MOCK_METHOD2(shut_down_dispatch, void(ImageDispatchLayer, Context*));
  MOCK_METHOD1(invalidate_cache, void(Context *));

  MOCK_METHOD1(send, void(ImageDispatchSpec*));
  MOCK_METHOD1(finished, void(ImageDispatchSpec*));
  MOCK_METHOD3(finish, void(int r, ImageDispatchLayer, uint64_t));

  MOCK_METHOD1(apply_qos_schedule_tick_min, void(uint64_t));
  MOCK_METHOD4(apply_qos_limit, void(uint64_t, uint64_t, uint64_t, uint64_t));
  MOCK_METHOD1(apply_qos_exclude_ops, void(uint64_t));

  MOCK_CONST_METHOD0(writes_blocked, bool());
  MOCK_METHOD0(block_writes, int());
  MOCK_METHOD1(block_writes, void(Context*));

  MOCK_METHOD0(unblock_writes, void());
  MOCK_METHOD1(wait_on_writes_unblocked, void(Context*));
};

} // namespace io
} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_IO_IMAGE_DISPATCHER_H
