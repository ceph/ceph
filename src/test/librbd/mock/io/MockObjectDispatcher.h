// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_IO_OBJECT_DISPATCHER_H
#define CEPH_TEST_LIBRBD_MOCK_IO_OBJECT_DISPATCHER_H

#include "gmock/gmock.h"
#include "include/Context.h"
#include "librbd/io/ObjectDispatcher.h"
#include "librbd/io/ObjectDispatchSpec.h"
#include "librbd/io/Types.h"

class Context;

namespace librbd {
namespace io {

struct ObjectDispatchInterface;

struct MockObjectDispatcher : public ObjectDispatcherInterface {
public:
  MOCK_METHOD1(shut_down, void(Context*));

  MOCK_METHOD1(register_dispatch, void(ObjectDispatchInterface*));
  MOCK_METHOD1(exists, bool(ObjectDispatchLayer));
  MOCK_METHOD2(shut_down_dispatch, void(ObjectDispatchLayer, Context*));

  MOCK_METHOD2(flush, void(FlushSource, Context*));

  MOCK_METHOD1(invalidate_cache, void(Context*));
  MOCK_METHOD1(reset_existence_cache, void(Context*));

  MOCK_METHOD5(extent_overwritten, void(uint64_t, uint64_t, uint64_t, uint64_t,
                                        uint64_t));

  MOCK_METHOD2(prepare_copyup, int(uint64_t, SnapshotSparseBufferlist*));

  MOCK_METHOD1(send, void(ObjectDispatchSpec*));
  MOCK_METHOD1(finished, void(ObjectDispatchSpec*));
};

} // namespace io
} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_IO_OBJECT_DISPATCHER_H
