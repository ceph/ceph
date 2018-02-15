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

struct MockObjectDispatcher : public ObjectDispatcherInterface {
public:
  MOCK_METHOD1(send, void(ObjectDispatchSpec*));

};

} // namespace io
} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_IO_OBJECT_DISPATCHER_H
