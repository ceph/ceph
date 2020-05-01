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
  MOCK_METHOD2(shut_down_dispatch, void(ImageDispatchLayer, Context*));

  MOCK_METHOD1(send, void(ImageDispatchSpec<>*));
};

} // namespace io
} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_IO_IMAGE_DISPATCHER_H
