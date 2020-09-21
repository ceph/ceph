// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_LIBRBD_MOCK_IO_QOS_IMAGE_DISPATCH_H
#define CEPH_TEST_LIBRBD_MOCK_IO_QOS_IMAGE_DISPATCH_H

#include "gmock/gmock.h"
#include "librbd/io/Types.h"
#include <atomic>

struct Context;

namespace librbd {
namespace io {

struct MockQosImageDispatch {
  MOCK_METHOD4(needs_throttle, bool(bool, const Extents&,
                                    std::atomic<uint32_t>*, Context*));
};

} // namespace io
} // namespace librbd

#endif // CEPH_TEST_LIBRBD_MOCK_IO_QOS_IMAGE_DISPATCH_H
