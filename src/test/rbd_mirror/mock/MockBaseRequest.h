// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_MOCK_BASE_REQUEST_H
#define CEPH_MOCK_BASE_REQUEST_H

#include "tools/rbd_mirror/BaseRequest.h"
#include <gmock/gmock.h>

struct Context;

namespace rbd {
namespace mirror {

struct MockBaseRequest : public BaseRequest {
  MockBaseRequest() : BaseRequest(nullptr) {}

  Context* on_finish = nullptr;

  MOCK_METHOD0(send, void());
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_MOCK_BASE_REQUEST_H
