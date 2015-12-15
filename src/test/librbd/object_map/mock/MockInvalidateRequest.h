// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/object_map/InvalidateRequest.h"

// template definitions
#include "librbd/object_map/InvalidateRequest.cc"

namespace librbd {
namespace object_map {

template <>
struct InvalidateRequest<MockImageCtx> {
  static std::list<InvalidateRequest *> s_requests;
  uint64_t snap_id;
  bool force;
  Context *on_finish;

  static InvalidateRequest* create(MockImageCtx &image_ctx, uint64_t snap_id,
                                   bool force, Context *on_finish) {
    assert(!s_requests.empty());
    InvalidateRequest* req = s_requests.front();
    req->snap_id = snap_id;
    req->force = force;
    req->on_finish = on_finish;
    s_requests.pop_front();
    return req;
  }

  InvalidateRequest() {
    s_requests.push_back(this);
  }

  MOCK_METHOD0(send, void());
};

typedef InvalidateRequest<MockImageCtx> MockInvalidateRequest;

std::list<InvalidateRequest<MockImageCtx>*> InvalidateRequest<MockImageCtx>::s_requests;

} // namespace object_map
} // namespace librbd
