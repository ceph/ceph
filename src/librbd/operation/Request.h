// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_OPERATION_REQUEST_H
#define CEPH_LIBRBD_OPERATION_REQUEST_H

#include "librbd/AsyncRequest.h"

namespace librbd {
namespace operation {

class Request : public AsyncRequest {
public:
  Request(ImageCtx &image_ctx, Context *on_finish);

  virtual void send();

protected:
  virtual void finish(int r);
  virtual void send_op() = 0;

};

} // namespace operation
} // namespace librbd

#endif // CEPH_LIBRBD_OPERATION_REQUEST_H
