// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_ASYNC_REQUEST_H
#define CEPH_LIBRBD_ASYNC_REQUEST_H

#include "include/int_types.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"

namespace librbd {

class ImageCtx;

class AsyncRequest
{
public:
  AsyncRequest(ImageCtx &image_ctx, Context *on_finish)
    : m_image_ctx(image_ctx), m_on_finish(on_finish)
  {
  }

  virtual ~AsyncRequest() {}

  void complete(int r) {
    if (should_complete(r)) {
      m_on_finish->complete(r);
      delete this;
    }
  }

  virtual void send() = 0;

protected:
  ImageCtx &m_image_ctx;
  Context *m_on_finish;

  librados::AioCompletion *create_callback_completion();
  Context *create_callback_context();

  virtual bool should_complete(int r) = 0;
};

class C_AsyncRequest : public Context
{
public:
  C_AsyncRequest(AsyncRequest *req)
    : m_req(req)
  {
  }

protected:
  virtual void finish(int r) {
    m_req->complete(r);
  }

private:
  AsyncRequest *m_req;
};

} // namespace librbd

#endif //CEPH_LIBRBD_ASYNC_REQUEST_H
