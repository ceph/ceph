// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef CEPH_LIBRBD_ASYNC_REQUEST_H
#define CEPH_LIBRBD_ASYNC_REQUEST_H

#include "include/int_types.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "include/xlist.h"
#include "include/compat.h"

namespace librbd {

class ImageCtx;

template <typename ImageCtxT = ImageCtx>
class AsyncRequest
{
public:
  AsyncRequest(ImageCtxT &image_ctx, Context *on_finish);
  virtual ~AsyncRequest();

  void complete(int r) {
    if (should_complete(r)) {
      r = filter_return_code(r);
      finish(r);
      delete this;
    }
  }

  virtual void send() = 0;

  inline bool is_canceled() const {
    return m_canceled;
  }
  inline void cancel() {
    m_canceled = true;
  }

protected:
  ImageCtxT &m_image_ctx;

  librados::AioCompletion *create_callback_completion();
  Context *create_callback_context();
  Context *create_async_callback_context();

  void async_complete(int r);

  virtual bool should_complete(int r) = 0;
  virtual int filter_return_code(int r) const {
    return r;
  }

  virtual void finish(int r) {
    finish_request();
    m_on_finish->complete(r);
  }

private:
  Context *m_on_finish;
  bool m_canceled;
  typename xlist<AsyncRequest<ImageCtxT> *>::item m_xlist_item;

  void start_request();
  void finish_request();
};

} // namespace librbd

extern template class librbd::AsyncRequest<librbd::ImageCtx>;

#endif //CEPH_LIBRBD_ASYNC_REQUEST_H
