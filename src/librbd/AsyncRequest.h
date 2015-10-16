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
    if (m_canceled && safely_cancel(r)) {
      m_on_finish->complete(-ERESTART);
      delete this;
    } else if (should_complete(r)) {
      m_on_finish->complete(filter_return_code(r));
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
  Context *m_on_finish;

  librados::AioCompletion *create_callback_completion();
  Context *create_callback_context();
  Context *create_async_callback_context();

  void async_complete(int r);

  virtual bool safely_cancel(int r) {
    return true;
  }
  virtual bool should_complete(int r) = 0;
  virtual int filter_return_code(int r) {
    return r;
  }
private:
  bool m_canceled;
  typename xlist<AsyncRequest<ImageCtxT> *>::item m_xlist_item;
};

} // namespace librbd

extern template class librbd::AsyncRequest<librbd::ImageCtx>;

#endif //CEPH_LIBRBD_ASYNC_REQUEST_H
