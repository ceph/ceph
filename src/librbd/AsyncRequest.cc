// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "librbd/AsyncRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "common/WorkQueue.h"
#include <boost/bind.hpp>

namespace librbd
{

template <typename T>
AsyncRequest<T>::AsyncRequest(T &image_ctx, Context *on_finish)
  : m_image_ctx(image_ctx), m_on_finish(on_finish), m_canceled(false),
    m_xlist_item(this) {
  assert(m_on_finish != NULL);
  Mutex::Locker l(m_image_ctx.async_ops_lock);
  m_image_ctx.async_requests.push_back(&m_xlist_item);
}

template <typename T>
AsyncRequest<T>::~AsyncRequest() {
  Mutex::Locker l(m_image_ctx.async_ops_lock);
  assert(m_xlist_item.remove_myself());
  m_image_ctx.async_requests_cond.Signal();
}

template <typename T>
void AsyncRequest<T>::async_complete(int r) {
  m_image_ctx.op_work_queue->queue(create_callback_context(), r);
}

template <typename T>
librados::AioCompletion *AsyncRequest<T>::create_callback_completion() {
  return librados::Rados::aio_create_completion(create_callback_context(),
						NULL, rados_ctx_cb);
}

template <typename T>
Context *AsyncRequest<T>::create_callback_context() {
  return new FunctionContext(boost::bind(&AsyncRequest<T>::complete, this, _1));
}

template <typename T>
Context *AsyncRequest<T>::create_async_callback_context() {
  return new FunctionContext(boost::bind(&AsyncRequest<T>::async_complete, this,
                                         _1));;
}

} // namespace librbd

template class librbd::AsyncRequest<librbd::ImageCtx>;
