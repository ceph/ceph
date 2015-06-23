// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "librbd/AsyncRequest.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include <boost/bind.hpp>

namespace librbd
{

AsyncRequest::AsyncRequest(ImageCtx &image_ctx, Context *on_finish)
  : m_image_ctx(image_ctx), m_on_finish(on_finish), m_canceled(false),
    m_xlist_item(this) {
  Mutex::Locker l(m_image_ctx.async_ops_lock);
  m_image_ctx.async_requests.push_back(&m_xlist_item);
}

AsyncRequest::~AsyncRequest() {
  Mutex::Locker l(m_image_ctx.async_ops_lock);
  assert(m_xlist_item.remove_myself());
  m_image_ctx.async_requests_cond.Signal();
}

void AsyncRequest::async_complete(int r) {
  m_image_ctx.op_work_queue->queue(create_callback_context(), r);
}

librados::AioCompletion *AsyncRequest::create_callback_completion() {
  return librados::Rados::aio_create_completion(create_callback_context(),
						NULL, rados_ctx_cb);
}

Context *AsyncRequest::create_callback_context() {
  return new FunctionContext(boost::bind(&AsyncRequest::complete, this, _1));
}

Context *AsyncRequest::create_async_callback_context() {
  return new FunctionContext(boost::bind(&AsyncRequest::async_complete, this,
                                         _1));;
}

} // namespace librbd
