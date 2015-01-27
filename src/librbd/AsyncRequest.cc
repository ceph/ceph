// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "librbd/AsyncRequest.h"
#include "librbd/internal.h"
#include <boost/bind.hpp>

namespace librbd
{

librados::AioCompletion *AsyncRequest::create_callback_completion() {
  return librados::Rados::aio_create_completion(create_callback_context(),
						NULL, rados_ctx_cb);
}

Context *AsyncRequest::create_callback_context() {
  return new FunctionContext(boost::bind(&AsyncRequest::complete, this, _1));
}

} // namespace librbd
