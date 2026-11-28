// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/io/ObjectDispatchSpec.h"
#include "include/Context.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ObjectDispatcherInterface.h"
#include "librbd/io/ReadResult.h"

namespace librbd {
namespace io {

asio::ContextWQ::Channel
ObjectDispatchSpec::channel_from_on_finish(Context* on_finish) {
  if (on_finish == nullptr) {
    return nullptr;
  }
  if (auto* req = dynamic_cast<C_AioRequest*>(on_finish)) {
    return req->get_completion_channel();
  }
  if (auto* req = dynamic_cast<ReadResult::C_ObjectReadRequest*>(on_finish)) {
    if (req->aio_completion != nullptr) {
      return req->aio_completion->get_completion_channel();
    }
  }
  return nullptr;
}

asio::ContextWQ::Channel
ObjectDispatchSpec::saved_completion_channel(Context* ctx) {
  auto* dispatcher = dynamic_cast<C_Dispatcher*>(ctx);
  if (dispatcher == nullptr || dispatcher->object_dispatch_spec == nullptr) {
    return nullptr;
  }
  return dispatcher->object_dispatch_spec->completion_channel;
}

void ObjectDispatchSpec::C_Dispatcher::complete(int r) {
  if (r < 0) {
    finish(r);
    return;
  }

  switch (object_dispatch_spec->dispatch_result) {
  case DISPATCH_RESULT_CONTINUE:
    object_dispatch_spec->send();
    break;
  case DISPATCH_RESULT_COMPLETE:
    finish(r);
    break;
  case DISPATCH_RESULT_INVALID:
  case DISPATCH_RESULT_RESTART:
    ceph_abort();
    break;
  }
}

void ObjectDispatchSpec::C_Dispatcher::finish(int r) {
  on_finish->complete(r);
  delete object_dispatch_spec;
}

void ObjectDispatchSpec::send() {
  object_dispatcher->send(this);
}

void ObjectDispatchSpec::fail(int r) {
  ceph_assert(r < 0);
  dispatcher_ctx.complete(r);
}

} // namespace io
} // namespace librbd
