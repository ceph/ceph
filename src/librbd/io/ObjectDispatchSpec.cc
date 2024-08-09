// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/ObjectDispatchSpec.h"
#include "include/Context.h"
#include "librbd/io/ObjectDispatcherInterface.h"
#include <boost/variant.hpp>

namespace librbd {
namespace io {

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
  case DISPATCH_RESULT_INIT:
  case DISPATCH_RESULT_INVALID:
  case DISPATCH_RESULT_RESTART:
    ceph_abort();
    break;
  }
}

void ObjectDispatchSpec::C_Dispatcher::finish(int r) {
  object_dispatch_spec->object_dispatcher->finished(this->object_dispatch_spec);
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
