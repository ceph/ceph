// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/ImageCtx.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ImageDispatcherInterface.h"
#include <boost/variant.hpp>

namespace librbd {
namespace io {

void ImageDispatchSpec::C_Dispatcher::complete(int r) {
  switch (image_dispatch_spec->dispatch_result) {
  case DISPATCH_RESULT_RESTART:
    ceph_assert(image_dispatch_spec->dispatch_layer != 0);
    image_dispatch_spec->dispatch_layer = static_cast<ImageDispatchLayer>(
      image_dispatch_spec->dispatch_layer - 1);
    [[fallthrough]];
  case DISPATCH_RESULT_CONTINUE:
    if (r < 0) {
      // bubble dispatch failure through AioCompletion
      image_dispatch_spec->dispatch_result = DISPATCH_RESULT_COMPLETE;
      image_dispatch_spec->fail(r);
      return;
    }

    image_dispatch_spec->send();
    break;
  case DISPATCH_RESULT_COMPLETE:
    finish(r);
    break;
  case DISPATCH_RESULT_INVALID:
  case DISPATCH_RESULT_INIT:
    ceph_abort();
    break;
  }
}

void ImageDispatchSpec::C_Dispatcher::finish(int r) {
  image_dispatch_spec->image_dispatcher->finished(this->image_dispatch_spec);
  delete image_dispatch_spec;
}

void ImageDispatchSpec::send() {
  image_dispatcher->send(this);
}

void ImageDispatchSpec::fail(int r) {
  dispatch_result = DISPATCH_RESULT_COMPLETE;
  aio_comp->fail(r);
}

} // namespace io
} // namespace librbd
