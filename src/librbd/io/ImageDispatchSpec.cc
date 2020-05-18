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

template <typename I>
void ImageDispatchSpec<I>::C_Dispatcher::complete(int r) {
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
    ceph_abort();
    break;
  }
}

template <typename I>
void ImageDispatchSpec<I>::C_Dispatcher::finish(int r) {
  image_dispatch_spec->finish(r);
}

template <typename I>
struct ImageDispatchSpec<I>::IsWriteOpVisitor
  : public boost::static_visitor<bool> {
  bool operator()(const Read&) const {
    return false;
  }

  template <typename T>
  bool operator()(const T&) const {
    return true;
  }
};

template <typename I>
void ImageDispatchSpec<I>::send() {
  image_dispatcher->send(this);
}

template <typename I>
void ImageDispatchSpec<I>::finish(int r) {
  image_dispatcher->finish(r, dispatch_layer, tid);
  delete this;
}

template <typename I>
void ImageDispatchSpec<I>::fail(int r) {
  dispatch_result = DISPATCH_RESULT_COMPLETE;
  aio_comp->fail(r);
}

template <typename I>
uint64_t ImageDispatchSpec<I>::extents_length() {
  uint64_t length = 0;
  auto &extents = this->image_extents;

  for (auto &extent : extents) {
    length += extent.second;
  }
  return length;
}

template <typename I>
const Extents& ImageDispatchSpec<I>::get_image_extents() const {
   return this->image_extents;
}

template <typename I>
uint64_t ImageDispatchSpec<I>::get_tid() {
  return this->tid;
}

template <typename I>
bool ImageDispatchSpec<I>::is_write_op() const {
  return boost::apply_visitor(IsWriteOpVisitor(), request);
}

template <typename I>
void ImageDispatchSpec<I>::start_op() {
  tid = 0;
  aio_comp->start_op();
}

} // namespace io
} // namespace librbd

template class librbd::io::ImageDispatchSpec<librbd::ImageCtx>;
