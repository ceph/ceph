// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/ImageDispatcher.h"
#include "include/Context.h"
#include "common/AsyncOpTracker.h"
#include "common/dout.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/io/ImageDispatch.h"
#include "librbd/io/ImageDispatchInterface.h"
#include "librbd/io/ImageDispatchSpec.h"
#include <boost/variant.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::ImageDispatcher: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace io {

template <typename I>
struct ImageDispatcher<I>::SendVisitor : public boost::static_visitor<bool> {
  ImageDispatchInterface* image_dispatch;
  ImageDispatchSpec<I>* image_dispatch_spec;

  SendVisitor(ImageDispatchInterface* image_dispatch,
              ImageDispatchSpec<I>* image_dispatch_spec)
    : image_dispatch(image_dispatch),
      image_dispatch_spec(image_dispatch_spec) {
  }

  bool operator()(typename ImageDispatchSpec<I>::Read& read) const {
    return image_dispatch->read(
      image_dispatch_spec->aio_comp,
      std::move(image_dispatch_spec->image_extents),
      std::move(read.read_result), image_dispatch_spec->op_flags,
      image_dispatch_spec->parent_trace, image_dispatch_spec->tid,
      &image_dispatch_spec->image_dispatch_flags,
      &image_dispatch_spec->dispatch_result,
      &image_dispatch_spec->dispatcher_ctx);
  }

  bool operator()(typename ImageDispatchSpec<I>::Discard& discard) const {
    return image_dispatch->discard(
      image_dispatch_spec->aio_comp,
      std::move(image_dispatch_spec->image_extents),
      discard.discard_granularity_bytes,
      image_dispatch_spec->parent_trace, image_dispatch_spec->tid,
      &image_dispatch_spec->image_dispatch_flags,
      &image_dispatch_spec->dispatch_result,
      &image_dispatch_spec->dispatcher_ctx);
  }

  bool operator()(typename ImageDispatchSpec<I>::Write& write) const {
    return image_dispatch->write(
      image_dispatch_spec->aio_comp,
      std::move(image_dispatch_spec->image_extents), std::move(write.bl),
      image_dispatch_spec->op_flags, image_dispatch_spec->parent_trace,
      image_dispatch_spec->tid, &image_dispatch_spec->image_dispatch_flags,
      &image_dispatch_spec->dispatch_result,
      &image_dispatch_spec->dispatcher_ctx);
  }

  bool  operator()(typename ImageDispatchSpec<I>::WriteSame& write_same) const {
    return image_dispatch->write_same(
      image_dispatch_spec->aio_comp,
      std::move(image_dispatch_spec->image_extents), std::move(write_same.bl),
      image_dispatch_spec->op_flags, image_dispatch_spec->parent_trace,
      image_dispatch_spec->tid, &image_dispatch_spec->image_dispatch_flags,
      &image_dispatch_spec->dispatch_result,
      &image_dispatch_spec->dispatcher_ctx);
  }

  bool  operator()(
      typename ImageDispatchSpec<I>::CompareAndWrite& compare_and_write) const {
    return image_dispatch->compare_and_write(
      image_dispatch_spec->aio_comp,
      std::move(image_dispatch_spec->image_extents),
      std::move(compare_and_write.cmp_bl), std::move(compare_and_write.bl),
      compare_and_write.mismatch_offset, image_dispatch_spec->op_flags,
      image_dispatch_spec->parent_trace, image_dispatch_spec->tid,
      &image_dispatch_spec->image_dispatch_flags,
      &image_dispatch_spec->dispatch_result,
      &image_dispatch_spec->dispatcher_ctx);
  }

  bool operator()(typename ImageDispatchSpec<I>::Flush& flush) const {
    return image_dispatch->flush(
      image_dispatch_spec->aio_comp, flush.flush_source,
      image_dispatch_spec->parent_trace, image_dispatch_spec->tid,
      &image_dispatch_spec->image_dispatch_flags,
      &image_dispatch_spec->dispatch_result,
      &image_dispatch_spec->dispatcher_ctx);
  }
};

template <typename I>
ImageDispatcher<I>::ImageDispatcher(I* image_ctx)
  : Dispatcher<I, ImageDispatcherInterface>(image_ctx) {
  // configure the core image dispatch handler on startup
  auto image_dispatch = new ImageDispatch(image_ctx);
  this->register_dispatch(image_dispatch);
}

template <typename I>
bool ImageDispatcher<I>::send_dispatch(
    ImageDispatchInterface* image_dispatch,
    ImageDispatchSpec<I>* image_dispatch_spec) {
  return boost::apply_visitor(
    SendVisitor{image_dispatch, image_dispatch_spec},
    image_dispatch_spec->request);
}

} // namespace io
} // namespace librbd

template class librbd::io::ImageDispatcher<librbd::ImageCtx>;
