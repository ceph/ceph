// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/ImageDispatcher.h"
#include "include/Context.h"
#include "common/AsyncOpTracker.h"
#include "common/dout.h"
#include "librbd/ImageCtx.h"
#include "librbd/io/ImageDispatch.h"
#include "librbd/io/ImageDispatchInterface.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/QueueImageDispatch.h"
#include "librbd/io/QosImageDispatch.h"
#include "librbd/io/RefreshImageDispatch.h"
#include "librbd/io/Utils.h"
#include "librbd/io/WriteBlockImageDispatch.h"
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
  ImageDispatchSpec* image_dispatch_spec;

  SendVisitor(ImageDispatchInterface* image_dispatch,
              ImageDispatchSpec* image_dispatch_spec)
    : image_dispatch(image_dispatch),
      image_dispatch_spec(image_dispatch_spec) {
  }

  bool operator()(ImageDispatchSpec::Read& read) const {
    return image_dispatch->read(
      image_dispatch_spec->aio_comp,
      std::move(image_dispatch_spec->image_extents),
      std::move(read.read_result), image_dispatch_spec->io_context,
      image_dispatch_spec->op_flags, read.read_flags,
      image_dispatch_spec->parent_trace, image_dispatch_spec->tid,
      &image_dispatch_spec->image_dispatch_flags,
      &image_dispatch_spec->dispatch_result,
      &image_dispatch_spec->aio_comp->image_dispatcher_ctx,
      &image_dispatch_spec->dispatcher_ctx);
  }

  bool operator()(ImageDispatchSpec::Discard& discard) const {
    return image_dispatch->discard(
      image_dispatch_spec->aio_comp,
      std::move(image_dispatch_spec->image_extents),
      discard.discard_granularity_bytes, image_dispatch_spec->parent_trace,
      image_dispatch_spec->tid, &image_dispatch_spec->image_dispatch_flags,
      &image_dispatch_spec->dispatch_result,
      &image_dispatch_spec->aio_comp->image_dispatcher_ctx,
      &image_dispatch_spec->dispatcher_ctx);
  }

  bool operator()(ImageDispatchSpec::Write& write) const {
    return image_dispatch->write(
      image_dispatch_spec->aio_comp,
      std::move(image_dispatch_spec->image_extents), std::move(write.bl),
      image_dispatch_spec->op_flags, image_dispatch_spec->parent_trace,
      image_dispatch_spec->tid, &image_dispatch_spec->image_dispatch_flags,
      &image_dispatch_spec->dispatch_result,
      &image_dispatch_spec->aio_comp->image_dispatcher_ctx,
      &image_dispatch_spec->dispatcher_ctx);
  }

  bool operator()(ImageDispatchSpec::WriteSame& write_same) const {
    return image_dispatch->write_same(
      image_dispatch_spec->aio_comp,
      std::move(image_dispatch_spec->image_extents), std::move(write_same.bl),
      image_dispatch_spec->op_flags, image_dispatch_spec->parent_trace,
      image_dispatch_spec->tid, &image_dispatch_spec->image_dispatch_flags,
      &image_dispatch_spec->dispatch_result,
      &image_dispatch_spec->aio_comp->image_dispatcher_ctx,
      &image_dispatch_spec->dispatcher_ctx);
  }

  bool operator()(
      ImageDispatchSpec::CompareAndWrite& compare_and_write) const {
    return image_dispatch->compare_and_write(
      image_dispatch_spec->aio_comp,
      std::move(image_dispatch_spec->image_extents),
      std::move(compare_and_write.cmp_bl), std::move(compare_and_write.bl),
      compare_and_write.mismatch_offset,
      image_dispatch_spec->op_flags, image_dispatch_spec->parent_trace,
      image_dispatch_spec->tid, &image_dispatch_spec->image_dispatch_flags,
      &image_dispatch_spec->dispatch_result,
      &image_dispatch_spec->aio_comp->image_dispatcher_ctx,
      &image_dispatch_spec->dispatcher_ctx);
  }

  bool operator()(ImageDispatchSpec::Flush& flush) const {
    return image_dispatch->flush(
      image_dispatch_spec->aio_comp, flush.flush_source,
      image_dispatch_spec->parent_trace, image_dispatch_spec->tid,
      &image_dispatch_spec->image_dispatch_flags,
      &image_dispatch_spec->dispatch_result,
      &image_dispatch_spec->aio_comp->image_dispatcher_ctx,
      &image_dispatch_spec->dispatcher_ctx);
  }

  bool operator()(ImageDispatchSpec::ListSnaps& list_snaps) const {
    return image_dispatch->list_snaps(
      image_dispatch_spec->aio_comp,
      std::move(image_dispatch_spec->image_extents),
      std::move(list_snaps.snap_ids), list_snaps.list_snaps_flags,
      list_snaps.snapshot_delta, image_dispatch_spec->parent_trace,
      image_dispatch_spec->tid, &image_dispatch_spec->image_dispatch_flags,
      &image_dispatch_spec->dispatch_result,
      &image_dispatch_spec->aio_comp->image_dispatcher_ctx,
      &image_dispatch_spec->dispatcher_ctx);
  }
};

template <typename I>
struct ImageDispatcher<I>::PreprocessVisitor
  : public boost::static_visitor<bool> {
  ImageDispatcher<I>* image_dispatcher;
  ImageDispatchSpec* image_dispatch_spec;

  PreprocessVisitor(ImageDispatcher<I>* image_dispatcher,
                    ImageDispatchSpec* image_dispatch_spec)
    : image_dispatcher(image_dispatcher),
      image_dispatch_spec(image_dispatch_spec) {
  }

  bool clip_request() const {
    auto area = (image_dispatch_spec->image_dispatch_flags &
        IMAGE_DISPATCH_FLAG_CRYPTO_HEADER ? ImageArea::CRYPTO_HEADER :
                                            ImageArea::DATA);
    int r = util::clip_request(image_dispatcher->m_image_ctx,
                               &image_dispatch_spec->image_extents, area);
    if (r < 0) {
      image_dispatch_spec->fail(r);
      return true;
    }
    return false;
  }

  bool operator()(ImageDispatchSpec::Read& read) const {
    if ((read.read_flags & READ_FLAG_DISABLE_CLIPPING) != 0) {
      return false;
    }
    return clip_request();
  }

  bool operator()(ImageDispatchSpec::Flush&) const {
    return clip_request();
  }

  bool operator()(ImageDispatchSpec::ListSnaps&) const {
    return false;
  }

  template <typename T>
  bool operator()(T&) const {
    if (clip_request()) {
      return true;
    }

    std::shared_lock image_locker{image_dispatcher->m_image_ctx->image_lock};
    if (image_dispatcher->m_image_ctx->snap_id != CEPH_NOSNAP ||
        image_dispatcher->m_image_ctx->read_only) {
      image_dispatch_spec->fail(-EROFS);
      return true;
    }
    return false;
  }
};

template <typename I>
ImageDispatcher<I>::ImageDispatcher(I* image_ctx)
  : Dispatcher<I, ImageDispatcherInterface>(image_ctx) {
  // configure the core image dispatch handler on startup
  auto image_dispatch = new ImageDispatch(image_ctx);
  this->register_dispatch(image_dispatch);

  auto queue_image_dispatch = new QueueImageDispatch(image_ctx);
  this->register_dispatch(queue_image_dispatch);

  m_qos_image_dispatch = new QosImageDispatch<I>(image_ctx);
  this->register_dispatch(m_qos_image_dispatch);

  auto refresh_image_dispatch = new RefreshImageDispatch(image_ctx);
  this->register_dispatch(refresh_image_dispatch);

  m_write_block_dispatch = new WriteBlockImageDispatch<I>(image_ctx);
  this->register_dispatch(m_write_block_dispatch);
}

template <typename I>
void ImageDispatcher<I>::invalidate_cache(Context* on_finish) {
  auto image_ctx = this->m_image_ctx;
  auto cct = image_ctx->cct;
  ldout(cct, 5) << dendl;

  auto ctx = new C_InvalidateCache(
      this, IMAGE_DISPATCH_LAYER_NONE, on_finish);
  ctx->complete(0);
}

template <typename I>
void ImageDispatcher<I>::shut_down(Context* on_finish) {
  // TODO ensure all IOs are executed via a dispatcher
  // ensure read-ahead / copy-on-read ops are finished since they are
  // currently outside dispatcher tracking
  auto async_op = new AsyncOperation();

  on_finish = new LambdaContext([async_op, on_finish](int r) {
      async_op->finish_op();
      delete async_op;
      on_finish->complete(0);
    });
  on_finish = new LambdaContext([this, on_finish](int r) {
      Dispatcher<I, ImageDispatcherInterface>::shut_down(on_finish);
    });
  async_op->start_op(*this->m_image_ctx);
  async_op->flush(on_finish);
}

template <typename I>
void ImageDispatcher<I>::apply_qos_schedule_tick_min(uint64_t tick) {
  m_qos_image_dispatch->apply_qos_schedule_tick_min(tick);
}

template <typename I>
void ImageDispatcher<I>::apply_qos_limit(uint64_t flag, uint64_t limit,
                                         uint64_t burst, uint64_t burst_seconds) {
  m_qos_image_dispatch->apply_qos_limit(flag, limit, burst, burst_seconds);
}

template <typename I>
void ImageDispatcher<I>::apply_qos_exclude_ops(uint64_t exclude_ops) {
  m_qos_image_dispatch->apply_qos_exclude_ops(exclude_ops);
}

template <typename I>
bool ImageDispatcher<I>::writes_blocked() const {
  return m_write_block_dispatch->writes_blocked();
}

template <typename I>
int ImageDispatcher<I>::block_writes() {
  return m_write_block_dispatch->block_writes();
}

template <typename I>
void ImageDispatcher<I>::block_writes(Context *on_blocked) {
  m_write_block_dispatch->block_writes(on_blocked);
}

template <typename I>
void ImageDispatcher<I>::unblock_writes() {
  m_write_block_dispatch->unblock_writes();
}

template <typename I>
void ImageDispatcher<I>::wait_on_writes_unblocked(Context *on_unblocked) {
  m_write_block_dispatch->wait_on_writes_unblocked(on_unblocked);
}

template <typename I>
bool ImageDispatcher<I>::send_dispatch(
    ImageDispatchInterface* image_dispatch,
    ImageDispatchSpec* image_dispatch_spec) {
  if (image_dispatch_spec->tid == 0) {
    image_dispatch_spec->tid = ++m_next_tid;

    bool finished = preprocess(image_dispatch_spec);
    if (finished) {
      return true;
    }
  }

  return boost::apply_visitor(
    SendVisitor{image_dispatch, image_dispatch_spec},
    image_dispatch_spec->request);
}

template <typename I>
bool ImageDispatcher<I>::preprocess(
    ImageDispatchSpec* image_dispatch_spec) {
  return boost::apply_visitor(
    PreprocessVisitor{this, image_dispatch_spec},
    image_dispatch_spec->request);
}

} // namespace io
} // namespace librbd

template class librbd::io::ImageDispatcher<librbd::ImageCtx>;
