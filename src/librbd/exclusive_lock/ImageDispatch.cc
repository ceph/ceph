// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/exclusive_lock/ImageDispatch.h"
#include "include/Context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/exclusive_lock/Policy.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/FlushTracker.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ImageDispatcherInterface.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::exclusive_lock::ImageDispatch: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace exclusive_lock {

using util::create_context_callback;

template <typename I>
ImageDispatch<I>::ImageDispatch(I* image_ctx)
  : m_image_ctx(image_ctx),
    m_lock(ceph::make_shared_mutex(
      util::unique_lock_name("librbd::exclusve_lock::ImageDispatch::m_lock",
                             this))),
    m_flush_tracker(new io::FlushTracker<I>(image_ctx)) {
}

template <typename I>
ImageDispatch<I>::~ImageDispatch() {
  delete m_flush_tracker;
}

template <typename I>
void ImageDispatch<I>::shut_down(Context* on_finish) {
  // release any IO waiting on exclusive lock
  Contexts on_dispatches;
  {
    std::unique_lock locker{m_lock};
    std::swap(on_dispatches, m_on_dispatches);
  }

  for (auto ctx : on_dispatches) {
    ctx->complete(0);
  }

  // ensure we don't have any pending flushes before deleting layer
  m_flush_tracker->shut_down();
  on_finish->complete(0);
}

template <typename I>
void ImageDispatch<I>::set_require_lock(io::Direction direction,
                                        Context* on_finish) {
  // pause any matching IO from proceeding past this layer
  set_require_lock(direction, true);

  if (direction == io::DIRECTION_READ) {
    on_finish->complete(0);
    return;
  }

  // push through a flush for any in-flight writes at lower levels
  auto aio_comp = io::AioCompletion::create_and_start(
    on_finish, util::get_image_ctx(m_image_ctx), io::AIO_TYPE_FLUSH);
  auto req = io::ImageDispatchSpec<I>::create_flush(
    *m_image_ctx, io::IMAGE_DISPATCH_LAYER_EXCLUSIVE_LOCK, aio_comp,
    io::FLUSH_SOURCE_INTERNAL, {});
  req->send();
}

template <typename I>
void ImageDispatch<I>::unset_require_lock(io::Direction direction) {
  set_require_lock(direction, false);
}

template <typename I>
bool ImageDispatch<I>::set_require_lock(io::Direction direction, bool enabled) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "direction=" << direction << ", enabled=" << enabled
                 << dendl;

  std::unique_lock locker{m_lock};
  auto prev_require_lock = (m_require_lock_on_read || m_require_lock_on_write);

  switch (direction) {
  case io::DIRECTION_READ:
    m_require_lock_on_read = enabled;
    break;
  case io::DIRECTION_WRITE:
    m_require_lock_on_write = enabled;
    break;
  case io::DIRECTION_BOTH:
    m_require_lock_on_read = enabled;
    m_require_lock_on_write = enabled;
    break;
  }

  bool require_lock = (m_require_lock_on_read || m_require_lock_on_write);
  return ((enabled && !prev_require_lock && require_lock) ||
          (!enabled && prev_require_lock && !require_lock));
}

template <typename I>
bool ImageDispatch<I>::read(
    io::AioCompletion* aio_comp, io::Extents &&image_extents,
    io::ReadResult &&read_result, int op_flags,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "image_extents=" << image_extents << dendl;

  if (needs_exclusive_lock(true, tid, dispatch_result, on_dispatched)) {
    return true;
  }

  m_flush_tracker->start_io(tid);
  return false;
}

template <typename I>
bool ImageDispatch<I>::write(
    io::AioCompletion* aio_comp, io::Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << ", image_extents=" << image_extents
                 << dendl;

  if (needs_exclusive_lock(false, tid, dispatch_result, on_dispatched)) {
    return true;
  }

  m_flush_tracker->start_io(tid);
  return false;
}

template <typename I>
bool ImageDispatch<I>::discard(
    io::AioCompletion* aio_comp, io::Extents &&image_extents,
    uint32_t discard_granularity_bytes, const ZTracer::Trace &parent_trace,
    uint64_t tid, std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << ", image_extents=" << image_extents
                 << dendl;

  if (needs_exclusive_lock(false, tid, dispatch_result, on_dispatched)) {
    return true;
  }

  m_flush_tracker->start_io(tid);
  return false;
}

template <typename I>
bool ImageDispatch<I>::write_same(
    io::AioCompletion* aio_comp, io::Extents &&image_extents, bufferlist &&bl,
    int op_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << ", image_extents=" << image_extents
                 << dendl;

  if (needs_exclusive_lock(false, tid, dispatch_result, on_dispatched)) {
    return true;
  }

  m_flush_tracker->start_io(tid);
  return false;
}

template <typename I>
bool ImageDispatch<I>::compare_and_write(
    io::AioCompletion* aio_comp, io::Extents &&image_extents,
    bufferlist &&cmp_bl, bufferlist &&bl, uint64_t *mismatch_offset,
    int op_flags, const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << ", image_extents=" << image_extents
                 << dendl;

  if (needs_exclusive_lock(false, tid, dispatch_result, on_dispatched)) {
    return true;
  }

  m_flush_tracker->start_io(tid);
  return false;
}

template <typename I>
bool ImageDispatch<I>::flush(
    io::AioCompletion* aio_comp, io::FlushSource flush_source,
    const ZTracer::Trace &parent_trace, uint64_t tid,
    std::atomic<uint32_t>* image_dispatch_flags,
    io::DispatchResult* dispatch_result, Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  // don't attempt to grab the exclusive lock if were are just internally
  // clearing out our in-flight IO queue
  if (flush_source != io::FLUSH_SOURCE_USER) {
    return false;
  }

  if (needs_exclusive_lock(false, tid, dispatch_result, on_dispatched)) {
    return true;
  }

  *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
  m_flush_tracker->flush(on_dispatched);
  return true;
}

template <typename I>
void ImageDispatch<I>::handle_finished(int r, uint64_t tid) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 20) << "tid=" << tid << dendl;

  m_flush_tracker->finish_io(tid);
}

template <typename I>
bool ImageDispatch<I>::is_lock_required(bool read_op) const {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  return ((read_op && m_require_lock_on_read) ||
          (!read_op && m_require_lock_on_write));
}

template <typename I>
bool ImageDispatch<I>::needs_exclusive_lock(bool read_op, uint64_t tid,
                                            io::DispatchResult* dispatch_result,
                                            Context* on_dispatched) {
  auto cct = m_image_ctx->cct;
  bool lock_required = false;
  {
    std::shared_lock locker{m_lock};
    lock_required = is_lock_required(read_op);
  }

  if (lock_required) {
    std::shared_lock owner_locker{m_image_ctx->owner_lock};
    if (m_image_ctx->exclusive_lock == nullptr) {
      // raced with the exclusive lock being disabled
      return false;
    }

    ldout(cct, 5) << "exclusive lock required: delaying IO" << dendl;
    if (!m_image_ctx->get_exclusive_lock_policy()->may_auto_request_lock()) {
      lderr(cct) << "op requires exclusive lock" << dendl;

      *dispatch_result = io::DISPATCH_RESULT_CONTINUE;
      on_dispatched->complete(
        m_image_ctx->exclusive_lock->get_unlocked_op_error());
      return true;
    }

    // block potential races with other incoming IOs
    std::unique_lock locker{m_lock};
    bool retesting_lock = (
      !m_on_dispatches.empty() && m_on_dispatches.front() == on_dispatched);
    if (!m_on_dispatches.empty() && !retesting_lock) {
      *dispatch_result = io::DISPATCH_RESULT_RESTART;
      m_on_dispatches.push_back(on_dispatched);
      return true;
    }

    if (!is_lock_required(read_op)) {
      return false;
    }

    ceph_assert(m_on_dispatches.empty() || retesting_lock);
    m_on_dispatches.push_back(on_dispatched);
    locker.unlock();

    *dispatch_result = io::DISPATCH_RESULT_RESTART;
    auto ctx = create_context_callback<
      ImageDispatch<I>, &ImageDispatch<I>::handle_acquire_lock>(this);
    m_image_ctx->exclusive_lock->acquire_lock(ctx);
    return true;
  }

  return false;
}

template <typename I>
void ImageDispatch<I>::handle_acquire_lock(int r) {
  auto cct = m_image_ctx->cct;
  ldout(cct, 5) << "r=" << r << dendl;

  std::unique_lock locker{m_lock};
  ceph_assert(!m_on_dispatches.empty());

  Context* failed_dispatch = nullptr;
  Contexts on_dispatches;
  if (r < 0) {
    lderr(cct) << "failed to acquire exclusive lock: " << cpp_strerror(r)
               << dendl;
    failed_dispatch = m_on_dispatches.front();
    m_on_dispatches.pop_front();
  } else {
    // re-test is lock is still required (i.e. it wasn't acquired) via a restart
    // dispatch
    std::swap(on_dispatches, m_on_dispatches);
  }
  locker.unlock();

  if (failed_dispatch != nullptr) {
    failed_dispatch->complete(r);
  }
  for (auto ctx : on_dispatches) {
    ctx->complete(0);
  }
}

} // namespace exclusive_lock
} // namespace librbd

template class librbd::exclusive_lock::ImageDispatch<librbd::ImageCtx>;
