// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/exclusive_lock/PreReleaseRequest.h"
#include "common/AsyncOpTracker.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/Journal.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/io/ImageRequestWQ.h"
#include "librbd/io/ObjectDispatcher.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::exclusive_lock::PreReleaseRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace exclusive_lock {

using util::create_async_context_callback;
using util::create_context_callback;

template <typename I>
PreReleaseRequest<I>* PreReleaseRequest<I>::create(
    I &image_ctx, bool shutting_down, AsyncOpTracker &async_op_tracker,
    Context *on_finish) {
  return new PreReleaseRequest(image_ctx, shutting_down, async_op_tracker,
                               on_finish);
}

template <typename I>
PreReleaseRequest<I>::PreReleaseRequest(I &image_ctx, bool shutting_down,
                                        AsyncOpTracker &async_op_tracker,
                                        Context *on_finish)
  : m_image_ctx(image_ctx), m_shutting_down(shutting_down),
    m_async_op_tracker(async_op_tracker),
    m_on_finish(create_async_context_callback(image_ctx, on_finish)) {
}

template <typename I>
PreReleaseRequest<I>::~PreReleaseRequest() {
  if (!m_shutting_down) {
    m_image_ctx.state->handle_prepare_lock_complete();
  }
}

template <typename I>
void PreReleaseRequest<I>::send() {
  send_prepare_lock();
}

template <typename I>
void PreReleaseRequest<I>::send_prepare_lock() {
  if (m_shutting_down) {
    send_cancel_op_requests();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  // release the lock if the image is not busy performing other actions
  Context *ctx = create_context_callback<
    PreReleaseRequest<I>, &PreReleaseRequest<I>::handle_prepare_lock>(this);
  m_image_ctx.state->prepare_lock(ctx);
}

template <typename I>
void PreReleaseRequest<I>::handle_prepare_lock(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  send_cancel_op_requests();
}

template <typename I>
void PreReleaseRequest<I>::send_cancel_op_requests() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  using klass = PreReleaseRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_cancel_op_requests>(this);
  m_image_ctx.cancel_async_requests(ctx);
}

template <typename I>
void PreReleaseRequest<I>::handle_cancel_op_requests(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  assert(r == 0);

  send_block_writes();
}

template <typename I>
void PreReleaseRequest<I>::send_block_writes() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  using klass = PreReleaseRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_block_writes>(this);

  {
    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
    // setting the lock as required will automatically cause the IO
    // queue to re-request the lock if any IO is queued
    if (m_image_ctx.clone_copy_on_read ||
        m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
      m_image_ctx.io_work_queue->set_require_lock(io::DIRECTION_BOTH, true);
    } else {
      m_image_ctx.io_work_queue->set_require_lock(io::DIRECTION_WRITE, true);
    }
    m_image_ctx.io_work_queue->block_writes(ctx);
  }
}

template <typename I>
void PreReleaseRequest<I>::handle_block_writes(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r == -EBLACKLISTED) {
    // allow clean shut down if blacklisted
    lderr(cct) << "failed to block writes because client is blacklisted"
               << dendl;
  } else if (r < 0) {
    lderr(cct) << "failed to block writes: " << cpp_strerror(r) << dendl;
    m_image_ctx.io_work_queue->unblock_writes();
    save_result(r);
    finish();
    return;
  }

  send_wait_for_ops();
}

template <typename I>
void PreReleaseRequest<I>::send_wait_for_ops() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  Context *ctx = create_context_callback<
    PreReleaseRequest<I>, &PreReleaseRequest<I>::handle_wait_for_ops>(this);
  m_async_op_tracker.wait_for_ops(ctx);
}

template <typename I>
void PreReleaseRequest<I>::handle_wait_for_ops(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  send_invalidate_cache();
}

template <typename I>
void PreReleaseRequest<I>::send_invalidate_cache() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
  Context *ctx = create_context_callback<
      PreReleaseRequest<I>,
      &PreReleaseRequest<I>::handle_invalidate_cache>(this);
  m_image_ctx.io_object_dispatcher->invalidate_cache(ctx);
}

template <typename I>
void PreReleaseRequest<I>::handle_invalidate_cache(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0 && r != -EBLACKLISTED && r != -EBUSY) {
    lderr(cct) << "failed to invalidate cache: " << cpp_strerror(r)
               << dendl;
    m_image_ctx.io_work_queue->unblock_writes();
    save_result(r);
    finish();
    return;
  }

  send_flush_notifies();
}

template <typename I>
void PreReleaseRequest<I>::send_flush_notifies() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  using klass = PreReleaseRequest<I>;
  Context *ctx =
    create_context_callback<klass, &klass::handle_flush_notifies>(this);
  m_image_ctx.image_watcher->flush(ctx);
}

template <typename I>
void PreReleaseRequest<I>::handle_flush_notifies(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  assert(r == 0);
  send_close_journal();
}

template <typename I>
void PreReleaseRequest<I>::send_close_journal() {
  {
    RWLock::WLocker snap_locker(m_image_ctx.snap_lock);
    std::swap(m_journal, m_image_ctx.journal);
  }

  if (m_journal == nullptr) {
    send_close_object_map();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  using klass = PreReleaseRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_close_journal>(
    this);
  m_journal->close(ctx);
}

template <typename I>
void PreReleaseRequest<I>::handle_close_journal(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    // error implies some journal events were not flushed -- continue
    lderr(cct) << "failed to close journal: " << cpp_strerror(r) << dendl;
  }

  delete m_journal;

  send_close_object_map();
}

template <typename I>
void PreReleaseRequest<I>::send_close_object_map() {
  {
    RWLock::WLocker snap_locker(m_image_ctx.snap_lock);
    std::swap(m_object_map, m_image_ctx.object_map);
  }

  if (m_object_map == nullptr) {
    send_unlock();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  using klass = PreReleaseRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_close_object_map>(this);
  m_object_map->close(ctx);
}

template <typename I>
void PreReleaseRequest<I>::handle_close_object_map(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to close object map: " << cpp_strerror(r) << dendl;
  }

  delete m_object_map;
  send_unlock();
}

template <typename I>
void PreReleaseRequest<I>::send_unlock() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  finish();
}

template <typename I>
void PreReleaseRequest<I>::finish() {
  m_on_finish->complete(m_error_result);
  delete this;
}

} // namespace exclusive_lock
} // namespace librbd

template class librbd::exclusive_lock::PreReleaseRequest<librbd::ImageCtx>;
