// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/exclusive_lock/PreReleaseRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ManagedLock.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/Journal.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::exclusive_lock::PreReleaseRequest: "

namespace librbd {
namespace exclusive_lock {

using util::create_async_context_callback;
using util::create_context_callback;

template <typename I>
PreReleaseRequest<I>* PreReleaseRequest<I>::create(I &image_ctx,
                                                   Context *on_releasing,
                                                   Context *on_finish,
                                                   bool shutting_down) {
  return new PreReleaseRequest(image_ctx, on_releasing, on_finish,
                               shutting_down);
}

template <typename I>
PreReleaseRequest<I>::PreReleaseRequest(I &image_ctx, Context *on_releasing,
                                        Context *on_finish, bool shutting_down)
  : m_image_ctx(image_ctx), m_on_releasing(on_releasing),
    m_on_finish(create_async_context_callback(image_ctx, on_finish)),
    m_shutting_down(shutting_down), m_error_result(0), m_object_map(nullptr),
    m_journal(nullptr) {
}

template <typename I>
PreReleaseRequest<I>::~PreReleaseRequest() {
  if (!m_shutting_down) {
    m_image_ctx.state->handle_prepare_lock_complete();
  }
  delete m_on_releasing;
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
  ldout(cct, 10) << __func__ << dendl;

  // release the lock if the image is not busy performing other actions
  Context *ctx = create_context_callback<
    PreReleaseRequest<I>, &PreReleaseRequest<I>::handle_prepare_lock>(this);
  m_image_ctx.state->prepare_lock(ctx);
}

template <typename I>
void PreReleaseRequest<I>::handle_prepare_lock(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << r << dendl;

  send_cancel_op_requests();
}

template <typename I>
void PreReleaseRequest<I>::send_cancel_op_requests() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  using klass = PreReleaseRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_cancel_op_requests>(this);
  m_image_ctx.cancel_async_requests(ctx);
}

template <typename I>
void PreReleaseRequest<I>::handle_cancel_op_requests(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << r << dendl;

  assert(r == 0);

  send_block_writes();
}

template <typename I>
void PreReleaseRequest<I>::send_block_writes() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  using klass = PreReleaseRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_block_writes>(this);

  {
    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
    if (m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
      m_image_ctx.aio_work_queue->set_require_lock_on_read();
    }
    m_image_ctx.aio_work_queue->block_writes(ctx);
  }
}

template <typename I>
void PreReleaseRequest<I>::handle_block_writes(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << r << dendl;

  save_result(r);
  if (r < 0) {
    m_image_ctx.aio_work_queue->unblock_writes();
    finish();
    return;
  }

  send_image_flush_notifies();
}

template <typename I>
void PreReleaseRequest<I>::send_image_flush_notifies() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  using klass = PreReleaseRequest<I>;
  Context *ctx =
    create_context_callback<klass, &klass::handle_image_flush_notifies>(this);
  m_image_ctx.image_watcher->flush(ctx);
}

template <typename I>
void PreReleaseRequest<I>::handle_image_flush_notifies(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

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
  ldout(cct, 10) << __func__ << dendl;

  using klass = PreReleaseRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_close_journal>(
    this);
  m_journal->close(ctx);
}

template <typename I>
void PreReleaseRequest<I>::handle_close_journal(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << r << dendl;

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
  ldout(cct, 10) << __func__ << dendl;

  using klass = PreReleaseRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_close_object_map>(this);
  m_object_map->close(ctx);
}

template <typename I>
void PreReleaseRequest<I>::handle_close_object_map(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << r << dendl;

  // object map shouldn't return errors
  assert(r == 0);
  delete m_object_map;

  send_unlock();
}

template <typename I>
void PreReleaseRequest<I>::send_unlock() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  if (m_on_releasing != nullptr) {
    // alert caller that we no longer own the exclusive lock
    m_on_releasing->complete(0);
    m_on_releasing = nullptr;
  }

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
