// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/exclusive_lock/ReleaseRequest.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/Journal.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::exclusive_lock::ReleaseRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace exclusive_lock {

using util::create_async_context_callback;
using util::create_context_callback;
using util::create_rados_safe_callback;

template <typename I>
ReleaseRequest<I>* ReleaseRequest<I>::create(I &image_ctx,
                                             const std::string &cookie,
                                             Context *on_releasing,
                                             Context *on_finish,
                                             bool shutting_down) {
  return new ReleaseRequest(image_ctx, cookie, on_releasing, on_finish,
                            shutting_down);
}

template <typename I>
ReleaseRequest<I>::ReleaseRequest(I &image_ctx, const std::string &cookie,
                                  Context *on_releasing, Context *on_finish,
                                  bool shutting_down)
  : m_image_ctx(image_ctx), m_cookie(cookie), m_on_releasing(on_releasing),
    m_on_finish(create_async_context_callback(image_ctx, on_finish)),
    m_shutting_down(shutting_down), m_object_map(nullptr), m_journal(nullptr) {
}

template <typename I>
ReleaseRequest<I>::~ReleaseRequest() {
  if (!m_shutting_down) {
    m_image_ctx.state->handle_prepare_lock_complete();
  }
  delete m_on_releasing;
}

template <typename I>
void ReleaseRequest<I>::send() {
  send_prepare_lock();
}

template <typename I>
void ReleaseRequest<I>::send_prepare_lock() {
  if (m_shutting_down) {
    send_cancel_op_requests();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  // release the lock if the image is not busy performing other actions
  Context *ctx = create_context_callback<
    ReleaseRequest<I>, &ReleaseRequest<I>::handle_prepare_lock>(this);
  m_image_ctx.state->prepare_lock(ctx);
}

template <typename I>
Context *ReleaseRequest<I>::handle_prepare_lock(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << *ret_val << dendl;

  send_cancel_op_requests();
  return nullptr;
}

template <typename I>
void ReleaseRequest<I>::send_cancel_op_requests() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  using klass = ReleaseRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_cancel_op_requests>(this);
  m_image_ctx.cancel_async_requests(ctx);
}

template <typename I>
Context *ReleaseRequest<I>::handle_cancel_op_requests(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << *ret_val << dendl;

  assert(*ret_val == 0);

  send_block_writes();
  return nullptr;
}

template <typename I>
void ReleaseRequest<I>::send_block_writes() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  using klass = ReleaseRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_block_writes>(this);

  {
    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
    // setting the lock as required will automatically cause the IO
    // queue to re-request the lock if any IO is queued
    if (m_image_ctx.clone_copy_on_read ||
        m_image_ctx.test_features(RBD_FEATURE_JOURNALING)) {
      m_image_ctx.aio_work_queue->set_require_lock(AIO_DIRECTION_BOTH, true);
    } else {
      m_image_ctx.aio_work_queue->set_require_lock(AIO_DIRECTION_WRITE, true);
    }
    m_image_ctx.aio_work_queue->block_writes(ctx);
  }
}

template <typename I>
Context *ReleaseRequest<I>::handle_block_writes(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << *ret_val << dendl;

  if (*ret_val == -EBLACKLISTED) {
    // allow clean shut down if blacklisted
    lderr(cct) << "failed to block writes because client is blacklisted"
               << dendl;
  } else if (*ret_val < 0) {
    lderr(cct) << "failed to block writes: " << cpp_strerror(*ret_val) << dendl;
    m_image_ctx.aio_work_queue->unblock_writes();
    return m_on_finish;
  }

  send_invalidate_cache(false);
  return nullptr;
}

template <typename I>
void ReleaseRequest<I>::send_invalidate_cache(bool purge_on_error) {
  if (m_image_ctx.object_cacher == nullptr) {
    send_flush_notifies();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "purge_on_error=" << purge_on_error << dendl;

  RWLock::RLocker owner_lock(m_image_ctx.owner_lock);
  Context *ctx = create_async_context_callback(
    m_image_ctx, create_context_callback<
      ReleaseRequest<I>,
      &ReleaseRequest<I>::handle_invalidate_cache>(this));
  m_image_ctx.invalidate_cache(purge_on_error, ctx);
}

template <typename I>
Context *ReleaseRequest<I>::handle_invalidate_cache(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << *ret_val << dendl;

  if (*ret_val == -EBLACKLISTED) {
    lderr(cct) << "failed to invalidate cache because client is blacklisted"
               << dendl;
    if (!m_image_ctx.is_cache_empty()) {
      // force purge the cache after after being blacklisted
      send_invalidate_cache(true);
      return nullptr;
    }
  } else if (*ret_val < 0 && *ret_val != -EBUSY) {
    lderr(cct) << "failed to invalidate cache: " << cpp_strerror(*ret_val)
               << dendl;
    m_image_ctx.aio_work_queue->unblock_writes();
    return m_on_finish;
  }

  send_flush_notifies();
  return nullptr;
}

template <typename I>
void ReleaseRequest<I>::send_flush_notifies() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  using klass = ReleaseRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_flush_notifies>(this);
  m_image_ctx.image_watcher->flush(ctx);
}

template <typename I>
Context *ReleaseRequest<I>::handle_flush_notifies(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  assert(*ret_val == 0);
  send_close_journal();
  return nullptr;
}

template <typename I>
void ReleaseRequest<I>::send_close_journal() {
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

  using klass = ReleaseRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_close_journal>(
    this);
  m_journal->close(ctx);
}

template <typename I>
Context *ReleaseRequest<I>::handle_close_journal(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << *ret_val << dendl;

  if (*ret_val < 0) {
    // error implies some journal events were not flushed -- continue
    lderr(cct) << "failed to close journal: " << cpp_strerror(*ret_val)
               << dendl;
  }

  delete m_journal;

  send_close_object_map();
  return nullptr;
}

template <typename I>
void ReleaseRequest<I>::send_close_object_map() {
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

  using klass = ReleaseRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_close_object_map>(this);
  m_object_map->close(ctx);
}

template <typename I>
Context *ReleaseRequest<I>::handle_close_object_map(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << *ret_val << dendl;

  // object map shouldn't return errors
  assert(*ret_val == 0);
  delete m_object_map;

  send_unlock();
  return nullptr;
}

template <typename I>
void ReleaseRequest<I>::send_unlock() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "cookie=" << m_cookie << dendl;

  if (m_on_releasing != nullptr) {
    // alert caller that we no longer own the exclusive lock
    m_on_releasing->complete(0);
    m_on_releasing = nullptr;
  }

  librados::ObjectWriteOperation op;
  rados::cls::lock::unlock(&op, RBD_LOCK_NAME, m_cookie);

  using klass = ReleaseRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_safe_callback<klass, &klass::handle_unlock>(this);
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid,
                                         rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
Context *ReleaseRequest<I>::handle_unlock(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << *ret_val << dendl;

  if (*ret_val < 0 && *ret_val != -ENOENT) {
    lderr(cct) << "failed to unlock: " << cpp_strerror(*ret_val) << dendl;
  }

  // treat errors as the image is unlocked
  *ret_val = 0;
  return m_on_finish;
}

} // namespace exclusive_lock
} // namespace librbd

template class librbd::exclusive_lock::ReleaseRequest<librbd::ImageCtx>;
