// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/CloseRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageWatcher.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::CloseRequest: "

namespace librbd {
namespace image {

using util::create_async_context_callback;
using util::create_context_callback;

template <typename I>
CloseRequest<I>::CloseRequest(I *image_ctx, Context *on_finish)
  : m_image_ctx(image_ctx), m_on_finish(on_finish), m_error_result(0),
    m_exclusive_lock(nullptr) {
  assert(image_ctx != nullptr);
}

template <typename I>
void CloseRequest<I>::send() {
  send_unregister_image_watcher();
}

template <typename I>
void CloseRequest<I>::send_unregister_image_watcher() {
  if (m_image_ctx->image_watcher == nullptr) {
    send_shut_down_aio_queue();
    return;
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  // prevent incoming requests from our peers
  m_image_ctx->image_watcher->unregister_watch(create_context_callback<
    CloseRequest<I>, &CloseRequest<I>::handle_unregister_image_watcher>(this));
}

template <typename I>
void CloseRequest<I>::handle_unregister_image_watcher(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  save_result(r);
  if (r < 0) {
    lderr(cct) << "failed to unregister image watcher: " << cpp_strerror(r)
               << dendl;
  }

  send_shut_down_aio_queue();
}

template <typename I>
void CloseRequest<I>::send_shut_down_aio_queue() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  RWLock::RLocker owner_locker(m_image_ctx->owner_lock);
  m_image_ctx->aio_work_queue->shut_down(create_context_callback<
    CloseRequest<I>, &CloseRequest<I>::handle_shut_down_aio_queue>(this));
}

template <typename I>
void CloseRequest<I>::handle_shut_down_aio_queue(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  send_shut_down_exclusive_lock();
}

template <typename I>
void CloseRequest<I>::send_shut_down_exclusive_lock() {
  {
    RWLock::WLocker owner_locker(m_image_ctx->owner_lock);
    RWLock::WLocker snap_locker(m_image_ctx->snap_lock);
    std::swap(m_exclusive_lock, m_image_ctx->exclusive_lock);

    if (m_exclusive_lock == nullptr) {
      delete m_image_ctx->object_map;
      m_image_ctx->object_map = nullptr;
    }
  }

  if (m_exclusive_lock == nullptr) {
    send_flush();
    return;
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_exclusive_lock->shut_down(create_context_callback<
    CloseRequest<I>, &CloseRequest<I>::handle_shut_down_exclusive_lock>(this));
}

template <typename I>
void CloseRequest<I>::handle_shut_down_exclusive_lock(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  // object map and journal closed during exclusive lock shutdown
  assert(m_image_ctx->journal == nullptr);
  assert(m_image_ctx->object_map == nullptr);
  delete m_exclusive_lock;

  save_result(r);
  if (r < 0) {
    lderr(cct) << "failed to shut down exclusive lock: " << cpp_strerror(r)
               << dendl;
  }
  send_flush_readahead();
}

template <typename I>
void CloseRequest<I>::send_flush() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  RWLock::RLocker owner_locker(m_image_ctx->owner_lock);
  m_image_ctx->flush(create_async_context_callback(
    *m_image_ctx, create_context_callback<
      CloseRequest<I>, &CloseRequest<I>::handle_flush>(this)));
}

template <typename I>
void CloseRequest<I>::handle_flush(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to flush IO: " << cpp_strerror(r) << dendl;
  }
  send_flush_readahead();
}

template <typename I>
void CloseRequest<I>::send_flush_readahead() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_image_ctx->readahead.wait_for_pending(create_context_callback<
    CloseRequest<I>, &CloseRequest<I>::handle_flush_readahead>(this));
}

template <typename I>
void CloseRequest<I>::handle_flush_readahead(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  send_shut_down_cache();
}

template <typename I>
void CloseRequest<I>::send_shut_down_cache() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_image_ctx->shut_down_cache(create_context_callback<
    CloseRequest<I>, &CloseRequest<I>::handle_shut_down_cache>(this));
}

template <typename I>
void CloseRequest<I>::handle_shut_down_cache(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  save_result(r);
  if (r < 0) {
    lderr(cct) << "failed to shut down cache: " << cpp_strerror(r) << dendl;
  }
  send_flush_op_work_queue();
}

template <typename I>
void CloseRequest<I>::send_flush_op_work_queue() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_image_ctx->op_work_queue->queue(create_context_callback<
    CloseRequest<I>, &CloseRequest<I>::handle_flush_op_work_queue>(this), 0);
}

template <typename I>
void CloseRequest<I>::handle_flush_op_work_queue(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;
  send_close_parent();
}

template <typename I>
void CloseRequest<I>::send_close_parent() {
  if (m_image_ctx->parent == nullptr) {
    send_flush_image_watcher();
    return;
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_image_ctx->parent->state->close(create_async_context_callback(
    *m_image_ctx, create_context_callback<
      CloseRequest<I>, &CloseRequest<I>::handle_close_parent>(this)));
}

template <typename I>
void CloseRequest<I>::handle_close_parent(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  delete m_image_ctx->parent;
  save_result(r);
  if (r < 0) {
    lderr(cct) << "error closing parent image: " << cpp_strerror(r) << dendl;
  }
  send_flush_image_watcher();
}

template <typename I>
void CloseRequest<I>::send_flush_image_watcher() {
  if (m_image_ctx->image_watcher == nullptr) {
    finish();
    return;
  }

  m_image_ctx->image_watcher->flush(create_context_callback<
    CloseRequest<I>, &CloseRequest<I>::handle_flush_image_watcher>(this));
}

template <typename I>
void CloseRequest<I>::handle_flush_image_watcher(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "error flushing image watcher: " << cpp_strerror(r) << dendl;
  }
  save_result(r);
  finish();
}

template <typename I>
void CloseRequest<I>::finish() {
  m_image_ctx->shutdown();
  m_on_finish->complete(m_error_result);
  delete this;
}

} // namespace image
} // namespace librbd

template class librbd::image::CloseRequest<librbd::ImageCtx>;
