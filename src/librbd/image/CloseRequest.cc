// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/CloseRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatcher.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ObjectDispatcherInterface.h"

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
  ceph_assert(image_ctx != nullptr);
}

template <typename I>
void CloseRequest<I>::send() {
  send_block_image_watcher();
}

template <typename I>
void CloseRequest<I>::send_block_image_watcher() {
  if (m_image_ctx->image_watcher == nullptr) {
    send_shut_down_update_watchers();
    return;
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  // prevent incoming requests from our peers
  m_image_ctx->image_watcher->block_notifies(create_context_callback<
    CloseRequest<I>, &CloseRequest<I>::handle_block_image_watcher>(this));
}

template <typename I>
void CloseRequest<I>::handle_block_image_watcher(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  send_shut_down_update_watchers();
}

template <typename I>
void CloseRequest<I>::send_shut_down_update_watchers() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_image_ctx->state->shut_down_update_watchers(create_async_context_callback(
    *m_image_ctx, create_context_callback<
      CloseRequest<I>, &CloseRequest<I>::handle_shut_down_update_watchers>(this)));
}

template <typename I>
void CloseRequest<I>::handle_shut_down_update_watchers(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  save_result(r);
  if (r < 0) {
    lderr(cct) << "failed to shut down update watchers: " << cpp_strerror(r)
               << dendl;
  }

  send_shut_down_exclusive_lock();
}

template <typename I>
void CloseRequest<I>::send_shut_down_exclusive_lock() {
  {
    std::unique_lock owner_locker{m_image_ctx->owner_lock};
    m_exclusive_lock = m_image_ctx->exclusive_lock;

    // if reading a snapshot -- possible object map is open
    std::unique_lock image_locker{m_image_ctx->image_lock};
    if (m_exclusive_lock == nullptr && m_image_ctx->object_map) {
      m_image_ctx->object_map->put();
      m_image_ctx->object_map = nullptr;
    }
  }

  if (m_exclusive_lock == nullptr) {
    send_flush();
    return;
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  // in-flight IO will be flushed and in-flight requests will be canceled
  // before releasing lock
  m_exclusive_lock->shut_down(create_context_callback<
    CloseRequest<I>, &CloseRequest<I>::handle_shut_down_exclusive_lock>(this));
}

template <typename I>
void CloseRequest<I>::handle_shut_down_exclusive_lock(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  {
    std::shared_lock owner_locker{m_image_ctx->owner_lock};
    ceph_assert(m_image_ctx->exclusive_lock == nullptr);

    // object map and journal closed during exclusive lock shutdown
    std::shared_lock image_locker{m_image_ctx->image_lock};
    ceph_assert(m_image_ctx->journal == nullptr);
    ceph_assert(m_image_ctx->object_map == nullptr);
  }

  m_exclusive_lock->put();
  m_exclusive_lock = nullptr;

  save_result(r);
  if (r < 0) {
    lderr(cct) << "failed to shut down exclusive lock: " << cpp_strerror(r)
               << dendl;
  }

  send_unregister_image_watcher();
}

template <typename I>
void CloseRequest<I>::send_flush() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  std::shared_lock owner_locker{m_image_ctx->owner_lock};
  auto ctx = create_context_callback<
    CloseRequest<I>, &CloseRequest<I>::handle_flush>(this);
  auto aio_comp = io::AioCompletion::create_and_start(ctx, m_image_ctx,
                                                      io::AIO_TYPE_FLUSH);
  auto req = io::ImageDispatchSpec<I>::create_flush(
    *m_image_ctx, io::IMAGE_DISPATCH_LAYER_INTERNAL_START, aio_comp,
    io::FLUSH_SOURCE_INTERNAL, {});
  req->send();
}

template <typename I>
void CloseRequest<I>::handle_flush(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to flush IO: " << cpp_strerror(r) << dendl;
  }
  send_unregister_image_watcher();
}

template <typename I>
void CloseRequest<I>::send_unregister_image_watcher() {
  if (m_image_ctx->image_watcher == nullptr) {
    send_flush_readahead();
    return;
  }

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

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

  send_flush_readahead();
}

template <typename I>
void CloseRequest<I>::send_flush_readahead() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_image_ctx->readahead.wait_for_pending(create_async_context_callback(
    *m_image_ctx, create_context_callback<
      CloseRequest<I>, &CloseRequest<I>::handle_flush_readahead>(this)));
}

template <typename I>
void CloseRequest<I>::handle_flush_readahead(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  send_shut_down_image_dispatcher();
}

template <typename I>
void CloseRequest<I>::send_shut_down_image_dispatcher() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_image_ctx->io_image_dispatcher->shut_down(create_context_callback<
    CloseRequest<I>,
    &CloseRequest<I>::handle_shut_down_image_dispatcher>(this));
}

template <typename I>
void CloseRequest<I>::handle_shut_down_image_dispatcher(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  save_result(r);
  if (r < 0) {
    lderr(cct) << "failed to shut down image dispatcher: "
               << cpp_strerror(r) << dendl;
  }

  send_shut_down_object_dispatcher();
}

template <typename I>
void CloseRequest<I>::send_shut_down_object_dispatcher() {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_image_ctx->io_object_dispatcher->shut_down(create_context_callback<
    CloseRequest<I>,
    &CloseRequest<I>::handle_shut_down_object_dispatcher>(this));
}

template <typename I>
void CloseRequest<I>::handle_shut_down_object_dispatcher(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  save_result(r);
  if (r < 0) {
    lderr(cct) << "failed to shut down object dispatcher: "
               << cpp_strerror(r) << dendl;
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

  m_image_ctx->parent = nullptr;
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
