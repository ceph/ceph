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
#include "librbd/exclusive_lock/ImageDispatch.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "librbd/io/ImageDispatcherInterface.h"
#include "librbd/io/ObjectDispatcherInterface.h"
#include "librbd/io/Types.h"
#include "librbd/PluginRegistry.h"

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
    I &image_ctx, ImageDispatch<I>* image_dispatch, bool shutting_down,
    AsyncOpTracker &async_op_tracker, Context *on_finish) {
  return new PreReleaseRequest(image_ctx, image_dispatch, shutting_down,
                               async_op_tracker, on_finish);
}

template <typename I>
PreReleaseRequest<I>::PreReleaseRequest(I &image_ctx,
                                        ImageDispatch<I>* image_dispatch,
                                        bool shutting_down,
                                        AsyncOpTracker &async_op_tracker,
                                        Context *on_finish)
  : m_image_ctx(image_ctx), m_image_dispatch(image_dispatch),
    m_shutting_down(shutting_down), m_async_op_tracker(async_op_tracker),
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

  ceph_assert(r == 0);

  send_set_require_lock();
}

template <typename I>
void PreReleaseRequest<I>::send_set_require_lock() {
  if (!m_image_ctx.test_features(RBD_FEATURE_EXCLUSIVE_LOCK)) {
    // exclusive-lock was disabled, no need to block IOs
    send_wait_for_ops();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  using klass = PreReleaseRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_set_require_lock>(this);

  // setting the lock as required will automatically cause the IO
  // queue to re-request the lock if any IO is queued
  if (m_image_ctx.clone_copy_on_read ||
      m_image_ctx.test_features(RBD_FEATURE_JOURNALING) ||
      m_image_ctx.test_features(RBD_FEATURE_DIRTY_CACHE)) {
    m_image_dispatch->set_require_lock(m_shutting_down,
                                       io::DIRECTION_BOTH, ctx);
  } else {
    m_image_dispatch->set_require_lock(m_shutting_down,
                                       io::DIRECTION_WRITE, ctx);
  }
}

template <typename I>
void PreReleaseRequest<I>::handle_set_require_lock(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    // IOs are still flushed regardless of the error
    lderr(cct) << "failed to set lock: " << cpp_strerror(r) << dendl;
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

  send_prepare_lock();
}

template <typename I>
void PreReleaseRequest<I>::send_prepare_lock() {
  if (m_shutting_down) {
    send_process_plugin_release_lock();
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

  send_process_plugin_release_lock();
}

template <typename I>
void PreReleaseRequest<I>::send_process_plugin_release_lock() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  std::shared_lock owner_lock{m_image_ctx.owner_lock};
  Context *ctx = create_async_context_callback(m_image_ctx, create_context_callback<
      PreReleaseRequest<I>,
      &PreReleaseRequest<I>::handle_process_plugin_release_lock>(this));
  m_image_ctx.plugin_registry->prerelease_exclusive_lock(ctx);
}

template <typename I>
void PreReleaseRequest<I>::handle_process_plugin_release_lock(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to handle plugins before releasing lock: "
               << cpp_strerror(r) << dendl;
    m_image_dispatch->unset_require_lock(io::DIRECTION_BOTH);
    save_result(r);
    finish();
    return;
  }

  send_invalidate_cache();
}

template <typename I>
void PreReleaseRequest<I>::send_invalidate_cache() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  Context *ctx = create_context_callback<
      PreReleaseRequest<I>,
      &PreReleaseRequest<I>::handle_invalidate_cache>(this);
  m_image_ctx.io_image_dispatcher->invalidate_cache(ctx);
}

template <typename I>
void PreReleaseRequest<I>::handle_invalidate_cache(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0 && r != -EBLOCKLISTED && r != -EBUSY) {
    lderr(cct) << "failed to invalidate cache: " << cpp_strerror(r)
               << dendl;
    m_image_dispatch->unset_require_lock(io::DIRECTION_BOTH);
    save_result(r);
    finish();
    return;
  }

  send_flush_io();
}

template <typename I>
void PreReleaseRequest<I>::send_flush_io() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  // ensure that all in-flight IO is flushed -- skipping the refresh layer
  // since it should have been flushed when the lock was required and now
  // refreshes are disabled / interlocked w/ this state machine.
  auto ctx = create_context_callback<
      PreReleaseRequest<I>, &PreReleaseRequest<I>::handle_flush_io>(this);
  auto aio_comp = io::AioCompletion::create_and_start(
    ctx, util::get_image_ctx(&m_image_ctx), librbd::io::AIO_TYPE_FLUSH);
  auto req = io::ImageDispatchSpec::create_flush(
    m_image_ctx, io::IMAGE_DISPATCH_LAYER_EXCLUSIVE_LOCK, aio_comp,
    io::FLUSH_SOURCE_EXCLUSIVE_LOCK_SKIP_REFRESH, {});
  req->send();
}

template <typename I>
void PreReleaseRequest<I>::handle_flush_io(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to flush IO: " << cpp_strerror(r) << dendl;
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

  ceph_assert(r == 0);
  send_close_journal();
}

template <typename I>
void PreReleaseRequest<I>::send_close_journal() {
  {
    std::unique_lock image_locker{m_image_ctx.image_lock};
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

  m_journal->put();
  m_journal = nullptr;

  send_close_object_map();
}

template <typename I>
void PreReleaseRequest<I>::send_close_object_map() {
  {
    std::unique_lock image_locker{m_image_ctx.image_lock};
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
    klass, &klass::handle_close_object_map>(this, m_object_map);
  m_object_map->close(ctx);
}

template <typename I>
void PreReleaseRequest<I>::handle_close_object_map(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to close object map: " << cpp_strerror(r) << dendl;
  }
  m_object_map->put();

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
