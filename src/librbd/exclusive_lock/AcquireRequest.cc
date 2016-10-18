// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/exclusive_lock/AcquireRequest.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "include/stringify.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/Lock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/Journal.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "librbd/image/RefreshRequest.h"
#include "librbd/journal/Policy.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::exclusive_lock::AcquireRequest: "

namespace librbd {
namespace exclusive_lock {

using util::create_async_context_callback;
using util::create_context_callback;
using util::create_rados_ack_callback;
using util::create_rados_safe_callback;


template <typename I>
AcquireRequest<I>* AcquireRequest<I>::create(I &image_ctx, LockT *managed_lock,
                                             Context *on_finish,
                                             bool try_lock) {
  return new AcquireRequest(image_ctx, managed_lock, on_finish, try_lock);
}

template <typename I>
AcquireRequest<I>::AcquireRequest(I &image_ctx, LockT *managed_lock,
                                  Context *on_finish, bool try_lock)
  : m_image_ctx(image_ctx), m_managed_lock(managed_lock),
    m_on_finish(create_async_context_callback(image_ctx, on_finish)),
    m_try_lock(try_lock), m_object_map(nullptr), m_journal(nullptr),
    m_error_result(0) {
}

template <typename I>
AcquireRequest<I>::~AcquireRequest() {
  if (!m_prepare_lock_completed) {
    m_image_ctx.state->handle_prepare_lock_complete();
  }
}

template <typename I>
void AcquireRequest<I>::send() {
  send_prepare_lock();
}

template <typename I>
void AcquireRequest<I>::send_prepare_lock() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  // acquire the lock if the image is not busy performing other actions
  Context *ctx = create_context_callback<
    AcquireRequest<I>, &AcquireRequest<I>::handle_prepare_lock>(this);
  m_image_ctx.state->prepare_lock(ctx);
}

template <typename I>
Context *AcquireRequest<I>::handle_prepare_lock(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << *ret_val << dendl;

  send_flush_notifies();
  return nullptr;
}

template <typename I>
void AcquireRequest<I>::send_flush_notifies() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  using klass = AcquireRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_flush_notifies>(
    this);
  m_image_ctx.image_watcher->flush(ctx);
}

template <typename I>
Context *AcquireRequest<I>::handle_flush_notifies(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  assert(*ret_val == 0);
  send_lock();
  return nullptr;
}

template <typename I>
void AcquireRequest<I>::send_lock() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  Context *ctx = create_context_callback<
    AcquireRequest<I>, &AcquireRequest<I>::handle_lock>(this);
  if (m_try_lock) {
    m_managed_lock->try_lock(ctx);
  } else {
    m_managed_lock->request_lock(ctx);
  }
}

template <typename I>
Context *AcquireRequest<I>::handle_lock(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << *ret_val << dendl;

  if (*ret_val >= 0 && (!m_try_lock || m_managed_lock->is_lock_owner())) {
    return send_refresh();
  } else {
    lderr(cct) << "failed to lock: " << cpp_strerror(*ret_val) << dendl;
    return m_on_finish;
  }
}

template <typename I>
Context *AcquireRequest<I>::send_refresh() {
  if (!m_image_ctx.state->is_refresh_required()) {
    return send_open_object_map();
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  using klass = AcquireRequest<I>;
  Context *ctx = create_async_context_callback(
    m_image_ctx, create_context_callback<klass, &klass::handle_refresh>(this));

  // ImageState is blocked waiting for lock to complete -- safe to directly
  // refresh
  image::RefreshRequest<I> *req = image::RefreshRequest<I>::create(
    m_image_ctx, true, ctx);
  req->send();
  return nullptr;
}

template <typename I>
Context *AcquireRequest<I>::handle_refresh(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << *ret_val << dendl;

  if (*ret_val == -ERESTART) {
    // next issued IO or op will (re)-refresh the image and shut down lock
    ldout(cct, 5) << ": exclusive lock dynamically disabled" << dendl;
    *ret_val = 0;
  } else if (*ret_val < 0) {
    lderr(cct) << "failed to refresh image: " << cpp_strerror(*ret_val)
               << dendl;
    m_error_result = *ret_val;
    send_unlock();
    return nullptr;
  }

  return send_open_object_map();
}

template <typename I>
Context *AcquireRequest<I>::send_open_journal() {
  bool journal_enabled;
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    journal_enabled = (m_image_ctx.test_features(RBD_FEATURE_JOURNALING,
                                                 m_image_ctx.snap_lock) &&
                       !m_image_ctx.get_journal_policy()->journal_disabled());
  }
  if (!journal_enabled) {
    apply();
    return m_on_finish;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  using klass = AcquireRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_open_journal>(
    this);
  m_journal = m_image_ctx.create_journal();

  // journal playback requires object map (if enabled) and itself
  apply();

  m_journal->open(ctx);
  return nullptr;
}

template <typename I>
Context *AcquireRequest<I>::handle_open_journal(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << *ret_val << dendl;

  if (*ret_val < 0) {
    lderr(cct) << "failed to open journal: " << cpp_strerror(*ret_val) << dendl;
    m_error_result = *ret_val;
    send_close_journal();
    return nullptr;
  }

  send_allocate_journal_tag();
  return nullptr;
}

template <typename I>
void AcquireRequest<I>::send_allocate_journal_tag() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
  using klass = AcquireRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_allocate_journal_tag>(this);
  m_image_ctx.get_journal_policy()->allocate_tag_on_lock(ctx);
}

template <typename I>
Context *AcquireRequest<I>::handle_allocate_journal_tag(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << *ret_val << dendl;

  if (*ret_val < 0) {
    lderr(cct) << "failed to allocate journal tag: " << cpp_strerror(*ret_val)
               << dendl;
    m_error_result = *ret_val;
    send_close_journal();
    return nullptr;
  }
  return m_on_finish;
}

template <typename I>
void AcquireRequest<I>::send_close_journal() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  using klass = AcquireRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_close_journal>(
    this);
  m_journal->close(ctx);
}

template <typename I>
Context *AcquireRequest<I>::handle_close_journal(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << *ret_val << dendl;

  if (*ret_val < 0) {
    lderr(cct) << "failed to close journal: " << cpp_strerror(*ret_val)
               << dendl;
  }

  send_close_object_map();
  return nullptr;
}

template <typename I>
Context *AcquireRequest<I>::send_open_object_map() {
  if (!m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP)) {
    return send_open_journal();
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  using klass = AcquireRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_open_object_map>(
    this);

  m_object_map = m_image_ctx.create_object_map(CEPH_NOSNAP);
  m_object_map->open(ctx);
  return nullptr;
}

template <typename I>
Context *AcquireRequest<I>::handle_open_object_map(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << *ret_val << dendl;

  if (*ret_val < 0) {
    lderr(cct) << "failed to open object map: " << cpp_strerror(*ret_val)
               << dendl;

    *ret_val = 0;
    delete m_object_map;
    m_object_map = nullptr;
  }

  return send_open_journal();
}

template <typename I>
void AcquireRequest<I>::send_close_object_map() {
  if (m_object_map == nullptr) {
    send_unlock();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  using klass = AcquireRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_close_object_map>(this);
  m_object_map->close(ctx);
}

template <typename I>
Context *AcquireRequest<I>::handle_close_object_map(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << *ret_val << dendl;

  // object map should never result in an error
  assert(*ret_val == 0);
  send_unlock();
  return nullptr;
}

template <typename I>
void AcquireRequest<I>::send_unlock() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  Context *ctx = create_context_callback<
    AcquireRequest<I>, &AcquireRequest<I>::handle_unlock>(this);
  m_managed_lock->release_lock(ctx);
}

template <typename I>
Context *AcquireRequest<I>::handle_unlock(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << *ret_val << dendl;

  if (*ret_val < 0) {
    lderr(cct) << "failed to unlock image: " << cpp_strerror(*ret_val) << dendl;
  }

  revert(ret_val);
  return m_on_finish;
}

template <typename I>
void AcquireRequest<I>::apply() {
  {
    RWLock::WLocker snap_locker(m_image_ctx.snap_lock);
    assert(m_image_ctx.object_map == nullptr);
    m_image_ctx.object_map = m_object_map;

    assert(m_image_ctx.journal == nullptr);
    m_image_ctx.journal = m_journal;
  }

  m_prepare_lock_completed = true;
  m_image_ctx.state->handle_prepare_lock_complete();
}

template <typename I>
void AcquireRequest<I>::revert(int *ret_val) {
  RWLock::WLocker snap_locker(m_image_ctx.snap_lock);
  m_image_ctx.object_map = nullptr;
  m_image_ctx.journal = nullptr;

  delete m_object_map;
  delete m_journal;

  assert(m_error_result < 0);
  *ret_val = m_error_result;
}

} // namespace exclusive_lock
} // namespace librbd

template class librbd::exclusive_lock::AcquireRequest<librbd::ImageCtx>;
