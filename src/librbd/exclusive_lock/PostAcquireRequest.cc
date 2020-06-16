// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/exclusive_lock/PostAcquireRequest.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "librbd/ExclusiveLock.h"
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
#define dout_prefix *_dout << "librbd::exclusive_lock::PostAcquireRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace exclusive_lock {

using util::create_async_context_callback;
using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
PostAcquireRequest<I>* PostAcquireRequest<I>::create(I &image_ctx,
                                                     Context *on_acquire,
                                                     Context *on_finish) {
  return new PostAcquireRequest(image_ctx, on_acquire, on_finish);
}

template <typename I>
PostAcquireRequest<I>::PostAcquireRequest(I &image_ctx, Context *on_acquire,
                                          Context *on_finish)
  : m_image_ctx(image_ctx),
    m_on_acquire(on_acquire),
    m_on_finish(create_async_context_callback(image_ctx, on_finish)),
    m_object_map(nullptr), m_journal(nullptr), m_error_result(0) {
}

template <typename I>
PostAcquireRequest<I>::~PostAcquireRequest() {
  if (!m_prepare_lock_completed) {
    m_image_ctx.state->handle_prepare_lock_complete();
  }
  delete m_on_acquire;
}

template <typename I>
void PostAcquireRequest<I>::send() {
  send_refresh();
}

template <typename I>
void PostAcquireRequest<I>::send_refresh() {
  if (!m_image_ctx.state->is_refresh_required()) {
    send_open_object_map();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  using klass = PostAcquireRequest<I>;
  Context *ctx = create_async_context_callback(
    m_image_ctx, create_context_callback<klass, &klass::handle_refresh>(this));

  // ImageState is blocked waiting for lock to complete -- safe to directly
  // refresh
  image::RefreshRequest<I> *req = image::RefreshRequest<I>::create(
    m_image_ctx, true, false, ctx);
  req->send();
}

template <typename I>
void PostAcquireRequest<I>::handle_refresh(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r == -ERESTART) {
    // next issued IO or op will (re)-refresh the image and shut down lock
    ldout(cct, 5) << "exclusive lock dynamically disabled" << dendl;
    r = 0;
  } else if (r < 0) {
    lderr(cct) << "failed to refresh image: " << cpp_strerror(r) << dendl;
    save_result(r);
    revert();
    finish();
    return;
  }

  send_open_object_map();
}

template <typename I>
void PostAcquireRequest<I>::send_open_journal() {
  // alert caller that we now own the exclusive lock
  m_on_acquire->complete(0);
  m_on_acquire = nullptr;

  bool journal_enabled;
  {
    std::shared_lock image_locker{m_image_ctx.image_lock};
    journal_enabled = (m_image_ctx.test_features(RBD_FEATURE_JOURNALING,
                                                 m_image_ctx.image_lock) &&
                       !m_image_ctx.get_journal_policy()->journal_disabled());
  }
  if (!journal_enabled) {
    apply();
    finish();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  using klass = PostAcquireRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_open_journal>(
    this);
  m_journal = m_image_ctx.create_journal();

  // journal playback requires object map (if enabled) and itself
  apply();

  m_journal->open(ctx);
}

template <typename I>
void PostAcquireRequest<I>::handle_open_journal(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  save_result(r);
  if (r < 0) {
    lderr(cct) << "failed to open journal: " << cpp_strerror(r) << dendl;
    send_close_journal();
    return;
  }

  send_allocate_journal_tag();
}

template <typename I>
void PostAcquireRequest<I>::send_allocate_journal_tag() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  std::shared_lock image_locker{m_image_ctx.image_lock};
  using klass = PostAcquireRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_allocate_journal_tag>(this, m_journal);
  m_image_ctx.get_journal_policy()->allocate_tag_on_lock(ctx);
}

template <typename I>
void PostAcquireRequest<I>::handle_allocate_journal_tag(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  save_result(r);
  if (r < 0) {
    lderr(cct) << "failed to allocate journal tag: " << cpp_strerror(r)
               << dendl;
    send_close_journal();
    return;
  }

  finish();
}

template <typename I>
void PostAcquireRequest<I>::send_close_journal() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  using klass = PostAcquireRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_close_journal>(
    this);
  m_journal->close(ctx);
}

template <typename I>
void PostAcquireRequest<I>::handle_close_journal(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  save_result(r);
  if (r < 0) {
    lderr(cct) << "failed to close journal: " << cpp_strerror(r) << dendl;
  }

  send_close_object_map();
}

template <typename I>
void PostAcquireRequest<I>::send_open_object_map() {
  if (!m_image_ctx.test_features(RBD_FEATURE_OBJECT_MAP)) {
    send_open_journal();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  using klass = PostAcquireRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_open_object_map>(
    this);

  m_object_map = m_image_ctx.create_object_map(CEPH_NOSNAP);
  m_object_map->open(ctx);
}

template <typename I>
void PostAcquireRequest<I>::handle_open_object_map(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to open object map: " << cpp_strerror(r) << dendl;
    m_object_map->put();
    m_object_map = nullptr;

    if (r != -EFBIG) {
      save_result(r);
      revert();
      finish();
      return;
    }
  }

  send_open_journal();
}

template <typename I>
void PostAcquireRequest<I>::send_close_object_map() {
  if (m_object_map == nullptr) {
    revert();
    finish();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  using klass = PostAcquireRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_close_object_map>(this);
  m_object_map->close(ctx);
}

template <typename I>
void PostAcquireRequest<I>::handle_close_object_map(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to close object map: " << cpp_strerror(r) << dendl;
  }

  revert();
  finish();
}

template <typename I>
void PostAcquireRequest<I>::apply() {
  {
    std::unique_lock image_locker{m_image_ctx.image_lock};
    ceph_assert(m_image_ctx.object_map == nullptr);
    m_image_ctx.object_map = m_object_map;

    ceph_assert(m_image_ctx.journal == nullptr);
    m_image_ctx.journal = m_journal;
  }

  m_prepare_lock_completed = true;
  m_image_ctx.state->handle_prepare_lock_complete();
}

template <typename I>
void PostAcquireRequest<I>::revert() {
  std::unique_lock image_locker{m_image_ctx.image_lock};
  m_image_ctx.object_map = nullptr;
  m_image_ctx.journal = nullptr;

  if (m_object_map) {
    m_object_map->put();
  }
  if (m_journal) {
    m_journal->put();
  }

  ceph_assert(m_error_result < 0);
}

template <typename I>
void PostAcquireRequest<I>::finish() {
  m_on_finish->complete(m_error_result);
  delete this;
}

} // namespace exclusive_lock
} // namespace librbd

template class librbd::exclusive_lock::PostAcquireRequest<librbd::ImageCtx>;
