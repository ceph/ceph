// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/exclusive_lock/ReleaseRequest.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "include/stringify.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::exclusive_lock::ReleaseRequest: "

namespace librbd {
namespace exclusive_lock {

using util::create_async_context_callback;
using util::create_context_callback;
using util::create_rados_safe_callback;

template <typename I>
ReleaseRequest<I>* ReleaseRequest<I>::create(I &image_ctx,
                                             const std::string &cookie,
                                             Context *on_finish) {
  return new ReleaseRequest(image_ctx, cookie, on_finish);
}

template <typename I>
ReleaseRequest<I>::ReleaseRequest(I &image_ctx, const std::string &cookie,
                                  Context *on_finish)
  : m_image_ctx(image_ctx), m_cookie(cookie),
    m_on_finish(create_async_context_callback(image_ctx, on_finish)),
    m_object_map(nullptr), m_journal(nullptr) {
}

template <typename I>
void ReleaseRequest<I>::send() {
  send_cancel_op_requests();
}

template <typename I>
void ReleaseRequest<I>::send_cancel_op_requests() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  using klass = ReleaseRequest<I>;
  Context *ctx = create_context_callback<klass,
                                         &klass::handle_cancel_op_requests>(this);
  m_image_ctx.cancel_async_requests(ctx);
}

template <typename I>
Context *ReleaseRequest<I>::handle_cancel_op_requests(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << *ret_val << dendl;

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
    send_unlock_object_map();
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

  using klass = ReleaseRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_close_journal>(
    this);
  m_journal->close(ctx);
}

template <typename I>
Context *ReleaseRequest<I>::handle_close_journal(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << *ret_val << dendl;

  if (*ret_val < 0) {
    // error implies some journal events were not flushed -- continue
    lderr(cct) << "failed to close journal: " << cpp_strerror(*ret_val)
               << dendl;
  }

  delete m_journal;

  send_unlock_object_map();
  return nullptr;
}

template <typename I>
void ReleaseRequest<I>::send_unlock_object_map() {
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

  using klass = ReleaseRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_unlock_object_map>(this);
  m_object_map->unlock(ctx);
}

template <typename I>
Context *ReleaseRequest<I>::handle_unlock_object_map(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << ": r=" << *ret_val << dendl;

  // object map shouldn't return errors
  assert(*ret_val == 0);
  delete m_object_map;

  send_unlock();
  return nullptr;
}

template <typename I>
void ReleaseRequest<I>::send_unlock() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << __func__ << dendl;

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
  ldout(cct, 10) << __func__ << ": r=" << *ret_val << dendl;

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
