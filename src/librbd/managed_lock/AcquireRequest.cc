// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/managed_lock/AcquireRequest.h"
#include "librbd/managed_lock/GetLockOwnerRequest.h"
#include "librbd/managed_lock/BreakLockRequest.h"
#include "librbd/Watcher.h"
#include "librbd/ManagedLock.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/Utils.h"

#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::managed_lock::AcquireRequest: "

using std::string;

namespace librbd {

using util::detail::C_AsyncCallback;
using util::create_context_callback;
using util::create_rados_safe_callback;

namespace managed_lock {

template <typename I>
AcquireRequest<I>* AcquireRequest<I>::create(librados::IoCtx& ioctx,
                                             Watcher *watcher,
                                             ContextWQ *work_queue,
                                             const string& oid,
                                             const string& cookie,
                                             Context *on_finish) {
    return new AcquireRequest(ioctx, watcher, work_queue, oid, cookie,
                              on_finish);
}

template <typename I>
AcquireRequest<I>::AcquireRequest(librados::IoCtx& ioctx, Watcher *watcher,
                                  ContextWQ *work_queue, const string& oid,
                                  const string& cookie, Context *on_finish)
  : m_ioctx(ioctx), m_watcher(watcher),
    m_cct(reinterpret_cast<CephContext *>(m_ioctx.cct())),
    m_work_queue(work_queue), m_oid(oid), m_cookie(cookie),
    m_on_finish(new C_AsyncCallback<ContextWQ>(work_queue, on_finish)),
    m_error_result(0) {
}

template <typename I>
AcquireRequest<I>::~AcquireRequest() {
}

template <typename I>
void AcquireRequest<I>::send() {
  send_lock();
}

template <typename I>
void AcquireRequest<I>::send_lock() {
  ldout(m_cct, 10) << __func__ << dendl;

  librados::ObjectWriteOperation op;
  rados::cls::lock::lock(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, m_cookie,
                         ManagedLock<I>::WATCHER_LOCK_TAG, "", utime_t(), 0);

  using klass = AcquireRequest;
  librados::AioCompletion *rados_completion =
    create_rados_safe_callback<klass, &klass::handle_lock>(this);
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
void AcquireRequest<I>::handle_lock(int r) {
  ldout(m_cct, 10) << __func__ << ": r=" << r << dendl;

  if (r == 0) {
    finish();
    return;
  } else if (r != -EBUSY) {
    save_result(r);
    lderr(m_cct) << "failed to lock: " << cpp_strerror(r) << dendl;
    finish();
    return;
  }

  send_get_lock_owner();
}

template <typename I>
void AcquireRequest<I>::send_get_lock_owner() {
  ldout(m_cct, 10) << __func__ << dendl;

  using klass = AcquireRequest;
  Context *ctx = create_context_callback<klass, &klass::handle_get_lock_owner>(
    this);
  GetLockOwnerRequest<I>* req = GetLockOwnerRequest<I>::create(m_ioctx, m_oid,
							       &m_lock_owner, ctx);
  req->send();
}

template <typename I>
void AcquireRequest<I>::handle_get_lock_owner(int r) {
  ldout(m_cct, 10) << __func__ << ": r=" << r << dendl;

  if (r == 0) {
    ldout(m_cct, 10) << "lock owner is still alive" << dendl;
    save_result(-EAGAIN);
    finish();
    return;
  } else if (r == -ENOTCONN) {
    ldout(m_cct, 10) << "lock owner is dead" << dendl;
    send_break_lock();
    return;
  } else if (r == -ENOENT) {
    ldout(m_cct, 20) << "no valid lockers detected" << dendl;
    send_lock();
    return;
  } else {
    lderr(m_cct) << "failed to retrieve lock owner: " << cpp_strerror(r)
                 << dendl;
    save_result(r);
    finish();
  }
}

template <typename I>
void AcquireRequest<I>::send_break_lock() {
  ldout(m_cct, 10) << __func__ << dendl;

  using klass = AcquireRequest;
  Context *ctx = create_context_callback<klass, &klass::handle_break_lock>(
    this);
  bool blacklist_lock_owner = m_cct->_conf->rbd_blacklist_on_break_lock;
  BreakLockRequest<I>* req = BreakLockRequest<I>::create(m_ioctx, m_work_queue,
                                                         m_oid, m_lock_owner,
                                                         blacklist_lock_owner,
                                                         ctx);
  req->send();
}

template <typename I>
void AcquireRequest<I>::handle_break_lock(int r) {
  ldout(m_cct, 10) << __func__ << ": r=" << r << dendl;

  if (r == -ENOENT) {
    r = 0;
  } else if (r < 0) {
    lderr(m_cct) << "failed to break lock: " << cpp_strerror(r) << dendl;
    save_result(r);
    finish();
    return;
  }

  send_lock();
}

template <typename I>
void AcquireRequest<I>::finish() {
  m_on_finish->complete(m_error_result);
  delete this;
}

} // namespace managed_lock
} // namespace librbd

template class librbd::managed_lock::AcquireRequest<librbd::ImageCtx>;
