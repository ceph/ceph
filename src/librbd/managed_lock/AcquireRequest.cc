// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/managed_lock/AcquireRequest.h"
#include "librbd/Watcher.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/managed_lock/BreakRequest.h"
#include "librbd/managed_lock/GetLockerRequest.h"
#include "librbd/managed_lock/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::managed_lock::AcquireRequest: " << this \
                           << " " << __func__ << ": "

using std::string;

namespace librbd {

using librbd::util::detail::C_AsyncCallback;
using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

namespace managed_lock {

template <typename I>
AcquireRequest<I>* AcquireRequest<I>::create(librados::IoCtx& ioctx,
                                             Watcher *watcher,
                                             asio::ContextWQ *work_queue,
                                             const string& oid,
                                             const string& cookie,
                                             bool exclusive,
					     bool blacklist_on_break_lock,
					     uint32_t blacklist_expire_seconds,
                                             Context *on_finish) {
    return new AcquireRequest(ioctx, watcher, work_queue, oid, cookie,
                              exclusive, blacklist_on_break_lock,
                              blacklist_expire_seconds, on_finish);
}

template <typename I>
AcquireRequest<I>::AcquireRequest(librados::IoCtx& ioctx, Watcher *watcher,
                                  asio::ContextWQ *work_queue,
                                  const string& oid,
                                  const string& cookie, bool exclusive,
                                  bool blacklist_on_break_lock,
                                  uint32_t blacklist_expire_seconds,
                                  Context *on_finish)
  : m_ioctx(ioctx), m_watcher(watcher),
    m_cct(reinterpret_cast<CephContext *>(m_ioctx.cct())),
    m_work_queue(work_queue), m_oid(oid), m_cookie(cookie),
    m_exclusive(exclusive),
    m_blacklist_on_break_lock(blacklist_on_break_lock),
    m_blacklist_expire_seconds(blacklist_expire_seconds),
    m_on_finish(new C_AsyncCallback<asio::ContextWQ>(work_queue, on_finish)) {
}

template <typename I>
AcquireRequest<I>::~AcquireRequest() {
}

template <typename I>
void AcquireRequest<I>::send() {
  send_get_locker();
}

template <typename I>
void AcquireRequest<I>::send_get_locker() {
  ldout(m_cct, 10) << dendl;

  Context *ctx = create_context_callback<
    AcquireRequest<I>, &AcquireRequest<I>::handle_get_locker>(this);
  auto req = GetLockerRequest<I>::create(m_ioctx, m_oid, m_exclusive,
                                         &m_locker, ctx);
  req->send();
}

template <typename I>
void AcquireRequest<I>::handle_get_locker(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r == -ENOENT) {
    ldout(m_cct, 20) << "no lockers detected" << dendl;
    m_locker = {};
  } else if (r == -EBUSY) {
    ldout(m_cct, 5) << "incompatible lock detected" << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "failed to retrieve lockers: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_lock();
}

template <typename I>
void AcquireRequest<I>::send_lock() {
  ldout(m_cct, 10) << "entity=client." << m_ioctx.get_instance_id() << ", "
                   << "cookie=" << m_cookie << dendl;

  librados::ObjectWriteOperation op;
  rados::cls::lock::lock(&op, RBD_LOCK_NAME,
                         m_exclusive ? LOCK_EXCLUSIVE : LOCK_SHARED, m_cookie,
                         util::get_watcher_lock_tag(), "", utime_t(), 0);

  using klass = AcquireRequest;
  librados::AioCompletion *rados_completion =
    create_rados_callback<klass, &klass::handle_lock>(this);
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op);
  ceph_assert(r == 0);
  rados_completion->release();
}

template <typename I>
void AcquireRequest<I>::handle_lock(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r == 0) {
    finish(0);
    return;
  } else if (r == -EBUSY && m_locker.cookie.empty()) {
    ldout(m_cct, 5) << "already locked, refreshing locker" << dendl;
    send_get_locker();
    return;
  } else if (r != -EBUSY) {
    lderr(m_cct) << "failed to lock: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_break_lock();
}

template <typename I>
void AcquireRequest<I>::send_break_lock() {
  ldout(m_cct, 10) << dendl;

  Context *ctx = create_context_callback<
    AcquireRequest<I>, &AcquireRequest<I>::handle_break_lock>(this);
  auto req = BreakRequest<I>::create(
    m_ioctx, m_work_queue, m_oid, m_locker, m_exclusive,
    m_blacklist_on_break_lock, m_blacklist_expire_seconds, false, ctx);
  req->send();
}

template <typename I>
void AcquireRequest<I>::handle_break_lock(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r == -EAGAIN) {
    ldout(m_cct, 5) << "lock owner is still alive" << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    lderr(m_cct) << "failed to break lock : " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  m_locker = {};
  send_lock();
}

template <typename I>
void AcquireRequest<I>::finish(int r) {
  m_on_finish->complete(r);
  delete this;
}

} // namespace managed_lock
} // namespace librbd

template class librbd::managed_lock::AcquireRequest<librbd::ImageCtx>;
