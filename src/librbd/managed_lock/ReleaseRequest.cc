// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/managed_lock/ReleaseRequest.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::managed_lock::ReleaseRequest: "

namespace librbd {
namespace managed_lock {

using util::detail::C_AsyncCallback;
using util::create_context_callback;
using util::create_rados_safe_callback;

template <typename L>
ReleaseRequest<L>* ReleaseRequest<L>::create(librados::IoCtx& ioctx,
                                       L *watcher,
                                       const string& oid,
                                       const string &cookie,
                                       Context *on_finish,
                                       bool shutting_down) {
  return new ReleaseRequest(ioctx, watcher, oid, cookie,
                            on_finish, shutting_down);
}

template <typename L>
ReleaseRequest<L>::ReleaseRequest(librados::IoCtx& ioctx, L *watcher,
                                  const string& oid, const string &cookie,
                                  Context *on_finish, bool shutting_down)
  : m_ioctx(ioctx), m_watcher(watcher), m_oid(oid), m_cookie(cookie),
    m_on_finish(new C_AsyncCallback<ContextWQ>(watcher->work_queue(),
                                               on_finish)),
    m_shutting_down(shutting_down) {
}

template <typename L>
ReleaseRequest<L>::~ReleaseRequest() {
}


template <typename L>
void ReleaseRequest<L>::send() {
  send_flush_notifies();
}

template <typename L>
void ReleaseRequest<L>::send_flush_notifies() {
  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  ldout(cct, 10) << __func__ << dendl;

  using klass = ReleaseRequest;
  Context *ctx = create_context_callback<
    klass, &klass::handle_flush_notifies>(this);
  m_watcher->flush(ctx);
}


template <typename L>
void ReleaseRequest<L>::handle_flush_notifies(int r) {
  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  ldout(cct, 10) << __func__ << dendl;

  assert(r == 0);
  send_unlock();
}

template <typename L>
void ReleaseRequest<L>::send_unlock() {
  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  ldout(cct, 10) << __func__ << dendl;

  librados::ObjectWriteOperation op;
  rados::cls::lock::unlock(&op, RBD_LOCK_NAME, m_cookie);

  using klass = ReleaseRequest;
  librados::AioCompletion *rados_completion =
    create_rados_safe_callback<klass, &klass::handle_unlock>(this);
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template <typename L>
void ReleaseRequest<L>::handle_unlock(int r) {
  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  ldout(cct, 10) << __func__ << ": r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "failed to unlock: " << cpp_strerror(r) << dendl;
  }

  finish();
}

template <typename L>
void ReleaseRequest<L>::finish() {
  m_on_finish->complete(0);
  delete this;
}

} // namespace managed_lock
} // namespace librbd

#include "librbd/managed_lock/LockWatcher.h"
template class librbd::managed_lock::ReleaseRequest<
                                            librbd::managed_lock::LockWatcher>;

