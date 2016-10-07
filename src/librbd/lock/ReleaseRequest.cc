// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/lock/ReleaseRequest.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/lock/LockWatcher.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::lock::ReleaseRequest: "

namespace librbd {
namespace lock {

using util::detail::C_AsyncCallback;
using util::create_context_callback;
using util::create_rados_safe_callback;


ReleaseRequest* ReleaseRequest::create(librados::IoCtx& ioctx,
                                       ContextWQ *work_queue,
                                       LockWatcher *watcher,
                                       const string& oid,
                                       const string &cookie,
                                       Context *on_releasing,
                                       Context *on_finish,
                                       bool shutting_down) {
  return new ReleaseRequest(ioctx, work_queue, watcher, oid, cookie,
                            on_releasing, on_finish, shutting_down);
}

ReleaseRequest::ReleaseRequest(librados::IoCtx& ioctx, ContextWQ *work_queue,
                               LockWatcher *watcher, const string& oid,
                               const string &cookie, Context *on_releasing,
                               Context *on_finish, bool shutting_down)
  : m_ioctx(ioctx), m_work_queue(work_queue), m_watcher(watcher), m_oid(oid),
    m_cookie(cookie), m_on_releasing(on_releasing),
    m_on_finish(new C_AsyncCallback<ContextWQ>(work_queue, on_finish)),
    m_shutting_down(shutting_down) {
}


ReleaseRequest::~ReleaseRequest() {
}


void ReleaseRequest::send() {
  send_flush_notifies();
}

void ReleaseRequest::send_flush_notifies() {
  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  ldout(cct, 10) << __func__ << dendl;

  using klass = ReleaseRequest;
  Context *ctx = create_context_callback<
    klass, &klass::handle_flush_notifies>(this);
  m_watcher->flush(ctx);
}


Context *ReleaseRequest::handle_flush_notifies(int *ret_val) {
  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  ldout(cct, 10) << __func__ << dendl;

  assert(*ret_val == 0);
  send_unlock();
  return nullptr;
}

void ReleaseRequest::send_unlock() {
  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  ldout(cct, 10) << __func__ << dendl;

  if (m_on_releasing != nullptr) {
    // alert caller that we no longer own the exclusive lock
    m_on_releasing->complete(0);
    m_on_releasing = nullptr;
  }

  librados::ObjectWriteOperation op;
  rados::cls::lock::unlock(&op, RBD_LOCK_NAME, m_cookie);

  using klass = ReleaseRequest;
  librados::AioCompletion *rados_completion =
    create_rados_safe_callback<klass, &klass::handle_unlock>(this);
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

Context *ReleaseRequest::handle_unlock(int *ret_val) {
  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  ldout(cct, 10) << __func__ << ": r=" << *ret_val << dendl;

  if (*ret_val < 0 && *ret_val != -ENOENT) {
    lderr(cct) << "failed to unlock: " << cpp_strerror(*ret_val) << dendl;
  }

  // treat errors as the image is unlocked
  *ret_val = 0;
  return m_on_finish;
}

} // namespace lock
} // namespace librbd

