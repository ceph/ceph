// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/managed_lock/BreakLockRequest.h"
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
#define dout_prefix *_dout << "librbd::managed_lock::BreakLockRequest: "

using std::string;

namespace librbd {

using util::create_context_callback;
using util::create_rados_safe_callback;

namespace managed_lock {

namespace {

struct C_BlacklistClient : public Context {
  librados::IoCtx& ioctx;
  std::string locker_address;
  Context *on_finish;

  C_BlacklistClient(librados::IoCtx& ioctx, const std::string &locker_address,
                    Context *on_finish)
    : ioctx(ioctx), locker_address(locker_address),
      on_finish(on_finish) {
  }

  virtual void finish(int r) override {
    librados::Rados rados(ioctx);
    CephContext *cct = reinterpret_cast<CephContext *>(ioctx.cct());
    r = rados.blacklist_add(locker_address,
                            cct->_conf->rbd_blacklist_expire_seconds);
    on_finish->complete(r);
  }
};

} // anonymous namespace

template <typename I>
BreakLockRequest<I>* BreakLockRequest<I>::create(librados::IoCtx& ioctx,
                                                 ContextWQ *work_queue,
                                                 const std::string& oid,
                                                 const typename ManagedLock<I>::LockOwner &owner,
                                                 bool blacklist_lock_owner,
                                                 Context *on_finish) {
    return new BreakLockRequest(ioctx, work_queue, oid, owner,
                                blacklist_lock_owner, on_finish);
}

template <typename I>
BreakLockRequest<I>::BreakLockRequest(librados::IoCtx& ioctx,
                                      ContextWQ *work_queue,
                                      const std::string& oid,
                                      const typename ManagedLock<I>::LockOwner &owner,
                                      bool blacklist_lock_owner,
                                      Context *on_finish)
  : m_ioctx(ioctx), m_cct(reinterpret_cast<CephContext *>(m_ioctx.cct())),
    m_work_queue(work_queue), m_oid(oid), m_lock_owner(owner),
    m_blacklist_lock_owner(blacklist_lock_owner),
    m_on_finish(on_finish) {
}

template <typename I>
void BreakLockRequest<I>::send() {
  send_blacklist();
}

template <typename I>
void BreakLockRequest<I>::send_blacklist() {
  if (!m_blacklist_lock_owner) {
    send_break_lock();
    return;
  }
  ldout(m_cct, 10) << __func__ << dendl;

  // TODO: need async version of RadosClient::blacklist_add
  using klass = BreakLockRequest;
  Context *ctx = create_context_callback<klass, &klass::handle_blacklist>(
    this);
  m_work_queue->queue(new C_BlacklistClient(m_ioctx, m_lock_owner.address, ctx),
                      0);
}

template <typename I>
void BreakLockRequest<I>::handle_blacklist(int r) {
  ldout(m_cct, 10) << __func__ << ": r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to blacklist lock owner: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }
  send_break_lock();
}

template <typename I>
void BreakLockRequest<I>::send_break_lock() {
  ldout(m_cct, 10) << __func__ << dendl;

  librados::ObjectWriteOperation op;
  rados::cls::lock::break_lock(&op, RBD_LOCK_NAME, m_lock_owner.cookie,
                               m_lock_owner.entity);

  using klass = BreakLockRequest;
  librados::AioCompletion *rados_completion =
    create_rados_safe_callback<klass, &klass::handle_break_lock>(this);
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
void BreakLockRequest<I>::handle_break_lock(int r) {
  ldout(m_cct, 10) << __func__ << ": r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed to break lock: " << cpp_strerror(r) << dendl;
  }

  finish(r);
}

template <typename I>
void BreakLockRequest<I>::finish(int r) {
  m_on_finish->complete(r);
  delete this;
}

} // namespace managed_lock
} // namespace librbd

template class librbd::managed_lock::BreakLockRequest<librbd::ImageCtx>;
