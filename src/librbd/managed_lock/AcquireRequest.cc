// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/managed_lock/AcquireRequest.h"
#include "librbd/Watcher.h"
#include "librbd/ManagedLock.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "include/stringify.h"
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
using util::create_rados_ack_callback;

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

  send_get_lockers();
}

template <typename I>
void AcquireRequest<I>::send_get_lockers() {
  ldout(m_cct, 10) << __func__ << dendl;

  librados::ObjectReadOperation op;
  rados::cls::lock::get_lock_info_start(&op, RBD_LOCK_NAME);

  using klass = AcquireRequest;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_get_lockers>(this);
  m_out_bl.clear();
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op, &m_out_bl);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
void AcquireRequest<I>::handle_get_lockers(int r) {
  ldout(m_cct, 10) << __func__ << ": r=" << r << dendl;

  std::map<rados::cls::lock::locker_id_t,
           rados::cls::lock::locker_info_t> lockers;
  ClsLockType lock_type;
  std::string lock_tag;

  if (r == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    r = rados::cls::lock::get_lock_info_finish(&it, &lockers,
                                               &lock_type, &lock_tag);
  }

  save_result(r);
  if (r < 0) {
    lderr(m_cct) << "failed to retrieve lockers: " << cpp_strerror(r) << dendl;
    finish();
    return;
  }

  if (lockers.empty()) {
    ldout(m_cct, 20) << "no lockers detected" << dendl;
    send_lock();
    return;
  }

  if (lock_tag != ManagedLock<I>::WATCHER_LOCK_TAG) {
    ldout(m_cct, 5) <<"locked by external mechanism: tag=" << lock_tag << dendl;
    save_result(-EBUSY);
    finish();
    return;
  }

  if (lock_type == LOCK_SHARED) {
    ldout(m_cct, 5) << "shared lock type detected" << dendl;
    save_result(-EBUSY);
    finish();
    return;
  }

  std::map<rados::cls::lock::locker_id_t,
           rados::cls::lock::locker_info_t>::iterator iter = lockers.begin();
  if (!ManagedLock<I>::decode_lock_cookie(iter->first.cookie, &m_locker_handle)) {
    ldout(m_cct, 5) << "locked by external mechanism: "
                    << "cookie=" << iter->first.cookie << dendl;
    save_result(-EBUSY);
    finish();
    return;
  }

  m_locker_entity = iter->first.locker;
  m_locker_cookie = iter->first.cookie;
  m_locker_address = stringify(iter->second.addr);
  if (m_locker_cookie.empty() || m_locker_address.empty()) {
    ldout(m_cct, 20) << "no valid lockers detected" << dendl;
    send_lock();
    return;
  }

  ldout(m_cct, 10) << "retrieved exclusive locker: "
                 << m_locker_entity << "@" << m_locker_address << dendl;
  send_get_watchers();
}

template <typename I>
void AcquireRequest<I>::send_get_watchers() {
  ldout(m_cct, 10) << __func__ << dendl;

  librados::ObjectReadOperation op;
  op.list_watchers(&m_watchers, &m_watchers_ret_val);

  using klass = AcquireRequest;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_get_watchers>(this);
  m_out_bl.clear();
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op, &m_out_bl);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
void AcquireRequest<I>::handle_get_watchers(int r) {
  ldout(m_cct, 10) << __func__ << ": r=" << r << dendl;

  if (r == 0) {
    r = m_watchers_ret_val;
  }
  save_result(r);
  if (r < 0) {
    lderr(m_cct) << "failed to retrieve watchers: " << cpp_strerror(r) << dendl;
    finish();
    return;
  }

  for (auto &watcher : m_watchers) {
    if ((strncmp(m_locker_address.c_str(),
                 watcher.addr, sizeof(watcher.addr)) == 0) &&
        (m_locker_handle == watcher.cookie)) {
      ldout(m_cct, 10) << "lock owner is still alive" << dendl;

      save_result(-EAGAIN);
      finish();
      return;
    }
  }

  send_blacklist();
}

template <typename I>
void AcquireRequest<I>::send_blacklist() {
  if (!m_cct->_conf->rbd_blacklist_on_break_lock) {
    send_break_lock();
    return;
  }
  ldout(m_cct, 10) << __func__ << dendl;

  // TODO: need async version of RadosClient::blacklist_add
  using klass = AcquireRequest;
  Context *ctx = create_context_callback<klass, &klass::handle_blacklist>(
    this);
  m_work_queue->queue(
      new C_BlacklistClient(m_ioctx, m_locker_address, ctx), 0);
}
template <typename I>
void AcquireRequest<I>::handle_blacklist(int r) {
  ldout(m_cct, 10) << __func__ << ": r=" << r << dendl;

  save_result(r);
  if (r < 0) {
    lderr(m_cct) << "failed to blacklist lock owner: " << cpp_strerror(r)
                 << dendl;
    finish();
    return;
  }
  send_break_lock();
}

template <typename I>
void AcquireRequest<I>::send_break_lock() {
  ldout(m_cct, 10) << __func__ << dendl;

  librados::ObjectWriteOperation op;
  rados::cls::lock::break_lock(&op, RBD_LOCK_NAME, m_locker_cookie,
                               m_locker_entity);

  using klass = AcquireRequest;
  librados::AioCompletion *rados_completion =
    create_rados_safe_callback<klass, &klass::handle_break_lock>(this);
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
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
