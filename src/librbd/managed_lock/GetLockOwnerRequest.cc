// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/managed_lock/GetLockOwnerRequest.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "librbd/Utils.h"

#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::managed_lock::GetLockOwnerRequest: "

using std::string;

namespace librbd {

using util::create_rados_ack_callback;

namespace managed_lock {

template <typename I>
GetLockOwnerRequest<I>* GetLockOwnerRequest<I>::create(librados::IoCtx& ioctx,
                                                       const std::string& oid,
                                                       typename ManagedLock<I>::LockOwner *owner,
                                                       Context *on_finish) {
    return new GetLockOwnerRequest(ioctx, oid, owner, on_finish);
}

template <typename I>
GetLockOwnerRequest<I>::GetLockOwnerRequest(librados::IoCtx& ioctx,
                                            const std::string& oid,
                                            typename ManagedLock<I>::LockOwner *owner,
                                            Context *on_finish)
  : m_ioctx(ioctx), m_cct(reinterpret_cast<CephContext *>(m_ioctx.cct())),
    m_oid(oid), m_lock_owner(owner), m_on_finish(on_finish) {
}

template <typename I>
void GetLockOwnerRequest<I>::send() {
  send_get_lockers();
}

template <typename I>
void GetLockOwnerRequest<I>::send_get_lockers() {
  ldout(m_cct, 10) << __func__ << dendl;

  librados::ObjectReadOperation op;
  rados::cls::lock::get_lock_info_start(&op, RBD_LOCK_NAME);

  using klass = GetLockOwnerRequest;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_get_lockers>(this);
  m_out_bl.clear();
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op, &m_out_bl);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
void GetLockOwnerRequest<I>::handle_get_lockers(int r) {
  ldout(m_cct, 10) << __func__ << ": r=" << r << dendl;

  std::map<rados::cls::lock::locker_id_t,
           rados::cls::lock::locker_info_t> lockers;
  ClsLockType lock_type = LOCK_NONE;
  std::string lock_tag;

  if (r == 0) {
    bufferlist::iterator it = m_out_bl.begin();
    r = rados::cls::lock::get_lock_info_finish(&it, &lockers,
                                               &lock_type, &lock_tag);
  }

  if (r < 0) {
    lderr(m_cct) << "failed to retrieve lockers: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (lockers.empty()) {
    ldout(m_cct, 20) << "no lockers detected" << dendl;
    finish(-ENOENT);
    return;
  }

  if (lock_tag != ManagedLock<I>::WATCHER_LOCK_TAG) {
    ldout(m_cct, 5) <<"locked by external mechanism: tag=" << lock_tag << dendl;
    finish(-EBUSY);
    return;
  }

  if (lock_type == LOCK_SHARED) {
    ldout(m_cct, 5) << "shared lock type detected" << dendl;
    finish(-EBUSY);
    return;
  }

  std::map<rados::cls::lock::locker_id_t,
           rados::cls::lock::locker_info_t>::iterator iter = lockers.begin();
  if (!ManagedLock<I>::decode_lock_cookie(iter->first.cookie, &m_lock_owner->handle)) {
    ldout(m_cct, 5) << "locked by external mechanism: "
                    << "cookie=" << iter->first.cookie << dendl;
    finish(-EBUSY);
    return;
  }

  m_lock_owner->entity = iter->first.locker;
  m_lock_owner->cookie = iter->first.cookie;
  m_lock_owner->address = stringify(iter->second.addr);
  if (m_lock_owner->cookie.empty() || m_lock_owner->address.empty()) {
    ldout(m_cct, 20) << "no valid lockers detected" << dendl;
    finish(-ENOENT);
    return;
  }

  ldout(m_cct, 10) << "retrieved exclusive locker: "
                 << m_lock_owner->entity << "@" << m_lock_owner->address << dendl;
  send_get_watchers();
}

template <typename I>
void GetLockOwnerRequest<I>::send_get_watchers() {
  ldout(m_cct, 10) << __func__ << dendl;

  librados::ObjectReadOperation op;
  op.list_watchers(&m_watchers, &m_watchers_ret_val);

  using klass = GetLockOwnerRequest;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_get_watchers>(this);
  m_out_bl.clear();
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op, &m_out_bl);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
void GetLockOwnerRequest<I>::handle_get_watchers(int r) {
  ldout(m_cct, 10) << __func__ << ": r=" << r << dendl;

  if (r == 0) {
    r = m_watchers_ret_val;
  }
  if (r < 0) {
    lderr(m_cct) << "failed to retrieve watchers: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  for (auto &watcher : m_watchers) {
    if ((strncmp(m_lock_owner->address.c_str(),
                 watcher.addr, sizeof(watcher.addr)) == 0) &&
        (m_lock_owner->handle == watcher.cookie)) {
      ldout(m_cct, 10) << "lock owner is alive" << dendl;
      finish(0);
      return;
    }
  }

  finish(-ENOTCONN);
}

template <typename I>
void GetLockOwnerRequest<I>::finish(int r) {
  m_on_finish->complete(r);
  delete this;
}

} // namespace managed_lock
} // namespace librbd

template class librbd::managed_lock::GetLockOwnerRequest<librbd::ImageCtx>;
