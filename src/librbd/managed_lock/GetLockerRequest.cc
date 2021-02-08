// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/managed_lock/GetLockerRequest.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/managed_lock/Types.h"
#include "librbd/managed_lock/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::managed_lock::GetLockerRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace managed_lock {

using librbd::util::create_rados_callback;

template <typename I>
GetLockerRequest<I>::GetLockerRequest(librados::IoCtx& ioctx,
				      const std::string& oid, bool exclusive,
                                      Locker *locker, Context *on_finish)
  : m_ioctx(ioctx), m_cct(reinterpret_cast<CephContext *>(m_ioctx.cct())),
    m_oid(oid), m_exclusive(exclusive), m_locker(locker),
    m_on_finish(on_finish) {
}

template <typename I>
void GetLockerRequest<I>::send() {
  send_get_lockers();
}

template <typename I>
void GetLockerRequest<I>::send_get_lockers() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  rados::cls::lock::get_lock_info_start(&op, RBD_LOCK_NAME);

  using klass = GetLockerRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_callback<klass, &klass::handle_get_lockers>(this);
  m_out_bl.clear();
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op, &m_out_bl);
  ceph_assert(r == 0);
  rados_completion->release();
}

template <typename I>
void GetLockerRequest<I>::handle_get_lockers(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::map<rados::cls::lock::locker_id_t,
           rados::cls::lock::locker_info_t> lockers;
  ClsLockType lock_type = ClsLockType::NONE;
  std::string lock_tag;
  if (r == 0) {
    auto it = m_out_bl.cbegin();
    r = rados::cls::lock::get_lock_info_finish(&it, &lockers, &lock_type,
                                               &lock_tag);
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

  if (lock_tag != util::get_watcher_lock_tag()) {
    ldout(m_cct, 5) <<"locked by external mechanism: tag=" << lock_tag << dendl;
    finish(-EBUSY);
    return;
  }

  if (m_exclusive && lock_type == ClsLockType::SHARED) {
    ldout(m_cct, 5) << "incompatible shared lock type detected" << dendl;
    finish(-EBUSY);
    return;
  } else if (!m_exclusive && lock_type == ClsLockType::EXCLUSIVE) {
    ldout(m_cct, 5) << "incompatible exclusive lock type detected" << dendl;
    finish(-EBUSY);
    return;
  }

  std::map<rados::cls::lock::locker_id_t,
           rados::cls::lock::locker_info_t>::iterator iter = lockers.begin();
  if (!util::decode_lock_cookie(iter->first.cookie, &m_locker->handle)) {
    ldout(m_cct, 5) << "locked by external mechanism: "
		    << "cookie=" << iter->first.cookie << dendl;
    finish(-EBUSY);
    return;
  }

  m_locker->entity = iter->first.locker;
  m_locker->cookie = iter->first.cookie;
  m_locker->address = iter->second.addr.get_legacy_str();
  if (m_locker->cookie.empty() || m_locker->address.empty()) {
    ldout(m_cct, 20) << "no valid lockers detected" << dendl;
    finish(-ENOENT);
    return;
  }

  ldout(m_cct, 10) << "retrieved exclusive locker: "
                 << m_locker->entity << "@" << m_locker->address << dendl;
  finish(0);
}

template <typename I>
void GetLockerRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace managed_lock
} // namespace librbd

template class librbd::managed_lock::GetLockerRequest<librbd::ImageCtx>;
