// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/exclusive_lock/GetLockerRequest.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/stringify.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/exclusive_lock/Types.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::exclusive_lock::GetLockerRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace exclusive_lock {

using util::create_rados_ack_callback;

template <typename I>
void GetLockerRequest<I>::send() {
  send_get_lockers();
}

template <typename I>
void GetLockerRequest<I>::send_get_lockers() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  librados::ObjectReadOperation op;
  rados::cls::lock::get_lock_info_start(&op, RBD_LOCK_NAME);

  using klass = GetLockerRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_ack_callback<klass, &klass::handle_get_lockers>(this);
  m_out_bl.clear();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid,
                                         rados_completion, &op, &m_out_bl);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
void GetLockerRequest<I>::handle_get_lockers(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

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
    lderr(cct) << "failed to retrieve lockers: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (lockers.empty()) {
    ldout(cct, 20) << "no lockers detected" << dendl;
    finish(-ENOENT);
    return;
  }

  if (lock_tag != ExclusiveLock<>::WATCHER_LOCK_TAG) {
    ldout(cct, 5) <<"locked by external mechanism: tag=" << lock_tag << dendl;
    finish(-EBUSY);
    return;
  }

  if (lock_type == LOCK_SHARED) {
    ldout(cct, 5) << "shared lock type detected" << dendl;
    finish(-EBUSY);
    return;
  }

  std::map<rados::cls::lock::locker_id_t,
           rados::cls::lock::locker_info_t>::iterator iter = lockers.begin();
  if (!ExclusiveLock<>::decode_lock_cookie(iter->first.cookie,
                                            &m_locker->handle)) {
    ldout(cct, 5) << "locked by external mechanism: "
                  << "cookie=" << iter->first.cookie << dendl;
    finish(-EBUSY);
    return;
  }

  m_locker->entity = iter->first.locker;
  m_locker->cookie = iter->first.cookie;
  m_locker->address = stringify(iter->second.addr);
  if (m_locker->cookie.empty() || m_locker->address.empty()) {
    ldout(cct, 20) << "no valid lockers detected" << dendl;
    finish(-ENOENT);
    return;
  }

  ldout(cct, 10) << "retrieved exclusive locker: "
                 << m_locker->entity << "@" << m_locker->address << dendl;
  finish(0);
}

template <typename I>
void GetLockerRequest<I>::finish(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace exclusive_lock
} // namespace librbd

template class librbd::exclusive_lock::GetLockerRequest<librbd::ImageCtx>;

