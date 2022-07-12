// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/managed_lock/ReacquireRequest.h"
#include "librbd/Watcher.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/managed_lock/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::managed_lock::ReacquireRequest: " \
                           << this << ": " << __func__

using std::string;

namespace librbd {
namespace managed_lock {

using librbd::util::create_rados_callback;

template <typename I>
ReacquireRequest<I>::ReacquireRequest(librados::IoCtx& ioctx,
                                      const string& oid,
                                      const string& old_cookie,
                                      const string &new_cookie,
                                      bool exclusive,
                                      Context *on_finish)
  : m_ioctx(ioctx), m_oid(oid), m_old_cookie(old_cookie),
    m_new_cookie(new_cookie), m_exclusive(exclusive), m_on_finish(on_finish) {
}


template <typename I>
void ReacquireRequest<I>::send() {
  set_cookie();
}

template <typename I>
void ReacquireRequest<I>::set_cookie() {
  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  ldout(cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  rados::cls::lock::set_cookie(&op, RBD_LOCK_NAME,
                               m_exclusive ? ClsLockType::EXCLUSIVE : ClsLockType::SHARED,
                               m_old_cookie, util::get_watcher_lock_tag(),
                               m_new_cookie);

  librados::AioCompletion *rados_completion = create_rados_callback<
    ReacquireRequest, &ReacquireRequest::handle_set_cookie>(this);
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op);
  ceph_assert(r == 0);
  rados_completion->release();
}

template <typename I>
void ReacquireRequest<I>::handle_set_cookie(int r) {
  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  ldout(cct, 10) << ": r=" << r << dendl;

  if (r == -EOPNOTSUPP) {
    ldout(cct, 10) << ": OSD doesn't support updating lock" << dendl;
  } else if (r < 0) {
    lderr(cct) << ": failed to update lock: " << cpp_strerror(r) << dendl;
  }

  m_on_finish->complete(r);
  delete this;
}

} // namespace managed_lock
} // namespace librbd

template class librbd::managed_lock::ReacquireRequest<librbd::ImageCtx>;
