// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/lock/ReacquireRequest.h"
#include "librbd/Lock.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::lock::ReacquireRequest: " \
                           << this << ": " << __func__

using std::string;

namespace librbd {
namespace lock {

using librbd::util::create_rados_safe_callback;


ReacquireRequest::ReacquireRequest(librados::IoCtx& ioctx,
                                   const string& oid, const string &old_cookie,
                                   const std::string &new_cookie,
                                   Context *on_finish)
  : m_ioctx(ioctx), m_oid(oid), m_old_cookie(old_cookie),
    m_new_cookie(new_cookie), m_on_finish(on_finish) {
}


void ReacquireRequest::send() {
  set_cookie();
}

void ReacquireRequest::set_cookie() {
  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  ldout(cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  rados::cls::lock::set_cookie(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, m_old_cookie,
                               Lock::WATCHER_LOCK_TAG, m_new_cookie);

  librados::AioCompletion *rados_completion = create_rados_safe_callback<
    ReacquireRequest, &ReacquireRequest::handle_set_cookie>(this);
  int r = m_ioctx.aio_operate(m_oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}


void ReacquireRequest::handle_set_cookie(int r) {
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

} // namespace lock
} // namespace librbd

