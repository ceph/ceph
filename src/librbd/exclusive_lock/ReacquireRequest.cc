// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/exclusive_lock/ReacquireRequest.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/lock/cls_lock_types.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::exclusive_lock::ReacquireRequest: " \
                           << this << ": " << __func__

namespace librbd {
namespace exclusive_lock {

using librbd::util::create_rados_safe_callback;

template <typename I>
ReacquireRequest<I>::ReacquireRequest(I &image_ctx,
                                      const std::string &old_cookie,
                                      const std::string &new_cookie,
                                      Context *on_finish)
  : m_image_ctx(image_ctx), m_old_cookie(old_cookie), m_new_cookie(new_cookie),
    m_on_finish(on_finish) {
}

template <typename I>
void ReacquireRequest<I>::send() {
  set_cookie();
}

template <typename I>
void ReacquireRequest<I>::set_cookie() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  rados::cls::lock::set_cookie(&op, RBD_LOCK_NAME, LOCK_EXCLUSIVE, m_old_cookie,
                               ExclusiveLock<>::WATCHER_LOCK_TAG, m_new_cookie);

  librados::AioCompletion *rados_completion = create_rados_safe_callback<
    ReacquireRequest<I>, &ReacquireRequest<I>::handle_set_cookie>(this);
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid,
                                         rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
void ReacquireRequest<I>::handle_set_cookie(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << ": r=" << r << dendl;

  if (r == -EOPNOTSUPP) {
    ldout(cct, 10) << ": OSD doesn't support updating lock" << dendl;
  } else if (r < 0) {
    lderr(cct) << ": failed to update lock: " << cpp_strerror(r) << dendl;
  }

  m_on_finish->complete(r);
  delete this;
}

} // namespace exclusive_lock
} // namespace librbd

template class librbd::exclusive_lock::ReacquireRequest<librbd::ImageCtx>;
