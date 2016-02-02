// -*- mode:C; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/object_map/UnlockRequest.h"
#include "cls/lock/cls_lock_client.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::object_map::UnlockRequest: "

namespace librbd {
namespace object_map {

using util::create_rados_safe_callback;

template <typename I>
UnlockRequest<I>::UnlockRequest(I &image_ctx, Context *on_finish)
  : m_image_ctx(image_ctx), m_on_finish(on_finish) {
}

template <typename I>
void UnlockRequest<I>::send() {
  send_unlock();
}

template <typename I>
void UnlockRequest<I>::send_unlock() {
  CephContext *cct = m_image_ctx.cct;
  std::string oid(ObjectMap::object_map_name(m_image_ctx.id, CEPH_NOSNAP));
  ldout(cct, 10) << this << " " << __func__ << ": oid=" << oid << dendl;

  librados::ObjectWriteOperation op;
  rados::cls::lock::unlock(&op, RBD_LOCK_NAME, "");

  using klass = UnlockRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_safe_callback<klass, &klass::handle_unlock>(this);
  int r = m_image_ctx.md_ctx.aio_operate(oid, rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
Context *UnlockRequest<I>::handle_unlock(int *ret_val) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << *ret_val << dendl;

  if (*ret_val < 0 && *ret_val != -ENOENT) {
    lderr(m_image_ctx.cct) << "failed to release object map lock: "
                           << cpp_strerror(*ret_val) << dendl;

  }

  *ret_val = 0;
  return m_on_finish;
}

} // namespace object_map
} // namespace librbd

template class librbd::object_map::UnlockRequest<librbd::ImageCtx>;
