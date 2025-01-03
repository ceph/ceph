// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/object_map/InvalidateRequest.h"
#include "common/dout.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::object_map::InvalidateRequest: "

namespace librbd {
namespace object_map {

template <typename I>
InvalidateRequest<I>* InvalidateRequest<I>::create(I &image_ctx,
                                                   uint64_t snap_id, bool force,
                                                   Context *on_finish) {
  return new InvalidateRequest<I>(image_ctx, snap_id, force, on_finish);
}

template <typename I>
void InvalidateRequest<I>::send() {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));
  ceph_assert(ceph_mutex_is_wlocked(image_ctx.image_lock));

  uint64_t snap_flags;
  int r = image_ctx.get_flags(m_snap_id, &snap_flags);
  if (r < 0 || ((snap_flags & RBD_FLAG_OBJECT_MAP_INVALID) != 0)) {
    this->async_complete(r);
    return;
  }

  CephContext *cct = image_ctx.cct;
  lderr(cct) << this << " invalidating object map in-memory" << dendl;

  // update in-memory flags
  uint64_t flags = RBD_FLAG_OBJECT_MAP_INVALID;
  if ((image_ctx.features & RBD_FEATURE_FAST_DIFF) != 0) {
    flags |= RBD_FLAG_FAST_DIFF_INVALID;
  }

  r = image_ctx.update_flags(m_snap_id, flags, true);
  if (r < 0) {
    this->async_complete(r);
    return;
  }

  // do not update on-disk flags if not image owner
  if (image_ctx.image_watcher == nullptr ||
      (!m_force && m_snap_id == CEPH_NOSNAP &&
       image_ctx.exclusive_lock != nullptr &&
       !image_ctx.exclusive_lock->is_lock_owner())) {
    this->async_complete(-EROFS);
    return;
  }

  lderr(cct) << this << " invalidating object map on-disk" << dendl;
  librados::ObjectWriteOperation op;
  cls_client::set_flags(&op, m_snap_id, flags, flags);

  librados::AioCompletion *rados_completion =
    this->create_callback_completion();
  r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid, rados_completion,
                                   &op);
  ceph_assert(r == 0);
  rados_completion->release();
}

template <typename I>
bool InvalidateRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  lderr(cct) << this << " " << __func__ << ": r=" << r << dendl;
  return true;
}

} // namespace object_map
} // namespace librbd

template class librbd::object_map::InvalidateRequest<librbd::ImageCtx>;
