// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/object_map/InvalidateRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::object_map::InvalidateRequest: "

namespace librbd {
namespace object_map {

void InvalidateRequest::send() {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.snap_lock.is_wlocked());

  uint64_t snap_flags;
  int r = m_image_ctx.get_flags(m_snap_id, &snap_flags);
  if (r < 0 || ((snap_flags & RBD_FLAG_OBJECT_MAP_INVALID) != 0)) {
    async_complete(r);
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  lderr(cct) << this << " invalidating object map in-memory" << dendl;

  // update in-memory flags
  uint64_t flags = RBD_FLAG_OBJECT_MAP_INVALID;
  if ((m_image_ctx.features & RBD_FEATURE_FAST_DIFF) != 0) {
    flags |= RBD_FLAG_FAST_DIFF_INVALID;
  }

  r = m_image_ctx.update_flags(m_snap_id, flags, true);
  if (r < 0) {
    async_complete(r);
  }

  // do not update on-disk flags if not image owner
  if (m_image_ctx.image_watcher == NULL ||
      (m_image_ctx.image_watcher->is_lock_supported(m_image_ctx.snap_lock) &&
       !m_image_ctx.image_watcher->is_lock_owner() && !m_force)) {
    async_complete(0);
    return;
  }

  lderr(cct) << this << " invalidating object map on-disk" << dendl;
  librados::ObjectWriteOperation op;
  if (m_snap_id == CEPH_NOSNAP && !m_force) {
    m_image_ctx.image_watcher->assert_header_locked(&op);
  }
  cls_client::set_flags(&op, m_snap_id, flags, flags);

  librados::AioCompletion *rados_completion = create_callback_completion();
  r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, rados_completion,
                                     &op);
  assert(r == 0);
  rados_completion->release();
}

bool InvalidateRequest::should_complete(int r) {
  CephContext *cct = m_image_ctx.cct;
  lderr(cct) << this << " " << __func__ << ": r=" << r << dendl;
  return true;
}

} // namespace object_map
} // namespace librbd
