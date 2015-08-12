// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/object_map/Request.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "common/RWLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::object_map::Request: "

namespace librbd {
namespace object_map {

bool Request::should_complete(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << &m_image_ctx << " should_complete: r=" << r << dendl;

  switch (m_state)
  {
  case STATE_REQUEST:
    if (r == -EBUSY) {
      lderr(cct) << "object map lock not owned by client" << dendl;
      return true;
    } else if (r < 0) {
      lderr(cct) << "failed to update object map: " << cpp_strerror(r)
		 << dendl;
      return invalidate();
    }

    {
      RWLock::WLocker l2(m_image_ctx.object_map_lock);
      finish();
    }
    return true;

  case STATE_INVALIDATE:
    ldout(cct, 20) << "INVALIDATE" << dendl;
    if (r < 0) {
      lderr(cct) << "failed to invalidate object map: " << cpp_strerror(r)
		 << dendl;
    }
    return true;

  default:
    lderr(cct) << "invalid state: " << m_state << dendl;
    assert(false);
    break;
  }
  return false;
}

bool Request::invalidate() {
  if (m_image_ctx.test_flags(RBD_FLAG_OBJECT_MAP_INVALID)) {
    return true;
  }

  CephContext *cct = m_image_ctx.cct;
  RWLock::WLocker snap_locker(m_image_ctx.snap_lock);

  // requests shouldn't be running while using snapshots
  assert(m_image_ctx.snap_id == CEPH_NOSNAP);

  uint64_t flags = RBD_FLAG_OBJECT_MAP_INVALID;
  if ((m_image_ctx.features & RBD_FEATURE_FAST_DIFF) != 0) {
    flags |= RBD_FLAG_FAST_DIFF_INVALID;
  }

  lderr(cct) << &m_image_ctx << " invalidating object map" << dendl;
  m_state = STATE_INVALIDATE;
  m_image_ctx.flags |= flags;

  librados::ObjectWriteOperation op;
  m_image_ctx.image_watcher->assert_header_locked(&op);
  cls_client::set_flags(&op, CEPH_NOSNAP, flags, flags);

  librados::AioCompletion *rados_completion = create_callback_completion();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid,
					 rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
  return false;
}

} // namespace object_map
} // namespace librbd
