// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/object_map/Request.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/RWLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/object_map/InvalidateRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::object_map::Request: "

namespace librbd {
namespace object_map {

bool Request::should_complete(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << this << " should_complete: r=" << r << dendl;

  switch (m_state)
  {
  case STATE_REQUEST:
    if (r < 0) {
      lderr(cct) << "failed to update object map: " << cpp_strerror(r)
		 << dendl;
      return invalidate();
    }

    finish_request();
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

  m_state = STATE_INVALIDATE;

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  RWLock::WLocker snap_locker(m_image_ctx.snap_lock);
  InvalidateRequest<> *req = new InvalidateRequest<>(m_image_ctx, m_snap_id,
                                                     true,
                                                     create_callback_context());
  req->send();
  return false;
}

} // namespace object_map
} // namespace librbd
