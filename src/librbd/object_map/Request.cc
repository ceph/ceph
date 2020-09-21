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
    if (r == -ETIMEDOUT &&
        !cct->_conf.get_val<bool>("rbd_invalidate_object_map_on_timeout")) {
      m_state = STATE_TIMEOUT;
      return true;
    } else if (r < 0) {
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
    ceph_abort();
    break;
  }
  return false;
}

bool Request::invalidate() {
  bool flags_set;
  int r = m_image_ctx.test_flags(m_snap_id, RBD_FLAG_OBJECT_MAP_INVALID,
                                 &flags_set);
  if (r < 0 || flags_set) {
    return true;
  }

  m_state = STATE_INVALIDATE;

  std::shared_lock owner_locker{m_image_ctx.owner_lock};
  std::unique_lock image_locker{m_image_ctx.image_lock};
  InvalidateRequest<> *req = new InvalidateRequest<>(m_image_ctx, m_snap_id,
                                                     true,
                                                     create_callback_context());
  req->send();
  return false;
}

} // namespace object_map
} // namespace librbd
