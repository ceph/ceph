// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "librbd/operation/CheckObjectMapRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "include/rbd/object_map_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/ObjectMap.h"
#include "librbd/operation/ObjectMapIterate.h"

#include <shared_mutex> // for std::shared_lock

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::CheckObjectMapRequest: "

namespace librbd {
namespace operation {

namespace {

template <typename I>
bool needs_invalidate(I& image_ctx, ObjectMap<I> &object_map,
		      uint64_t object_no, uint8_t current_state,
		      uint8_t new_state) {
  if ( (current_state == OBJECT_EXISTS ||
	current_state == OBJECT_EXISTS_CLEAN) &&
       (new_state == OBJECT_NONEXISTENT ||
	new_state == OBJECT_PENDING)) {
    return false;
  }
  return true;
}

} // anonymous namespace

template <typename I>
CheckObjectMapRequest<I>::~CheckObjectMapRequest() {
  if (m_opened_object_map != nullptr) {
    m_opened_object_map->put();
    m_opened_object_map = nullptr;
  }
}

template <typename I>
void CheckObjectMapRequest<I>::send() {
  uint64_t snap_id;
  {
    std::shared_lock image_locker{m_image_ctx.image_lock};
    m_object_map = m_image_ctx.object_map;
    snap_id = m_image_ctx.snap_id;
  }

  if (m_object_map == nullptr) {
    if (snap_id == CEPH_NOSNAP) {
      // the HEAD object map is loaded when the exclusive lock is acquired
      lderr(m_image_ctx.cct) << "object map is not loaded" << dendl;
      this->async_complete(-EINVAL);
      return;
    }

    send_open_object_map(snap_id);
    return;
  }

  send_verify_objects();
}

template <typename I>
bool CheckObjectMapRequest<I>::should_complete(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " should_complete: " << " r=" << r << dendl;

  std::shared_lock owner_lock{m_image_ctx.owner_lock};
  switch (m_state) {
  case STATE_OPEN_OBJECT_MAP:
    ldout(cct, 5) << "OPEN_OBJECT_MAP" << dendl;
    if (r == 0) {
      send_verify_objects();
      return false;
    }
    break;

  case STATE_VERIFY_OBJECTS:
    ldout(cct, 5) << "VERIFY_OBJECTS" << dendl;
    break;

  default:
    ceph_abort();
    break;
  }

  if (r < 0) {
    lderr(cct) << "check object map encountered an error: " << cpp_strerror(r)
               << dendl;
  }
  return true;
}

template <typename I>
void CheckObjectMapRequest<I>::send_open_object_map(uint64_t snap_id) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 5) << this << " send_open_object_map" << dendl;
  m_state = STATE_OPEN_OBJECT_MAP;

  // a clone snapshot's object map isn't loaded into the image context while
  // the snapshot still has a live parent overlap -- it can be invalidated by
  // a copyup at any time and must not be trusted by the read path.  Open a
  // private instance for the duration of the operation instead.
  m_opened_object_map = m_image_ctx.create_object_map(snap_id);
  m_object_map = m_opened_object_map;
  m_opened_object_map->open(this->create_callback_context());
}

template <typename I>
void CheckObjectMapRequest<I>::send_verify_objects() {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 5) << this << " send_verify_objects" << dendl;
  m_state = STATE_VERIFY_OBJECTS;

  ObjectMapIterateRequest<I> *req =
    new ObjectMapIterateRequest<I>(m_image_ctx,
				   this->create_callback_context(),
				   m_prog_ctx, *m_object_map,
				   needs_invalidate);

  req->send();
}

} // namespace operation
} // namespace librbd

template class librbd::operation::CheckObjectMapRequest<librbd::ImageCtx>;
