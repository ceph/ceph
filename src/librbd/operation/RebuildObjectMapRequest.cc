// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/RebuildObjectMapRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "osdc/Striper.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "librbd/operation/ResizeRequest.h"
#include "librbd/operation/TrimRequest.h"
#include "librbd/operation/ObjectMapIterate.h"
#include "librbd/Utils.h"
#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>

#include <shared_mutex> // for std::shared_lock

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::RebuildObjectMapRequest: "

namespace librbd {
namespace operation {

using util::create_context_callback;

template <typename I>
void RebuildObjectMapRequest<I>::send() {
  send_resize_object_map();
}

template <typename I>
bool RebuildObjectMapRequest<I>::should_complete(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " should_complete: " << " r=" << r << dendl;

  std::shared_lock owner_lock{m_image_ctx.owner_lock};
  switch (m_state) {
  case STATE_RESIZE_OBJECT_MAP:
    ldout(cct, 5) << "RESIZE_OBJECT_MAP" << dendl;
    if (r == -ESTALE && !m_attempted_trim) {
      // objects are still flagged as in-use -- delete them
      m_attempted_trim = true;
      send_trim_image();
      return false;
    } else if (r == 0) {
      send_verify_objects();
    }
    break;

  case STATE_TRIM_IMAGE:
    ldout(cct, 5) << "TRIM_IMAGE" << dendl;
    if (r == 0) {
      send_resize_object_map();
    }
    break;

  case STATE_VERIFY_OBJECTS:
    ldout(cct, 5) << "VERIFY_OBJECTS" << dendl;
    if (r == 0) {
      send_save_object_map();
    }
    break;

  case STATE_SAVE_OBJECT_MAP:
    ldout(cct, 5) << "SAVE_OBJECT_MAP" << dendl;
    if (r == 0) {
      send_update_header();
    }
    break;
  case STATE_UPDATE_HEADER:
    ldout(cct, 5) << "UPDATE_HEADER" << dendl;
    if (r == 0) {
      return true;
    }
    break;

  default:
    ceph_abort();
    break;
  }

  if (r == -ERESTART) {
    ldout(cct, 5) << "rebuild object map operation interrupted" << dendl;
    return true;
  } else if (r < 0) {
    lderr(cct) << "rebuild object map encountered an error: " << cpp_strerror(r)
               << dendl;
    return true;
  }
  return false;
}

template <typename I>
void RebuildObjectMapRequest<I>::send_resize_object_map() {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  CephContext *cct = m_image_ctx.cct;

  m_image_ctx.image_lock.lock_shared();
  ceph_assert(m_image_ctx.object_map != nullptr);

  uint64_t size = get_image_size();
  uint64_t num_objects = Striper::get_num_objects(m_image_ctx.layout, size);

  if (m_image_ctx.object_map->size() == num_objects) {
    m_image_ctx.image_lock.unlock_shared();
    send_verify_objects();
    return;
  }

  ldout(cct, 5) << this << " send_resize_object_map" << dendl;
  m_state = STATE_RESIZE_OBJECT_MAP;

  // should have been canceled prior to releasing lock
  ceph_assert(m_image_ctx.exclusive_lock == nullptr ||
              m_image_ctx.exclusive_lock->is_lock_owner());

  m_image_ctx.object_map->aio_resize(size, OBJECT_NONEXISTENT,
                                     this->create_callback_context());
  m_image_ctx.image_lock.unlock_shared();
}

template <typename I>
void RebuildObjectMapRequest<I>::send_trim_image() {
  CephContext *cct = m_image_ctx.cct;

  std::shared_lock l{m_image_ctx.owner_lock};

  // should have been canceled prior to releasing lock
  ceph_assert(m_image_ctx.exclusive_lock == nullptr ||
              m_image_ctx.exclusive_lock->is_lock_owner());
  ldout(cct, 5) << this << " send_trim_image" << dendl;
  m_state = STATE_TRIM_IMAGE;

  uint64_t new_size;
  uint64_t orig_size;
  {
    std::shared_lock l{m_image_ctx.image_lock};
    ceph_assert(m_image_ctx.object_map != nullptr);

    new_size = get_image_size();
    orig_size = m_image_ctx.get_object_size() *
                m_image_ctx.object_map->size();
  }
  TrimRequest<I> *req = TrimRequest<I>::create(m_image_ctx,
                                               this->create_callback_context(),
                                               orig_size, new_size, m_prog_ctx);
  req->send();
}

template <typename I>
bool update_object_map(I& image_ctx, uint64_t object_no, uint8_t current_state,
		      uint8_t new_state) {
  CephContext *cct = image_ctx.cct;
  uint64_t snap_id = image_ctx.snap_id;

  current_state = (*image_ctx.object_map)[object_no];
  if (current_state == OBJECT_EXISTS && new_state == OBJECT_NONEXISTENT &&
      snap_id == CEPH_NOSNAP) {
    // might be writing object to OSD concurrently
    new_state = current_state;
  }

  if (new_state != current_state) {
    ldout(cct, 15) << image_ctx.get_object_name(object_no)
                   << " rebuild updating object map "
                   << static_cast<uint32_t>(current_state) << "->"
                   << static_cast<uint32_t>(new_state) << dendl;
    image_ctx.object_map->set_state(object_no, new_state, current_state);
  }
  return false;
}

template <typename I>
void RebuildObjectMapRequest<I>::send_verify_objects() {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  CephContext *cct = m_image_ctx.cct;

  m_state = STATE_VERIFY_OBJECTS;
  ldout(cct, 5) << this << " send_verify_objects" << dendl;

  ObjectMapIterateRequest<I> *req =
    new ObjectMapIterateRequest<I>(m_image_ctx,
				   this->create_callback_context(),
				   m_prog_ctx, update_object_map);

  req->send();
}

template <typename I>
void RebuildObjectMapRequest<I>::send_save_object_map() {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 5) << this << " send_save_object_map" << dendl;
  m_state = STATE_SAVE_OBJECT_MAP;

  // should have been canceled prior to releasing lock
  ceph_assert(m_image_ctx.exclusive_lock == nullptr ||
              m_image_ctx.exclusive_lock->is_lock_owner());

  std::shared_lock image_locker{m_image_ctx.image_lock};
  ceph_assert(m_image_ctx.object_map != nullptr);
  m_image_ctx.object_map->aio_save(this->create_callback_context());
}

template <typename I>
void RebuildObjectMapRequest<I>::send_update_header() {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));

  // should have been canceled prior to releasing lock
  ceph_assert(m_image_ctx.exclusive_lock == nullptr ||
              m_image_ctx.exclusive_lock->is_lock_owner());

  ldout(m_image_ctx.cct, 5) << this << " send_update_header" << dendl;
  m_state = STATE_UPDATE_HEADER;

  librados::ObjectWriteOperation op;

  uint64_t flags = RBD_FLAG_OBJECT_MAP_INVALID | RBD_FLAG_FAST_DIFF_INVALID;
  cls_client::set_flags(&op, m_image_ctx.snap_id, 0, flags);

  librados::AioCompletion *comp = this->create_callback_completion();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid, comp, &op);
  ceph_assert(r == 0);
  comp->release();

  std::unique_lock image_locker{m_image_ctx.image_lock};
  m_image_ctx.update_flags(m_image_ctx.snap_id, flags, false);
}

template <typename I>
uint64_t RebuildObjectMapRequest<I>::get_image_size() const {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.image_lock));
  if (m_image_ctx.snap_id == CEPH_NOSNAP) {
    if (!m_image_ctx.resize_reqs.empty()) {
      return m_image_ctx.resize_reqs.front()->get_image_size();
    } else {
      return m_image_ctx.size;
    }
  }
  return  m_image_ctx.get_image_size(m_image_ctx.snap_id);
}

} // namespace operation
} // namespace librbd

template class librbd::operation::RebuildObjectMapRequest<librbd::ImageCtx>;
