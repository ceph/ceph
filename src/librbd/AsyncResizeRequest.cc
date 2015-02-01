// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "librbd/AsyncResizeRequest.h"
#include "librbd/AsyncTrimRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "common/dout.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AsyncResizeRequest: "

namespace librbd
{

bool AsyncResizeRequest::should_complete(int r)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " should_complete: " << " r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "resize encountered an error: " << cpp_strerror(r) << dendl;
    RWLock::WLocker l(m_image_ctx.md_lock);
    if (m_image_ctx.size == m_new_size) {
      m_image_ctx.size = m_original_size;
    }
    return true;
  }

  switch (m_state) {
  case STATE_TRIM_IMAGE:
    ldout(cct, 5) << "TRIM_IMAGE" << dendl;
    send_grow_object_map();
    break;

  case STATE_GROW_OBJECT_MAP:
    ldout(cct, 5) << "GROW_OBJECT_MAP" << dendl;
    send_update_header();
    break;

  case STATE_UPDATE_HEADER:
    ldout(cct, 5) << "UPDATE_HEADER" << dendl;
    if (send_shrink_object_map()) {
      return true;
    }
    break;

  case STATE_SHRINK_OBJECT_MAP:
    ldout(cct, 5) << "SHRINK_OBJECT_MAP" << dendl;
    return true;

  case STATE_FINISHED:
    ldout(cct, 5) << "FINISHED" << dendl;
    return true;

  default:
    lderr(cct) << "invalid state: " << m_state << dendl;
    assert(false);
    break;
  }
  return false;
}

void AsyncResizeRequest::send() {
  CephContext *cct = m_image_ctx.cct;
  if (m_original_size == m_new_size) {
    ldout(cct, 2) << this << " no change in size (" << m_original_size
		  << " -> " << m_new_size << ")" << dendl; 
    m_state = STATE_FINISHED;
    complete(0);
  } else if (m_new_size > m_original_size) {
    ldout(cct, 2) << this << " expanding image (" << m_original_size
		  << " -> " << m_new_size << ")" << dendl; 
    send_grow_object_map();
  } else {
    ldout(cct, 2) << this << " shrinking image (" << m_original_size
		  << " -> " << m_new_size << ")" << dendl; 
    send_trim_image();
  }
}

void AsyncResizeRequest::send_trim_image() {
  ldout(m_image_ctx.cct, 5) << this << " send_trim_image: "
                            << " original_size=" << m_original_size
                            << " new_size=" << m_new_size << dendl;
  m_state = STATE_TRIM_IMAGE;

  {
    // update in-memory size to clip concurrent IO operations
    RWLock::WLocker l(m_image_ctx.md_lock);
    m_image_ctx.size = m_new_size;
  }

  AsyncTrimRequest *req = new AsyncTrimRequest(m_image_ctx,
					       create_callback_context(),
					       m_original_size, m_new_size,
					       m_prog_ctx);
  req->send();
}

void AsyncResizeRequest::send_grow_object_map() {
  bool lost_exclusive_lock = false;
  bool object_map_enabled = true;
  {
    RWLock::RLocker l(m_image_ctx.owner_lock);
    RWLock::RLocker l2(m_image_ctx.md_lock);
    if (m_image_ctx.object_map == NULL) {
      object_map_enabled = false;
    } else { 
      ldout(m_image_ctx.cct, 5) << this << " send_grow_object_map: "
                                << " original_size=" << m_original_size
                                << " new_size=" << m_new_size << dendl;
      m_state = STATE_GROW_OBJECT_MAP;

      if (m_image_ctx.image_watcher->is_lock_supported() &&
	  !m_image_ctx.image_watcher->is_lock_owner()) {
	ldout(m_image_ctx.cct, 1) << "lost exclusive lock during grow object map" << dendl;
	lost_exclusive_lock = true;
      } else {
	m_image_ctx.object_map->aio_resize(m_new_size, OBJECT_NONEXISTENT,
					   create_callback_context());
	object_map_enabled = true;
      }
    }
  }

  if (!object_map_enabled) {
    send_update_header();
  } else if (lost_exclusive_lock) {
    // only complete when not holding locks
    complete(-ERESTART);
  }
}

bool AsyncResizeRequest::send_shrink_object_map() {
  bool lost_exclusive_lock = false;
  {
    RWLock::RLocker l(m_image_ctx.owner_lock);
    RWLock::RLocker l2(m_image_ctx.md_lock);
    if (m_image_ctx.object_map == NULL ||
	m_new_size > m_original_size) {
      return true;
    }

    ldout(m_image_ctx.cct, 5) << this << " send_shrink_object_map: "
			      << " original_size=" << m_original_size
			      << " new_size=" << m_new_size << dendl;
    m_state = STATE_SHRINK_OBJECT_MAP;

    if (m_image_ctx.image_watcher->is_lock_supported() &&
        !m_image_ctx.image_watcher->is_lock_owner()) {
      ldout(m_image_ctx.cct, 1) << "lost exclusive lock during shrink object map" << dendl;
      lost_exclusive_lock = true;
    } else {
      m_image_ctx.object_map->aio_resize(m_new_size, OBJECT_NONEXISTENT,
					 create_callback_context());
    }
  }

  if (lost_exclusive_lock) {
    // only complete when not holding locks
    complete(-ERESTART);
  }
  return false;
}

void AsyncResizeRequest::send_update_header() {
  bool lost_exclusive_lock = false;

  ldout(m_image_ctx.cct, 5) << this << " send_update_header: "
                            << " original_size=" << m_original_size
                            << " new_size=" << m_new_size << dendl;
  m_state = STATE_UPDATE_HEADER;

  {
    RWLock::RLocker l(m_image_ctx.owner_lock); 
    RWLock::WLocker l2(m_image_ctx.md_lock);
    if (m_image_ctx.image_watcher->is_lock_supported() &&
	!m_image_ctx.image_watcher->is_lock_owner()) {
      ldout(m_image_ctx.cct, 1) << "lost exclusive lock during header update" << dendl;
      lost_exclusive_lock = true;
    } else {
      m_image_ctx.size = m_new_size;

      librados::ObjectWriteOperation op;
      if (m_image_ctx.old_format) {
	// rewrite header
	bufferlist bl;
	m_image_ctx.header.image_size = m_new_size;
	bl.append((const char *)&m_image_ctx.header, sizeof(m_image_ctx.header));
	op.write(0, bl);
      } else {
	if (m_image_ctx.image_watcher->is_lock_supported()) {
	  m_image_ctx.image_watcher->assert_header_locked(&op);
	}
	cls_client::set_size(&op, m_new_size);
      }

      librados::AioCompletion *rados_completion = create_callback_completion();
      int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid,
					     rados_completion, &op);
      assert(r == 0);
      rados_completion->release();
    }
  }

  if (lost_exclusive_lock) {
    // only complete when not holding locks
    complete(-ERESTART);
  }
}

} // namespace librbd
