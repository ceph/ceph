// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/ResizeRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "librbd/operation/TrimRequest.h"
#include "common/dout.h"
#include "common/errno.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ResizeRequest: "

namespace librbd {
namespace operation {

template <typename I>
ResizeRequest<I>::ResizeRequest(I &image_ctx, Context *on_finish,
                                uint64_t new_size, ProgressContext &prog_ctx)
  : Request<I>(image_ctx, on_finish),
    m_original_size(0), m_new_size(new_size), m_prog_ctx(prog_ctx),
    m_new_parent_overlap(0), m_xlist_item(this)
{
}

template <typename I>
ResizeRequest<I>::~ResizeRequest() {
  I &image_ctx = this->m_image_ctx;
  ResizeRequest *next_req = NULL;
  {
    RWLock::WLocker snap_locker(image_ctx.snap_lock);
    assert(m_xlist_item.remove_myself());
    if (!image_ctx.resize_reqs.empty()) {
      next_req = image_ctx.resize_reqs.front();
    }
  }

  if (next_req != NULL) {
    RWLock::RLocker owner_locker(image_ctx.owner_lock);
    next_req->send();
  }
}

template <typename I>
bool ResizeRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " should_complete: " << " r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "resize encountered an error: " << cpp_strerror(r) << dendl;
    return true;
  }
  if (m_state == STATE_FINISHED) {
    ldout(cct, 5) << "FINISHED" << dendl;
    return true;
  }

  RWLock::RLocker owner_lock(image_ctx.owner_lock);
  switch (m_state) {
  case STATE_FLUSH:
    ldout(cct, 5) << "FLUSH" << dendl;
    send_invalidate_cache();
    break;

  case STATE_INVALIDATE_CACHE:
    ldout(cct, 5) << "INVALIDATE_CACHE" << dendl;
    send_trim_image();
    break;

  case STATE_TRIM_IMAGE:
    ldout(cct, 5) << "TRIM_IMAGE" << dendl;
    send_update_header();
    break;

  case STATE_GROW_OBJECT_MAP:
    ldout(cct, 5) << "GROW_OBJECT_MAP" << dendl;
    send_update_header();
    break;

  case STATE_UPDATE_HEADER:
    ldout(cct, 5) << "UPDATE_HEADER" << dendl;
    if (send_shrink_object_map()) {
      update_size_and_overlap();
      return true;
    }
    break;

  case STATE_SHRINK_OBJECT_MAP:
    ldout(cct, 5) << "SHRINK_OBJECT_MAP" << dendl;
    update_size_and_overlap();
    return true;

  default:
    lderr(cct) << "invalid state: " << m_state << dendl;
    assert(false);
    break;
  }
  return false;
}

template <typename I>
void ResizeRequest<I>::send() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  {
    RWLock::WLocker snap_locker(image_ctx.snap_lock);
    if (!m_xlist_item.is_on_list()) {
      image_ctx.resize_reqs.push_back(&m_xlist_item);
      if (image_ctx.resize_reqs.front() != this) {
        return;
      }
    }

    assert(image_ctx.resize_reqs.front() == this);
    m_original_size = image_ctx.size;
    compute_parent_overlap();
  }

  Request<I>::send();
}

template <typename I>
void ResizeRequest<I>::send_op() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  if (this->is_canceled()) {
    this->async_complete(-ERESTART);
  } else if (m_original_size == m_new_size) {
    ldout(cct, 2) << this << " no change in size (" << m_original_size
		  << " -> " << m_new_size << ")" << dendl;
    m_state = STATE_FINISHED;
    this->async_complete(0);
  } else if (m_new_size > m_original_size) {
    ldout(cct, 2) << this << " expanding image (" << m_original_size
		  << " -> " << m_new_size << ")" << dendl;
    send_grow_object_map();
  } else {
    ldout(cct, 2) << this << " shrinking image (" << m_original_size
		  << " -> " << m_new_size << ")" << dendl;
    send_flush();
  }
}

template <typename I>
void ResizeRequest<I>::send_flush() {
  I &image_ctx = this->m_image_ctx;
  ldout(image_ctx.cct, 5) << this << " send_flush: "
                          << " original_size=" << m_original_size
                          << " new_size=" << m_new_size << dendl;
  m_state = STATE_FLUSH;

  // with clipping adjusted, ensure that write / copy-on-read operations won't
  // (re-)create objects that we just removed. need async callback to ensure
  // we don't have cache_lock already held
  image_ctx.flush_async_operations(this->create_async_callback_context());
}

template <typename I>
void ResizeRequest<I>::send_invalidate_cache() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());
  ldout(image_ctx.cct, 5) << this << " send_invalidate_cache: "
                          << " original_size=" << m_original_size
                          << " new_size=" << m_new_size << dendl;
  m_state = STATE_INVALIDATE_CACHE;

  // need to invalidate since we're deleting objects, and
  // ObjectCacher doesn't track non-existent objects
  image_ctx.invalidate_cache(this->create_callback_context());
}

template <typename I>
void ResizeRequest<I>::send_trim_image() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());
  ldout(image_ctx.cct, 5) << this << " send_trim_image: "
                          << " original_size=" << m_original_size
                          << " new_size=" << m_new_size << dendl;
  m_state = STATE_TRIM_IMAGE;

  TrimRequest<I> *req = new TrimRequest<I>(image_ctx,
                                           this->create_callback_context(),
				           m_original_size, m_new_size,
                                           m_prog_ctx);
  req->send();
}

template <typename I>
void ResizeRequest<I>::send_grow_object_map() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());
  if (!image_ctx.object_map.enabled()) {
    send_update_header();
    return;
  }

  ldout(image_ctx.cct, 5) << this << " send_grow_object_map: "
                          << " original_size=" << m_original_size
                          << " new_size=" << m_new_size << dendl;
  m_state = STATE_GROW_OBJECT_MAP;

  // should have been canceled prior to releasing lock
  assert(!image_ctx.image_watcher->is_lock_supported() ||
         image_ctx.image_watcher->is_lock_owner());

  image_ctx.object_map.aio_resize(m_new_size, OBJECT_NONEXISTENT,
				  this->create_callback_context());
}

template <typename I>
bool ResizeRequest<I>::send_shrink_object_map() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());
  if (!image_ctx.object_map.enabled() || m_new_size > m_original_size) {
    return true;
  }

  ldout(image_ctx.cct, 5) << this << " send_shrink_object_map: "
		            << " original_size=" << m_original_size
			    << " new_size=" << m_new_size << dendl;
  m_state = STATE_SHRINK_OBJECT_MAP;

  // should have been canceled prior to releasing lock
  assert(!image_ctx.image_watcher->is_lock_supported() ||
         image_ctx.image_watcher->is_lock_owner());

  image_ctx.object_map.aio_resize(m_new_size, OBJECT_NONEXISTENT,
				  this->create_callback_context());
  return false;
}

template <typename I>
void ResizeRequest<I>::send_update_header() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  ldout(image_ctx.cct, 5) << this << " send_update_header: "
                            << " original_size=" << m_original_size
                            << " new_size=" << m_new_size << dendl;
  m_state = STATE_UPDATE_HEADER;

  // should have been canceled prior to releasing lock
  assert(!image_ctx.image_watcher->is_lock_supported() ||
         image_ctx.image_watcher->is_lock_owner());

  librados::ObjectWriteOperation op;
  if (image_ctx.old_format) {
    // rewrite only the size field of the header
    // NOTE: format 1 image headers are not stored in fixed endian format
    bufferlist bl;
    bl.append(reinterpret_cast<const char*>(&m_new_size), sizeof(m_new_size));
    op.write(offsetof(rbd_obj_header_ondisk, image_size), bl);
  } else {
    if (image_ctx.image_watcher->is_lock_supported()) {
      image_ctx.image_watcher->assert_header_locked(&op);
    }
    cls_client::set_size(&op, m_new_size);
  }

  librados::AioCompletion *rados_completion =
    this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid,
    				       rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
}

template <typename I>
void ResizeRequest<I>::compute_parent_overlap() {
  I &image_ctx = this->m_image_ctx;
  RWLock::RLocker l2(image_ctx.parent_lock);
  if (image_ctx.parent == NULL) {
    m_new_parent_overlap = 0;
  } else {
    m_new_parent_overlap = MIN(m_new_size, image_ctx.parent_md.overlap);
  }
}

template <typename I>
void ResizeRequest<I>::update_size_and_overlap() {
  I &image_ctx = this->m_image_ctx;
  RWLock::WLocker snap_locker(image_ctx.snap_lock);
  image_ctx.size = m_new_size;

  RWLock::WLocker parent_locker(image_ctx.parent_lock);
  if (image_ctx.parent != NULL && m_new_size < m_original_size) {
    image_ctx.parent_md.overlap = m_new_parent_overlap;
  }
}

} // namespace operation
} // namespace librbd

template class librbd::operation::ResizeRequest<librbd::ImageCtx>;
