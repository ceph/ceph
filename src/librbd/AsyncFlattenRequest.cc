// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/AsyncFlattenRequest.h"
#include "librbd/AioRequest.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/ObjectMap.h"
#include "common/dout.h"
#include "common/errno.h"
#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AsyncFlattenRequest: "

namespace librbd {

class AsyncFlattenObjectContext : public C_AsyncObjectThrottle {
public:
  AsyncFlattenObjectContext(AsyncObjectThrottle &throttle, ImageCtx *image_ctx,
                            uint64_t object_size, ::SnapContext snapc,
                            uint64_t object_no)
    : C_AsyncObjectThrottle(throttle, *image_ctx), m_object_size(object_size),
      m_snapc(snapc), m_object_no(object_no)
  {
  }

  virtual int send() {
    assert(m_image_ctx.owner_lock.is_locked());
    CephContext *cct = m_image_ctx.cct;

    if (m_image_ctx.image_watcher->is_lock_supported() &&
        !m_image_ctx.image_watcher->is_lock_owner()) {
      ldout(cct, 1) << "lost exclusive lock during flatten" << dendl;
      return -ERESTART;
    }

    bufferlist bl;
    string oid = m_image_ctx.get_object_name(m_object_no);
    AioWrite *req = new AioWrite(&m_image_ctx, oid, m_object_no, 0, bl, m_snapc,
                                 this);
    if (!req->has_parent()) {
      // stop early if the parent went away - it just means
      // another flatten finished first or the image was resized
      delete req;
      return 1;
    }

    req->send();
    return 0;
  }

private:
  uint64_t m_object_size;
  ::SnapContext m_snapc;
  uint64_t m_object_no;
};

bool AsyncFlattenRequest::should_complete(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " should_complete: " << " r=" << r << dendl;
  if (r < 0 && !(r == -ENOENT && m_ignore_enoent) ) {
    lderr(cct) << "flatten encountered an error: " << cpp_strerror(r) << dendl;
    return true;
  }

  switch (m_state) {
  case STATE_FLATTEN_OBJECTS:
    ldout(cct, 5) << "FLATTEN_OBJECTS" << dendl;
    return send_update_header();

  case STATE_UPDATE_HEADER:
    ldout(cct, 5) << "UPDATE_HEADER" << dendl;
    return send_update_children();

  case STATE_UPDATE_CHILDREN:
    ldout(cct, 5) << "UPDATE_CHILDREN" << dendl;
    return true;

  default:
    lderr(cct) << "invalid state: " << m_state << dendl;
    assert(false);
    break;
  }
  return false;
}

void AsyncFlattenRequest::send() {
  assert(m_image_ctx.owner_lock.is_locked());
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " send" << dendl;

  m_state = STATE_FLATTEN_OBJECTS;
  AsyncObjectThrottle::ContextFactory context_factory(
    boost::lambda::bind(boost::lambda::new_ptr<AsyncFlattenObjectContext>(),
      boost::lambda::_1, &m_image_ctx, m_object_size, m_snapc,
      boost::lambda::_2));
  AsyncObjectThrottle *throttle = new AsyncObjectThrottle(
    this, m_image_ctx, context_factory, create_callback_context(), m_prog_ctx,
    0, m_overlap_objects);
  throttle->start_ops(cct->_conf->rbd_concurrent_management_ops);
}

bool AsyncFlattenRequest::send_update_header() {
  assert(m_image_ctx.owner_lock.is_locked());
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 5) << this << " send_update_header" << dendl;
  m_state = STATE_UPDATE_HEADER;

  // should have been canceled prior to releasing lock
  assert(!m_image_ctx.image_watcher->is_lock_supported() ||
         m_image_ctx.image_watcher->is_lock_owner());

  {
    RWLock::RLocker parent_locker(m_image_ctx.parent_lock);
    // stop early if the parent went away - it just means
    // another flatten finished first, so this one is useless.
    if (!m_image_ctx.parent) {
      ldout(cct, 5) << "image already flattened" << dendl;
      return true;
    }
    m_parent_spec = m_image_ctx.parent_md.spec;
  }
  m_ignore_enoent = true;

  // remove parent from this (base) image
  librados::ObjectWriteOperation op;
  if (m_image_ctx.image_watcher->is_lock_supported()) {
    m_image_ctx.image_watcher->assert_header_locked(&op);
  }
  cls_client::remove_parent(&op);

  librados::AioCompletion *rados_completion = create_callback_completion();
  int r = m_image_ctx.md_ctx.aio_operate(m_image_ctx.header_oid,
        				 rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
  return false;
}

bool AsyncFlattenRequest::send_update_children() {
  CephContext *cct = m_image_ctx.cct;

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);

  // should have been canceled prior to releasing lock
  assert(!m_image_ctx.image_watcher->is_lock_supported() ||
         m_image_ctx.image_watcher->is_lock_owner());

  // if there are no snaps, remove from the children object as well
  // (if snapshots remain, they have their own parent info, and the child
  // will be removed when the last snap goes away)
  RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
  if (!m_image_ctx.snaps.empty()) {
    return true;
  }

  ldout(cct, 2) << "removing child from children list..." << dendl;
  m_state = STATE_UPDATE_CHILDREN;

  librados::ObjectWriteOperation op;
  cls_client::remove_child(&op, m_parent_spec, m_image_ctx.id);

  librados::AioCompletion *rados_completion = create_callback_completion();
  int r = m_image_ctx.md_ctx.aio_operate(RBD_CHILDREN, rados_completion,
    				     &op);
  assert(r == 0);
  rados_completion->release();
  return false;
}

} // namespace librbd
