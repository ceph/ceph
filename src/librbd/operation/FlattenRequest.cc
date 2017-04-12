// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/FlattenRequest.h"
#include "librbd/AioObjectRequest.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "common/dout.h"
#include "common/errno.h"
#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::FlattenRequest: "

namespace librbd {
namespace operation {

template <typename I>
class C_FlattenObject : public C_AsyncObjectThrottle<I> {
public:
  C_FlattenObject(AsyncObjectThrottle<I> &throttle, I *image_ctx,
                  uint64_t object_size, ::SnapContext snapc, uint64_t object_no)
    : C_AsyncObjectThrottle<I>(throttle, *image_ctx), m_object_size(object_size),
      m_snapc(snapc), m_object_no(object_no)
  {
  }

  virtual int send() {
    I &image_ctx = this->m_image_ctx;
    assert(image_ctx.owner_lock.is_locked());
    CephContext *cct = image_ctx.cct;

    if (image_ctx.exclusive_lock != nullptr &&
        !image_ctx.exclusive_lock->is_lock_owner()) {
      ldout(cct, 1) << "lost exclusive lock during flatten" << dendl;
      return -ERESTART;
    }

    bufferlist bl;
    string oid = image_ctx.get_object_name(m_object_no);
    AioObjectWrite *req = new AioObjectWrite(&image_ctx, oid, m_object_no, 0,
                                             bl, m_snapc, this, 0);
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

template <typename I>
bool FlattenRequest<I>::should_complete(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " should_complete: " << " r=" << r << dendl;
  if (r == -ERESTART) {
    ldout(cct, 5) << "flatten operation interrupted" << dendl;
    return true;
  } else if (r < 0 && !(r == -ENOENT && m_ignore_enoent) ) {
    lderr(cct) << "flatten encountered an error: " << cpp_strerror(r) << dendl;
    return true;
  }

  RWLock::RLocker owner_locker(image_ctx.owner_lock);
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

template <typename I>
void FlattenRequest<I>::send_op() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " send" << dendl;

  m_state = STATE_FLATTEN_OBJECTS;
  typename AsyncObjectThrottle<I>::ContextFactory context_factory(
    boost::lambda::bind(boost::lambda::new_ptr<C_FlattenObject<I> >(),
      boost::lambda::_1, &image_ctx, m_object_size, m_snapc,
      boost::lambda::_2));
  AsyncObjectThrottle<I> *throttle = new AsyncObjectThrottle<I>(
    this, image_ctx, context_factory, this->create_callback_context(), &m_prog_ctx,
    0, m_overlap_objects);
  throttle->start_ops(image_ctx.concurrent_management_ops);
}

template <typename I>
bool FlattenRequest<I>::send_update_header() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());
  CephContext *cct = image_ctx.cct;

  ldout(cct, 5) << this << " send_update_header" << dendl;
  m_state = STATE_UPDATE_HEADER;

  // should have been canceled prior to releasing lock
  assert(image_ctx.exclusive_lock == nullptr ||
         image_ctx.exclusive_lock->is_lock_owner());

  {
    RWLock::RLocker parent_locker(image_ctx.parent_lock);
    // stop early if the parent went away - it just means
    // another flatten finished first, so this one is useless.
    if (!image_ctx.parent) {
      ldout(cct, 5) << "image already flattened" << dendl;
      return true;
    }
    m_parent_spec = image_ctx.parent_md.spec;
  }
  m_ignore_enoent = true;

  // remove parent from this (base) image
  librados::ObjectWriteOperation op;
  cls_client::remove_parent(&op);

  librados::AioCompletion *rados_completion = this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(image_ctx.header_oid,
        				 rados_completion, &op);
  assert(r == 0);
  rados_completion->release();
  return false;
}

template <typename I>
bool FlattenRequest<I>::send_update_children() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());
  CephContext *cct = image_ctx.cct;

  // should have been canceled prior to releasing lock
  assert(image_ctx.exclusive_lock == nullptr ||
         image_ctx.exclusive_lock->is_lock_owner());

  // if there are no snaps, remove from the children object as well
  // (if snapshots remain, they have their own parent info, and the child
  // will be removed when the last snap goes away)
  RWLock::RLocker snap_locker(image_ctx.snap_lock);
  if ((image_ctx.features & RBD_FEATURE_DEEP_FLATTEN) == 0 &&
      !image_ctx.snaps.empty()) {
    return true;
  }

  ldout(cct, 2) << "removing child from children list..." << dendl;
  m_state = STATE_UPDATE_CHILDREN;

  librados::ObjectWriteOperation op;
  cls_client::remove_child(&op, m_parent_spec, image_ctx.id);

  librados::AioCompletion *rados_completion = this->create_callback_completion();
  int r = image_ctx.md_ctx.aio_operate(RBD_CHILDREN, rados_completion,
    				     &op);
  assert(r == 0);
  rados_completion->release();
  return false;
}

} // namespace operation
} // namespace librbd

template class librbd::operation::FlattenRequest<librbd::ImageCtx>;
