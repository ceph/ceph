// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/TrimRequest.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/AioObjectRequest.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
#include "librbd/Utils.h"
#include "common/ContextCompletion.h"
#include "common/dout.h"
#include "common/errno.h"
#include "osdc/Striper.h"

#include <boost/bind.hpp>
#include <boost/lambda/bind.hpp>
#include <boost/lambda/construct.hpp>
#include <boost/scope_exit.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::TrimRequest: "

namespace librbd {
namespace operation {

template <typename I>
class C_CopyupObject : public C_AsyncObjectThrottle<I> {
public:
  C_CopyupObject(AsyncObjectThrottle<I> &throttle, I *image_ctx,
                 ::SnapContext snapc, uint64_t object_no)
    : C_AsyncObjectThrottle<I>(throttle, *image_ctx), m_snapc(snapc),
      m_object_no(object_no)
  {
  }

  virtual int send() {
    I &image_ctx = this->m_image_ctx;
    assert(image_ctx.owner_lock.is_locked());
    assert(image_ctx.exclusive_lock == nullptr ||
           image_ctx.exclusive_lock->is_lock_owner());

    string oid = image_ctx.get_object_name(m_object_no);
    ldout(image_ctx.cct, 10) << "removing (with copyup) " << oid << dendl;

    AioObjectRequest<> *req = new AioObjectTrim(&image_ctx, oid, m_object_no,
                                                m_snapc, this);
    req->send();
    return 0;
  }
private:
  ::SnapContext m_snapc;
  uint64_t m_object_no;
};

template <typename I>
class C_RemoveObject : public C_AsyncObjectThrottle<I> {
public:
  C_RemoveObject(AsyncObjectThrottle<I> &throttle, ImageCtx *image_ctx,
                 uint64_t object_no)
    : C_AsyncObjectThrottle<I>(throttle, *image_ctx), m_object_no(object_no)
  {
  }

  virtual int send() {
    I &image_ctx = this->m_image_ctx;
    assert(image_ctx.owner_lock.is_locked());
    assert(image_ctx.exclusive_lock == nullptr ||
           image_ctx.exclusive_lock->is_lock_owner());

    {
      RWLock::RLocker snap_locker(image_ctx.snap_lock);
      if (image_ctx.object_map != nullptr &&
          !image_ctx.object_map->object_may_exist(m_object_no)) {
        return 1;
      }
    }

    string oid = image_ctx.get_object_name(m_object_no);
    ldout(image_ctx.cct, 10) << "removing " << oid << dendl;

    librados::AioCompletion *rados_completion =
      util::create_rados_safe_callback(this);
    int r = image_ctx.data_ctx.aio_remove(oid, rados_completion);
    assert(r == 0);
    rados_completion->release();
    return 0;
  }

private:
  uint64_t m_object_no;
};

template <typename I>
TrimRequest<I>::TrimRequest(I &image_ctx, Context *on_finish,
                            uint64_t original_size, uint64_t new_size,
                            ProgressContext &prog_ctx)
  : AsyncRequest<I>(image_ctx, on_finish), m_new_size(new_size),
    m_prog_ctx(prog_ctx)
{
  uint64_t period = image_ctx.get_stripe_period();
  uint64_t new_num_periods = ((m_new_size + period - 1) / period);
  m_delete_off = MIN(new_num_periods * period, original_size);
  // first object we can delete free and clear
  m_delete_start = new_num_periods * image_ctx.get_stripe_count();
  m_num_objects = Striper::get_num_objects(image_ctx.layout, original_size);

  CephContext *cct = image_ctx.cct;
  ldout(cct, 10) << this << " trim image " << original_size << " -> "
		 << m_new_size << " periods " << new_num_periods
                 << " discard to offset " << m_delete_off
                 << " delete objects " << m_delete_start
                 << " to " << m_num_objects << dendl;
}

template <typename I>
bool TrimRequest<I>::should_complete(int r)
{
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 5) << this << " should_complete: r=" << r << dendl;
  if (r == -ERESTART) {
    ldout(cct, 5) << "trim operation interrupted" << dendl;
    return true;
  } else if (r < 0) {
    lderr(cct) << "trim encountered an error: " << cpp_strerror(r) << dendl;
    return true;
  }

  RWLock::RLocker owner_lock(image_ctx.owner_lock);
  switch (m_state) {
  case STATE_COPYUP_OBJECTS:
    ldout(cct, 5) << " COPYUP_OBJECTS" << dendl;
    send_pre_remove();
    break;

  case STATE_PRE_REMOVE:
    ldout(cct, 5) << " PRE_REMOVE" << dendl;
    send_remove_objects();
    break;

  case STATE_REMOVE_OBJECTS:
    ldout(cct, 5) << " REMOVE_OBJECTS" << dendl;
    send_post_remove();
    break;

  case STATE_POST_REMOVE:
    ldout(cct, 5) << " POST_OBJECTS" << dendl;
    send_clean_boundary();
    break;

  case STATE_CLEAN_BOUNDARY:
    ldout(cct, 5) << "CLEAN_BOUNDARY" << dendl;
    send_finish(0);
    break;

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

template <typename I>
void TrimRequest<I>::send() {
  send_copyup_objects();
}

template <typename I>
void TrimRequest<I>::send_copyup_objects() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());
  assert(image_ctx.exclusive_lock == nullptr ||
         image_ctx.exclusive_lock->is_lock_owner());

  if (m_delete_start >= m_num_objects) {
    send_clean_boundary();
    return;
  }

  ::SnapContext snapc;
  bool has_snapshots;
  uint64_t parent_overlap;
  {
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    RWLock::RLocker parent_locker(image_ctx.parent_lock);

    snapc = image_ctx.snapc;
    has_snapshots = !image_ctx.snaps.empty();
    int r = image_ctx.get_parent_overlap(CEPH_NOSNAP, &parent_overlap);
    assert(r == 0);
  }

  // copyup is only required for portion of image that overlaps parent
  uint64_t copyup_end = Striper::get_num_objects(image_ctx.layout,
                                                 parent_overlap);
  // TODO: protect against concurrent shrink and snap create?
  if (copyup_end <= m_delete_start || !has_snapshots) {
    send_pre_remove();
    return;
  }

  uint64_t copyup_start = m_delete_start;
  m_delete_start = copyup_end;

  ldout(image_ctx.cct, 5) << this << " send_copyup_objects: "
			    << " start object=" << copyup_start << ", "
			    << " end object=" << copyup_end << dendl;
  m_state = STATE_COPYUP_OBJECTS;

  Context *ctx = this->create_callback_context();
  typename AsyncObjectThrottle<I>::ContextFactory context_factory(
    boost::lambda::bind(boost::lambda::new_ptr<C_CopyupObject<I> >(),
      boost::lambda::_1, &image_ctx, snapc, boost::lambda::_2));
  AsyncObjectThrottle<I> *throttle = new AsyncObjectThrottle<I>(
    this, image_ctx, context_factory, ctx, &m_prog_ctx, copyup_start,
    copyup_end);
  throttle->start_ops(image_ctx.concurrent_management_ops);
}

template <typename I>
void TrimRequest<I>::send_remove_objects() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  ldout(image_ctx.cct, 5) << this << " send_remove_objects: "
			    << " delete_start=" << m_delete_start
			    << " num_objects=" << m_num_objects << dendl;
  m_state = STATE_REMOVE_OBJECTS;

  Context *ctx = this->create_callback_context();
  typename AsyncObjectThrottle<I>::ContextFactory context_factory(
    boost::lambda::bind(boost::lambda::new_ptr<C_RemoveObject<I> >(),
      boost::lambda::_1, &image_ctx, boost::lambda::_2));
  AsyncObjectThrottle<I> *throttle = new AsyncObjectThrottle<I>(
    this, image_ctx, context_factory, ctx, &m_prog_ctx, m_delete_start,
    m_num_objects);
  throttle->start_ops(image_ctx.concurrent_management_ops);
}

template <typename I>
void TrimRequest<I>::send_pre_remove() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());
  if (m_delete_start >= m_num_objects) {
    send_clean_boundary();
    return;
  }

  bool remove_objects = false;
  {
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    if (image_ctx.object_map == nullptr) {
      remove_objects = true;
    } else {
      ldout(image_ctx.cct, 5) << this << " send_pre_remove: "
				<< " delete_start=" << m_delete_start
				<< " num_objects=" << m_num_objects << dendl;
      m_state = STATE_PRE_REMOVE;

      assert(image_ctx.exclusive_lock->is_lock_owner());

      // flag the objects as pending deletion
      Context *ctx = this->create_callback_context();
      RWLock::WLocker object_map_locker(image_ctx.object_map_lock);
      if (!image_ctx.object_map->aio_update(m_delete_start, m_num_objects,
					    OBJECT_PENDING, OBJECT_EXISTS,
                                            ctx)) {
        delete ctx;
        remove_objects = true;
      }
    }
  }

  // avoid possible recursive lock attempts
  if (remove_objects) {
    // no object map update required
    send_remove_objects();
  }
}

template <typename I>
void TrimRequest<I>::send_post_remove() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  bool clean_boundary = false;
  {
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    if (image_ctx.object_map == nullptr) {
      clean_boundary = true;
    } else {
      ldout(image_ctx.cct, 5) << this << " send_post_remove: "
          		        << " delete_start=" << m_delete_start
          		        << " num_objects=" << m_num_objects << dendl;
      m_state = STATE_POST_REMOVE;

      assert(image_ctx.exclusive_lock->is_lock_owner());

      // flag the pending objects as removed
      Context *ctx = this->create_callback_context();
      RWLock::WLocker object_map_locker(image_ctx.object_map_lock);
      if (!image_ctx.object_map->aio_update(m_delete_start, m_num_objects,
					    OBJECT_NONEXISTENT,
					    OBJECT_PENDING, ctx)) {
        delete ctx;
	clean_boundary = true;
      }
    }
  }

  // avoid possible recursive lock attempts
  if (clean_boundary) {
    // no object map update required
    send_clean_boundary();
  }
}

template <typename I>
void TrimRequest<I>::send_clean_boundary() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());
  CephContext *cct = image_ctx.cct;
  if (m_delete_off <= m_new_size) {
    send_finish(0);
    return;
  }

  // should have been canceled prior to releasing lock
  assert(image_ctx.exclusive_lock == nullptr ||
         image_ctx.exclusive_lock->is_lock_owner());
  uint64_t delete_len = m_delete_off - m_new_size;
  ldout(image_ctx.cct, 5) << this << " send_clean_boundary: "
			    << " delete_off=" << m_delete_off
			    << " length=" << delete_len << dendl;
  m_state = STATE_CLEAN_BOUNDARY;

  ::SnapContext snapc;
  {
    RWLock::RLocker snap_locker(image_ctx.snap_lock);
    snapc = image_ctx.snapc;
  }

  // discard the weird boundary
  std::vector<ObjectExtent> extents;
  Striper::file_to_extents(cct, image_ctx.format_string,
			   &image_ctx.layout, m_new_size, delete_len, 0,
                           extents);

  ContextCompletion *completion =
    new ContextCompletion(this->create_async_callback_context(), true);
  for (vector<ObjectExtent>::iterator p = extents.begin();
       p != extents.end(); ++p) {
    ldout(cct, 20) << " ex " << *p << dendl;
    Context *req_comp = new C_ContextCompletion(*completion);

    AioObjectRequest<> *req;
    if (p->offset == 0) {
      req = new AioObjectTrim(&image_ctx, p->oid.name, p->objectno, snapc,
                              req_comp);
    } else {
      req = new AioObjectTruncate(&image_ctx, p->oid.name, p->objectno,
                                  p->offset, snapc, req_comp);
    }
    req->send();
  }
  completion->finish_adding_requests();
}

template <typename I>
void TrimRequest<I>::send_finish(int r) {
  m_state = STATE_FINISHED;
  this->async_complete(r);
}

} // namespace operation
} // namespace librbd

template class librbd::operation::TrimRequest<librbd::ImageCtx>;
