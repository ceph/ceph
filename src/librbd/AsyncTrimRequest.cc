// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "librbd/AsyncTrimRequest.h"
#include "librbd/AsyncObjectThrottle.h"
#include "librbd/AioRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
#include "librbd/ObjectMap.h"
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
#define dout_prefix *_dout << "librbd::AsyncTrimRequest: "

namespace librbd
{

class AsyncTrimObjectContext : public C_AsyncObjectThrottle {
public:
  AsyncTrimObjectContext(AsyncObjectThrottle &throttle, ImageCtx *image_ctx,
			 uint64_t object_no)
    : C_AsyncObjectThrottle(throttle), m_image_ctx(*image_ctx),
      m_object_no(object_no)
  {
  }

  virtual int send() {
    if (!m_image_ctx.object_map.object_may_exist(m_object_no)) {
      return 1;
    }

    RWLock::RLocker l(m_image_ctx.owner_lock);
    if (m_image_ctx.image_watcher->is_lock_supported() &&
        !m_image_ctx.image_watcher->is_lock_owner()) {
      return -ERESTART;
    }

    string oid = m_image_ctx.get_object_name(m_object_no);
    ldout(m_image_ctx.cct, 10) << "removing " << oid << dendl;

    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(this, NULL, rados_ctx_cb);
    int r = m_image_ctx.data_ctx.aio_remove(oid, rados_completion);
    assert(r == 0);
    rados_completion->release();
    return 0;
  }

private:
  ImageCtx &m_image_ctx;
  uint64_t m_object_no;
};

AsyncTrimRequest::AsyncTrimRequest(ImageCtx &image_ctx, Context *on_finish,
				   uint64_t original_size, uint64_t new_size,
				   ProgressContext &prog_ctx)
  : AsyncRequest(image_ctx, on_finish), m_new_size(new_size), m_prog_ctx(prog_ctx)
{
  uint64_t period = m_image_ctx.get_stripe_period();
  uint64_t new_num_periods = ((m_new_size + period - 1) / period);
  m_delete_off = MIN(new_num_periods * period, original_size);
  // first object we can delete free and clear
  m_delete_start = new_num_periods * m_image_ctx.get_stripe_count();
  m_num_objects = Striper::get_num_objects(m_image_ctx.layout, original_size);

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " trim image " << original_size << " -> "
		 << m_new_size << " periods " << new_num_periods
                 << " discard to offset " << m_delete_off
                 << " delete objects " << m_delete_start
                 << " to " << m_num_objects << dendl;
}


bool AsyncTrimRequest::should_complete(int r)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " should_complete: r=" << r << dendl;
  if (r < 0) {
    lderr(cct) << "trim encountered an error: " << cpp_strerror(r) << dendl;
    return true;
  }

  switch (m_state) {
  case STATE_PRE_REMOVE:
    ldout(cct, 5) << " PRE_REMOVE" << dendl;
    send_remove_objects();
    break; 

  case STATE_REMOVE_OBJECTS:
    ldout(cct, 5) << " REMOVE_OBJECTS" << dendl;
    if (send_post_remove()) {
      return true;
    }
    break;

  case STATE_POST_REMOVE:
    ldout(cct, 5) << " POST_OBJECTS" << dendl;
    if (send_clean_boundary()) {
      return true;
    }
    break;

  case STATE_CLEAN_BOUNDARY:
    ldout(cct, 5) << "CLEAN_BOUNDARY" << dendl;
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

void AsyncTrimRequest::send() {
  if (m_delete_start < m_num_objects) {
    send_pre_remove();
  } else {
    bool finished = send_clean_boundary();
    if (finished) {
      m_state = STATE_FINISHED;
      complete(0);
    }
  }
}

void AsyncTrimRequest::send_remove_objects() {
  CephContext *cct = m_image_ctx.cct;
  ldout(m_image_ctx.cct, 5) << this << " send_remove_objects: "
			    << " delete_start=" << m_delete_start
			    << " num_objects=" << m_num_objects << dendl;
  m_state = STATE_REMOVE_OBJECTS;

  Context *ctx = create_callback_context();
  AsyncObjectThrottle::ContextFactory context_factory(
    boost::lambda::bind(boost::lambda::new_ptr<AsyncTrimObjectContext>(),
      boost::lambda::_1, &m_image_ctx, boost::lambda::_2));
  AsyncObjectThrottle *throttle = new AsyncObjectThrottle(
    *this, context_factory, ctx, m_prog_ctx, m_delete_start, m_num_objects);
  throttle->start_ops(cct->_conf->rbd_concurrent_management_ops);
}

void AsyncTrimRequest::send_pre_remove() {
  bool remove_objects = false;
  bool lost_exclusive_lock = false;
  {
    RWLock::RLocker l(m_image_ctx.owner_lock);
    if (!m_image_ctx.object_map.enabled()) {
      remove_objects = true;
    } else {
      ldout(m_image_ctx.cct, 5) << this << " send_pre_remove: "
				<< " delete_start=" << m_delete_start
				<< " num_objects=" << m_num_objects << dendl;
      m_state = STATE_PRE_REMOVE;

      if (!m_image_ctx.image_watcher->is_lock_owner()) {
        ldout(m_image_ctx.cct, 1) << "lost exclusive lock during trim" << dendl;
        lost_exclusive_lock = true;
      } else {
        // flag the objects as pending deletion
        Context *ctx = create_callback_context();
        if (!m_image_ctx.object_map.aio_update(m_delete_start, m_num_objects,
					       OBJECT_PENDING, OBJECT_EXISTS,
                                               ctx)) {
          delete ctx;
          remove_objects = true;
        }
      }
    }
  }

  // avoid possible recursive lock attempts
  if (remove_objects) {
    // no object map update required
    send_remove_objects();
  } else if (lost_exclusive_lock) {
    complete(-ERESTART);
  }
}

bool AsyncTrimRequest::send_post_remove() {
  bool clean_boundary = false;
  bool lost_exclusive_lock = false;
  {
    RWLock::RLocker l(m_image_ctx.owner_lock);
    if (!m_image_ctx.object_map.enabled()) {
      clean_boundary = true;
    } else {
      ldout(m_image_ctx.cct, 5) << this << " send_post_remove: "
          		        << " delete_start=" << m_delete_start
          		        << " num_objects=" << m_num_objects << dendl;
      m_state = STATE_POST_REMOVE;

      if (!m_image_ctx.image_watcher->is_lock_owner()) {
        ldout(m_image_ctx.cct, 1) << "lost exclusive lock during trim" << dendl;
      } else {
        // flag the pending objects as removed
        Context *ctx = create_callback_context();
        if (!m_image_ctx.object_map.aio_update(m_delete_start, m_num_objects,
					       OBJECT_NONEXISTENT,
					       OBJECT_PENDING, ctx)) {
          delete ctx;
	  clean_boundary = true;
	}
      }
    }
  }

  // avoid possible recursive lock attempts
  if (clean_boundary) {
    // no object map update required
    return send_clean_boundary();
  } else if (lost_exclusive_lock) {
    complete(-ERESTART);
  }
  return false;
}

bool AsyncTrimRequest::send_clean_boundary() {
  CephContext *cct = m_image_ctx.cct;
  if (m_delete_off <= m_new_size) {
    return true;
  }

  bool lost_exclusive_lock = false;
  ContextCompletion *completion = NULL;
  {
    ldout(m_image_ctx.cct, 5) << this << " send_clean_boundary: "
			      << " delete_start=" << m_delete_start
			      << " num_objects=" << m_num_objects << dendl;
    m_state = STATE_CLEAN_BOUNDARY;

    RWLock::RLocker l(m_image_ctx.owner_lock);
    if (m_image_ctx.image_watcher->is_lock_supported() &&
	!m_image_ctx.image_watcher->is_lock_owner()) {
      ldout(m_image_ctx.cct, 1) << "lost exclusive lock during trim" << dendl;
      lost_exclusive_lock = true;
    } else {
      ::SnapContext snapc;
      uint64_t parent_overlap;
      {
        RWLock::RLocker l2(m_image_ctx.snap_lock);
        snapc = m_image_ctx.snapc;

        RWLock::RLocker l3(m_image_ctx.parent_lock);
        int r = m_image_ctx.get_parent_overlap(CEPH_NOSNAP, &parent_overlap);
        assert(r == 0);
      }

      // discard the weird boundary, if any
      vector<ObjectExtent> extents;
      Striper::file_to_extents(cct, m_image_ctx.format_string,
			       &m_image_ctx.layout, m_new_size,
			       m_delete_off - m_new_size, 0, extents);

      completion = new ContextCompletion(create_callback_context(), true);
      for (vector<ObjectExtent>::iterator p = extents.begin();
           p != extents.end(); ++p) {
        ldout(cct, 20) << " ex " << *p << dendl;
        Context *req_comp = new C_ContextCompletion(*completion);

        // reverse map this object extent onto the parent
        vector<pair<uint64_t,uint64_t> > objectx;
        Striper::extent_to_file(cct, &m_image_ctx.layout, p->objectno, 0,
				m_image_ctx.layout.fl_object_size, objectx);
        uint64_t object_overlap =
	  m_image_ctx.prune_parent_extents(objectx, parent_overlap);

        AbstractWrite *req;
        if (p->offset == 0) {
          req = new AioRemove(&m_image_ctx, p->oid.name, p->objectno, objectx,
                              object_overlap, snapc, CEPH_NOSNAP, req_comp);
        } else {
          req = new AioTruncate(&m_image_ctx, p->oid.name, p->objectno, p->offset,
                                objectx, object_overlap, snapc, CEPH_NOSNAP,
                                req_comp);
        }
        req->send();
      }
    }

  }

  // avoid possible recursive lock attempts
  if (lost_exclusive_lock) {
    complete(-ERESTART);
  } else if (completion != NULL) {
    completion->finish_adding_requests();
  }
  return false;
}

} // namespace librbd
