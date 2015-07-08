// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Mutex.h"
#include "common/RWLock.h"

#include "librbd/AioCompletion.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"

#include "librbd/AioRequest.h"
#include "librbd/CopyupRequest.h"

#include <boost/bind.hpp>
#include <boost/optional.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AioRequest: "

namespace librbd {

  AioRequest::AioRequest() :
    m_ictx(NULL),
    m_object_no(0), m_object_off(0), m_object_len(0),
    m_snap_id(CEPH_NOSNAP), m_completion(NULL), m_parent_completion(NULL),
    m_hide_enoent(false) {}
  AioRequest::AioRequest(ImageCtx *ictx, const std::string &oid,
			 uint64_t objectno, uint64_t off, uint64_t len,
			 const ::SnapContext &snapc, librados::snap_t snap_id,
			 Context *completion,
			 bool hide_enoent) :
    m_ictx(ictx), m_oid(oid), m_object_no(objectno),
    m_object_off(off), m_object_len(len), m_snap_id(snap_id),
    m_completion(completion), m_parent_completion(NULL),
    m_hide_enoent(hide_enoent) {
    m_snaps.insert(m_snaps.end(), snapc.snaps.begin(), snapc.snaps.end());
  }

  AioRequest::~AioRequest() {
    if (m_parent_completion) {
      m_parent_completion->release();
      m_parent_completion = NULL;
    }
  }

  void AioRequest::complete(int r)
  {
    if (should_complete(r)) {
      ldout(m_ictx->cct, 20) << "complete " << this << dendl;
      if (m_hide_enoent && r == -ENOENT) {
	r = 0;
      }
      m_completion->complete(r);
      delete this;
    }
  }

  void AioRequest::read_from_parent(vector<pair<uint64_t,uint64_t> >& image_extents,
                                    bool block_completion)
  {
    assert(!m_parent_completion);
    m_parent_completion = aio_create_completion_internal(this, rbd_req_cb);
    if (block_completion) {
      // prevent the parent image from being deleted while this
      // request is still in-progress
      m_parent_completion->get();
      m_parent_completion->block();
    }

    ldout(m_ictx->cct, 20) << "read_from_parent this = " << this
			   << " parent completion " << m_parent_completion
			   << " extents " << image_extents
			   << dendl;
    aio_read(m_ictx->parent, image_extents, NULL, &m_read_data,
             m_parent_completion, 0);
  }

  static inline bool is_copy_on_read(ImageCtx *ictx, librados::snap_t snap_id) {
    assert(ictx->snap_lock.is_locked());
    return (ictx->cct->_conf->rbd_clone_copy_on_read) &&
           (!ictx->read_only) && (snap_id == CEPH_NOSNAP);
  }

  /** read **/

  AioRead::AioRead(ImageCtx *ictx, const std::string &oid,
                   uint64_t objectno, uint64_t offset, uint64_t len,
                   vector<pair<uint64_t,uint64_t> >& be,
                   const ::SnapContext &snapc,
                   librados::snap_t snap_id, bool sparse,
                   Context *completion, int op_flags)
    : AioRequest(ictx, oid, objectno, offset, len, snapc, snap_id, completion,
		 false),
      m_buffer_extents(be), m_tried_parent(false),
      m_sparse(sparse), m_op_flags(op_flags), m_state(LIBRBD_AIO_READ_FLAT) {
    RWLock::RLocker l(m_ictx->snap_lock);
    RWLock::RLocker l2(m_ictx->parent_lock);

    Striper::extent_to_file(m_ictx->cct, &m_ictx->layout,
                            m_object_no, 0, m_ictx->layout.fl_object_size,
                            m_image_extents);

    guard_read();
  }

  void AioRead::guard_read()
  {
    assert(m_ictx->snap_lock.is_locked());

    uint64_t image_overlap = 0;
    m_ictx->get_parent_overlap(m_snap_id, &image_overlap);
    uint64_t object_overlap =
      m_ictx->prune_parent_extents(m_image_extents, image_overlap);
    if (object_overlap) {
      ldout(m_ictx->cct, 20) << __func__ << " guarding read" << dendl;
      m_state = LIBRBD_AIO_READ_GUARD;
    }
  }

  bool AioRead::should_complete(int r)
  {
    ldout(m_ictx->cct, 20) << "should_complete " << this << " " << m_oid << " " << m_object_off << "~" << m_object_len
                           << " r = " << r << dendl;

    bool finished = true;

    switch (m_state) {
    case LIBRBD_AIO_READ_GUARD:
      ldout(m_ictx->cct, 20) << "should_complete " << this
                             << " READ_CHECK_GUARD" << dendl;

      // This is the step to read from parent
      if (!m_tried_parent && r == -ENOENT) {
        {
          RWLock::RLocker l(m_ictx->snap_lock);
          RWLock::RLocker l2(m_ictx->parent_lock);
          if (m_ictx->parent == NULL) {
	    ldout(m_ictx->cct, 20) << "parent is gone; do nothing" << dendl;
	    m_state = LIBRBD_AIO_READ_FLAT;
	    finished = false;
	    break;
	  }

          // calculate reverse mapping onto the image
          vector<pair<uint64_t,uint64_t> > image_extents;
          Striper::extent_to_file(m_ictx->cct, &m_ictx->layout,
			          m_object_no, m_object_off, m_object_len,
			          image_extents);

          uint64_t image_overlap = 0;
          r = m_ictx->get_parent_overlap(m_snap_id, &image_overlap);
          if (r < 0) {
            assert(0 == "FIXME");
          }
          uint64_t object_overlap = m_ictx->prune_parent_extents(image_extents,
                                                                 image_overlap);
          if (object_overlap) {
            m_tried_parent = true;
            if (is_copy_on_read(m_ictx, m_snap_id)) {
              m_state = LIBRBD_AIO_READ_COPYUP;
	    }

            read_from_parent(image_extents, true);
            finished = false;
          }
        }

        if (m_tried_parent) {
          // release reference to the parent read completion.  this request
          // might be completed after unblock is invoked.
          AioCompletion *parent_completion = m_parent_completion;
          parent_completion->unblock(m_ictx->cct);
          parent_completion->put();
        }
      }
      break;
    case LIBRBD_AIO_READ_COPYUP:
      ldout(m_ictx->cct, 20) << "should_complete " << this << " READ_COPYUP" << dendl;
      // This is the extra step for copy-on-read: kick off an asynchronous copyup.
      // It is different from copy-on-write as asynchronous copyup will finish
      // by itself so state won't go back to LIBRBD_AIO_READ_GUARD.

      assert(m_tried_parent);
      if (r > 0) {
        // If read entire object from parent success and CoR is possible, kick
        // off a asynchronous copyup. This approach minimizes the latency
        // impact.
        Mutex::Locker copyup_locker(m_ictx->copyup_list_lock);
        map<uint64_t, CopyupRequest*>::iterator it =
          m_ictx->copyup_list.find(m_object_no);
        if (it == m_ictx->copyup_list.end()) {
          RWLock::RLocker l(m_ictx->snap_lock);
          RWLock::RLocker l2(m_ictx->parent_lock);
          if (m_ictx->parent == NULL) {
            ldout(m_ictx->cct, 20) << "parent is gone; do nothing" << dendl;
            break;
          }

          // If parent still exists, overlap might also have changed.
          uint64_t parent_overlap;
          r = m_ictx->get_parent_overlap(CEPH_NOSNAP, &parent_overlap);
          assert(r == 0);

          uint64_t newlen = m_ictx->prune_parent_extents(
            m_image_extents, parent_overlap);
          if (newlen != 0) {
            // create and kick off a CopyupRequest
            CopyupRequest *new_req = new CopyupRequest(m_ictx, m_oid,
                                                       m_object_no,
						       m_image_extents);
            m_ictx->copyup_list[m_object_no] = new_req;
            new_req->queue_send();
          }
        }
      }
      break;
    case LIBRBD_AIO_READ_FLAT:
      ldout(m_ictx->cct, 20) << "should_complete " << this << " READ_FLAT" << dendl;
      // The read content should be deposit in m_read_data
      break;
    default:
      lderr(m_ictx->cct) << "invalid request state: " << m_state << dendl;
      assert(0);
    }

    return finished;
  }

  void AioRead::send() {
    ldout(m_ictx->cct, 20) << "send " << this << " " << m_oid << " "
                           << m_object_off << "~" << m_object_len << dendl;

    // send read request to parent if the object doesn't exist locally
    if (!m_ictx->object_map.object_may_exist(m_object_no)) {
      complete(-ENOENT);
      return;
    }

    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(this, rados_req_cb, NULL);
    int r;
    librados::ObjectReadOperation op;
    int flags = m_ictx->get_read_flags(m_snap_id);
    if (m_sparse) {
      op.sparse_read(m_object_off, m_object_len, &m_ext_map, &m_read_data,
		     NULL);
    } else {
      op.read(m_object_off, m_object_len, &m_read_data, NULL);
    }
    op.set_op_flags2(m_op_flags);

    r = m_ictx->data_ctx.aio_operate(m_oid, rados_completion, &op, flags, NULL);
    assert(r == 0);

    rados_completion->release();
  }

  /** write **/

  AbstractWrite::AbstractWrite()
    : m_state(LIBRBD_AIO_WRITE_FLAT),
      m_parent_overlap(0),
      m_snap_seq(0) {}
  AbstractWrite::AbstractWrite(ImageCtx *ictx, const std::string &oid,
			       uint64_t object_no, uint64_t object_off, uint64_t len,
			       vector<pair<uint64_t,uint64_t> >& objectx,
			       uint64_t object_overlap,
			       const ::SnapContext &snapc, librados::snap_t snap_id,
			       Context *completion,
			       bool hide_enoent)
    : AioRequest(ictx, oid, object_no, object_off, len, snapc, snap_id, 
                 completion, hide_enoent),
      m_state(LIBRBD_AIO_WRITE_FLAT), m_snap_seq(snapc.seq.val),
      m_entire_object(NULL)
  {
    m_object_image_extents = objectx;
    m_parent_overlap = object_overlap;
  }

  void AbstractWrite::guard_write()
  {
    if (has_parent()) {
      m_state = LIBRBD_AIO_WRITE_GUARD;
      m_write.assert_exists();
      ldout(m_ictx->cct, 20) << __func__ << " guarding write" << dendl;
    }
  }

  bool AbstractWrite::should_complete(int r)
  {
    ldout(m_ictx->cct, 20) << "write " << this << " " << m_oid << " " << m_object_off << "~" << m_object_len
			   << " should_complete: r = " << r << dendl;

    map<uint64_t, CopyupRequest*>::iterator it;
    bool finished = true;
    switch (m_state) {
    case LIBRBD_AIO_WRITE_PRE:
      ldout(m_ictx->cct, 20) << "WRITE_PRE" << dendl;
      if (r < 0) {
	return true;
      }

      send_write();
      finished = false;
      break;

    case LIBRBD_AIO_WRITE_POST:
      ldout(m_ictx->cct, 20) << "WRITE_POST" << dendl;
      finished = true;
      break;

    case LIBRBD_AIO_WRITE_GUARD:
      ldout(m_ictx->cct, 20) << "WRITE_CHECK_GUARD" << dendl;

      if (r == -ENOENT) {
	RWLock::RLocker l(m_ictx->snap_lock);
	RWLock::RLocker l2(m_ictx->parent_lock);

	/*
	 * Parent may have disappeared; if so, recover by using
	 * send_copyup() to send the original write req (the copyup
	 * operation itself will be a no-op, since someone must have
	 * populated the child object while we weren't looking).
	 * Move to WRITE_FLAT state as we'll be done with the
	 * operation once the null copyup completes.
	 */

	if (m_ictx->parent == NULL) {
	  ldout(m_ictx->cct, 20) << "parent is gone; do null copyup " << dendl;
	  m_state = LIBRBD_AIO_WRITE_FLAT;
	  send_copyup();
	  finished = false;
	  break;
	}

	// If parent still exists, overlap might also have changed.
	uint64_t parent_overlap;
        r = m_ictx->get_parent_overlap(CEPH_NOSNAP, &parent_overlap);
        assert(r == 0);

	uint64_t newlen = m_ictx->prune_parent_extents(
	  m_object_image_extents, parent_overlap);

	// copyup the entire object up to the overlap point, if any
	if (newlen != 0) {
	  ldout(m_ictx->cct, 20) << "should_complete(" << this << ") overlap "
				 << parent_overlap << " newlen "
				 << newlen << " image_extents"
				 << m_object_image_extents << dendl;

	  m_state = LIBRBD_AIO_WRITE_COPYUP;

          if (is_copy_on_read(m_ictx, m_snap_id)) {
            m_ictx->copyup_list_lock.Lock();
            it = m_ictx->copyup_list.find(m_object_no);
            if (it == m_ictx->copyup_list.end()) {
              // If it is not in the list, create a CopyupRequest and wait for it.
              CopyupRequest *new_req = new CopyupRequest(m_ictx, m_oid,
                                                         m_object_no,
							 m_object_image_extents);
              // make sure to wait on this CopyupRequest
              new_req->append_request(this);
              m_ictx->copyup_list[m_object_no] = new_req;

              m_entire_object = &(new_req->get_copyup_data());
              m_ictx->copyup_list_lock.Unlock();
              new_req->send();
            } else {
              it->second->append_request(this);
              m_entire_object = &it->second->get_copyup_data();
              m_ictx->copyup_list_lock.Unlock();
            }
          } else {
            read_from_parent(m_object_image_extents, false);
          }
	} else {
	  ldout(m_ictx->cct, 20) << "should_complete(" << this
				 << "): parent overlap now 0" << dendl;
	  m_object_image_extents.clear();
	  m_state = LIBRBD_AIO_WRITE_FLAT;
	  send_copyup();
	}
	finished = false;
	break;
      } else if (r < 0) {
        // pass the error code to the finish context
        m_state = LIBRBD_AIO_WRITE_ERROR;
        complete(r);
	finished = false;
	break;
      }

      finished = send_post();
      break;

    case LIBRBD_AIO_WRITE_COPYUP:
      ldout(m_ictx->cct, 20) << "WRITE_COPYUP" << dendl;
      m_state = LIBRBD_AIO_WRITE_GUARD;
      if (r < 0) {
	return should_complete(r);
      }

      // Read data from waiting list safely. If this AioWrite created a
      // CopyupRequest, m_read_data should be empty.
      if (m_entire_object != NULL) {
	assert(m_read_data.length() == 0);
	m_read_data.append(*m_entire_object);
      }

      send_copyup();
      finished = false;
      break;

    case LIBRBD_AIO_WRITE_FLAT:
      ldout(m_ictx->cct, 20) << "WRITE_FLAT" << dendl;

      finished = send_post();
      break;

    case LIBRBD_AIO_WRITE_ERROR:
      assert(r < 0);
      lderr(m_ictx->cct) << "WRITE_ERROR: " << cpp_strerror(r)
			 << dendl; 
      break;

    default:
      lderr(m_ictx->cct) << "invalid request state: " << m_state << dendl;
      assert(0);
    }

    return finished;
  }

  void AbstractWrite::send() {
    ldout(m_ictx->cct, 20) << "send " << this << " " << m_oid << " "
			   << m_object_off << "~" << m_object_len << dendl;

    if (!send_pre()) {
      send_write();
    }
  }

  bool AbstractWrite::send_pre() {
    bool lost_exclusive_lock = false;
    {
      RWLock::RLocker l(m_ictx->owner_lock);
      if (!m_ictx->object_map.enabled()) {
	return false;
      }

      if (!m_ictx->image_watcher->is_lock_owner()) {
	ldout(m_ictx->cct, 1) << "lost exclusive lock during write" << dendl;
	lost_exclusive_lock = true;
      } else {
	ldout(m_ictx->cct, 20) << "send_pre " << this << " " << m_oid << " "
			       << m_object_off << "~" << m_object_len << dendl;

        uint8_t new_state;
        boost::optional<uint8_t> current_state;
        pre_object_map_update(&new_state);

        m_state = LIBRBD_AIO_WRITE_PRE;
        FunctionContext *ctx = new FunctionContext(
          boost::bind(&AioRequest::complete, this, _1));
        if (!m_ictx->object_map.aio_update(m_object_no, new_state,
					    current_state, ctx)) {
	  // no object map update required
	  delete ctx;
	  return false;
	}
      }
    }

    if (lost_exclusive_lock) {
      complete(-ERESTART);
    }
    return true;
  }

  bool AbstractWrite::send_post() {
    ldout(m_ictx->cct, 20) << "send_post " << this << " " << m_oid << " "
			   << m_object_off << "~" << m_object_len << dendl;

    RWLock::RLocker l(m_ictx->owner_lock);
    if (!m_ictx->object_map.enabled() || !post_object_map_update()) {
      return true;
    }

    if (m_ictx->image_watcher->is_lock_supported() &&
        !m_ictx->image_watcher->is_lock_owner()) {
      // leave the object flagged as pending
      ldout(m_ictx->cct, 1) << "lost exclusive lock during write" << dendl;
      return true;
    }

    m_state = LIBRBD_AIO_WRITE_POST;
    FunctionContext *ctx = new FunctionContext(
      boost::bind(&AioRequest::complete, this, _1));
    if (!m_ictx->object_map.aio_update(m_object_no, OBJECT_NONEXISTENT,
					OBJECT_PENDING, ctx)) {
      // no object map update required
      delete ctx;
      return true;
    }
    return false;
  }

  void AbstractWrite::send_write() {
    ldout(m_ictx->cct, 20) << "send_write " << this << " " << m_oid << " "
			   << m_object_off << "~" << m_object_len << dendl;

    m_state = LIBRBD_AIO_WRITE_FLAT;
    guard_write();
    add_write_ops(&m_write);
    assert(m_write.size() != 0);

    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(this, NULL, rados_req_cb);
    int r = m_ictx->data_ctx.aio_operate(m_oid, rados_completion, &m_write,
					 m_snap_seq, m_snaps);
    assert(r == 0);
    rados_completion->release();
  }

  void AbstractWrite::send_copyup() {
    ldout(m_ictx->cct, 20) << "send_copyup " << this << " " << m_oid << " " << m_object_off << "~" << m_object_len << dendl;
    librados::ObjectWriteOperation op;
    if (!m_read_data.is_zero()) {
      op.exec("rbd", "copyup", m_read_data);
    }
    add_write_ops(&op);
    assert(op.size() != 0);

    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(this, NULL, rados_req_cb);
    m_ictx->md_ctx.aio_operate(m_oid, rados_completion, &op,
			       m_snap_seq, m_snaps);
    rados_completion->release();
  }

  void AioWrite::add_write_ops(librados::ObjectWriteOperation *wr) {
    wr->set_alloc_hint(m_ictx->get_object_size(), m_ictx->get_object_size());
    wr->write(m_object_off, m_write_data);
    wr->set_op_flags2(m_op_flags);
  }
}
