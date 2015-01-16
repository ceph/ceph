// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/Mutex.h"
#include "common/RWLock.h"

#include "librbd/AioCompletion.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"

#include "librbd/AioRequest.h"
#include "librbd/CopyupRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AioRequest: "

namespace librbd {

  AioRequest::AioRequest() :
    m_ictx(NULL), m_ioctx(NULL),
    m_object_no(0), m_object_off(0), m_object_len(0),
    m_snap_id(CEPH_NOSNAP), m_completion(NULL), m_parent_completion(NULL),
    m_hide_enoent(false) {}
  AioRequest::AioRequest(ImageCtx *ictx, const std::string &oid,
			 uint64_t objectno, uint64_t off, uint64_t len,
			 const ::SnapContext &snapc, librados::snap_t snap_id,
			 Context *completion,
			 bool hide_enoent) :
    m_ictx(ictx), m_ioctx(&ictx->data_ctx), m_oid(oid), m_object_no(objectno),
    m_object_off(off), m_object_len(len), m_snap_id(snap_id),
    m_completion(completion), m_parent_completion(NULL),
    m_hide_enoent(hide_enoent) {
    for (std::vector<snapid_t>::const_iterator it = snapc.snaps.begin();
         it != snapc.snaps.end(); ++it)
      m_snaps.push_back(it->val);
  }

  AioRequest::~AioRequest() {
    if (m_parent_completion) {
      m_parent_completion->release();
      m_parent_completion = NULL;
    }
  }

  void AioRequest::read_from_parent(vector<pair<uint64_t,uint64_t> >& image_extents)
  {
    assert(!m_parent_completion);
    m_parent_completion = aio_create_completion_internal(this, rbd_req_cb);
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
        uint64_t object_overlap = m_ictx->prune_parent_extents(image_extents, image_overlap);
        if (object_overlap) {
          m_tried_parent = true;

          if (is_copy_on_read(m_ictx, m_snap_id)) {
            // If there is a valid object being coyping up, directly extract
            // content and finish up.
            Mutex::Locker l3(m_ictx->copyup_list_lock);

            map<uint64_t, CopyupRequest*>::iterator it =
              m_ictx->copyup_list.find(m_object_no);
            if (it != m_ictx->copyup_list.end()) {
              Mutex::Locker l4(it->second->get_lock());
              if (it->second->is_ready()) {
                ceph::bufferlist &copyup_data = it->second->get_copyup_data();
                m_read_data.substr_of(copyup_data, m_object_off, m_object_len);
                ldout(m_ictx->cct, 20) << __func__ << " extract content from copyup_list, obj-"
                                       << m_object_no << dendl;
                finished = true;
                break;
              }
            }
          }

          read_from_parent(image_extents);

          if (is_copy_on_read(m_ictx, m_snap_id))
            m_state = LIBRBD_AIO_READ_COPYUP; 
          else 
            m_state = LIBRBD_AIO_READ_GUARD;

          finished = false;
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
        m_ictx->copyup_list_lock.Lock();
        map<uint64_t, CopyupRequest*>::iterator it =
          m_ictx->copyup_list.find(m_object_no);
        if (it == m_ictx->copyup_list.end()) {
          RWLock::RLocker l(m_ictx->snap_lock);
          RWLock::RLocker l2(m_ictx->parent_lock);

          if (m_ictx->parent == NULL) {
            ldout(m_ictx->cct, 20) << "parent is gone; do nothing" << dendl;
            m_state = LIBRBD_AIO_READ_FLAT;
            finished = true;
            break;
          }

          // If parent still exists, overlap might also have changed.
          uint64_t newlen = m_ictx->prune_parent_extents(
            m_image_extents, m_ictx->parent_md.overlap);
          if (newlen != 0) {
            // create and kick off a CopyupRequest
            CopyupRequest *new_req = new CopyupRequest(m_ictx, m_oid,
                                                       m_object_no, true);
            m_ictx->copyup_list[m_object_no] = new_req;
            m_ictx->copyup_list_lock.Unlock();

            new_req->read_from_parent(m_image_extents);
          }
        } else {
          m_ictx->copyup_list_lock.Unlock();
        }

        finished = true;
      }

      if (r < 0) {
        ldout(m_ictx->cct, 20) << "error checking for object existence" << dendl;
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

  int AioRead::send() {
    ldout(m_ictx->cct, 20) << "send " << this << " " << m_oid << " " << m_object_off << "~" << m_object_len << dendl;

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

    r = m_ioctx->aio_operate(m_oid, rados_completion, &op, flags, NULL);

    rados_completion->release();
    return r;
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
      m_state(LIBRBD_AIO_WRITE_FLAT), m_snap_seq(snapc.seq.val), m_entire_object(NULL)
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
	uint64_t newlen = m_ictx->prune_parent_extents(
	  m_object_image_extents, m_ictx->parent_md.overlap);

	// copyup the entire object up to the overlap point, if any
	if (newlen != 0) {
	  ldout(m_ictx->cct, 20) << "should_complete(" << this << ") overlap "
				 << m_ictx->parent_md.overlap << " newlen "
				 << newlen << " image_extents"
				 << m_object_image_extents << dendl;

	  m_state = LIBRBD_AIO_WRITE_COPYUP;

          if (is_copy_on_read(m_ictx, m_snap_id)) {
            m_ictx->copyup_list_lock.Lock();
            it = m_ictx->copyup_list.find(m_object_no);
            if (it == m_ictx->copyup_list.end()) {
              // If it is not in the list, create a CopyupRequest and wait for it.
              CopyupRequest *new_req = new CopyupRequest(m_ictx, m_oid,
                                                         m_object_no, false);
              // make sure to wait on this CopyupRequest
              new_req->append_request(this);
              m_ictx->copyup_list[m_object_no] = new_req;
              m_ictx->copyup_list_lock.Unlock();

              m_entire_object = &(new_req->get_copyup_data());
              ldout(m_ictx->cct, 20) << __func__ << " creating new Copyup for AioWrite, obj-"
                                     << m_object_no << dendl;
              new_req->read_from_parent(m_object_image_extents);
              ldout(m_ictx->cct, 20) << __func__ << " issuing read_from_parent" << dendl;
            } else {
              Mutex::Locker l3(it->second->get_lock());
              if (it->second->is_ready()) {
                ldout(m_ictx->cct, 20) << __func__ << " extracting contect from copyup_list, obj-"
                                       << m_object_no << " length=" << it->second->get_copyup_data().length()
                                       << dendl;
                m_read_data.append(it->second->get_copyup_data());
                ldout(m_ictx->cct, 20) << __func__ << " extracted contect from copyup_list, obj-"
                                       << m_object_no << " length=" << m_read_data.length()
                                       << dendl;
                m_ictx->copyup_list_lock.Unlock();
                return should_complete(1);
              } else {
                ldout(m_ictx->cct, 20) << __func__ << " someone is reading back from parent" << dendl;
                it->second->append_request(this);
                m_entire_object = &(m_ictx->copyup_list[m_object_no]->get_copyup_data());
                m_ictx->copyup_list_lock.Unlock();
              }
            }
          } else {
            read_from_parent(m_object_image_extents);
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
      }
      if (r < 0) {
	ldout(m_ictx->cct, 20) << "error checking for object existence" << dendl;
	break;
      }
      break;

    case LIBRBD_AIO_WRITE_COPYUP:
      ldout(m_ictx->cct, 20) << "WRITE_COPYUP" << dendl;
      m_state = LIBRBD_AIO_WRITE_GUARD;
      if (r < 0)
	return should_complete(r);

      // Read data from waiting list safely. If this AioWrite created a
      // CopyupRequest, m_read_data should be empty.
      {
        RWLock::RLocker l3(m_ictx->snap_lock);
        if (is_copy_on_read(m_ictx, m_snap_id) &&
            m_read_data.is_zero()) {
          Mutex::Locker l(m_ictx->copyup_list_lock);
          ldout(m_ictx->cct, 20) << __func__ << " releasing self pending, obj-"
                                 << m_object_no << dendl;
          it = m_ictx->copyup_list.find(m_object_no);
          assert(it != m_ictx->copyup_list.end());
          assert(it->second->is_ready());
          assert(m_entire_object);
          m_read_data.append(*m_entire_object);
        }
      }

      send_copyup();
      finished = false;
      break;

    case LIBRBD_AIO_WRITE_FLAT:
      ldout(m_ictx->cct, 20) << "WRITE_FLAT" << dendl;
      // nothing to do
      break;

    default:
      lderr(m_ictx->cct) << "invalid request state: " << m_state << dendl;
      assert(0);
    }

    return finished;
  }

  int AbstractWrite::send() {
    ldout(m_ictx->cct, 20) << "send " << this << " " << m_oid << " " << m_object_off << "~" << m_object_len << dendl;
    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(this, NULL, rados_req_cb);
    int r;
    assert(m_write.size());
    r = m_ioctx->aio_operate(m_oid, rados_completion, &m_write,
			     m_snap_seq, m_snaps);
    rados_completion->release();
    return r;
  }

  void AbstractWrite::send_copyup() {
    ldout(m_ictx->cct, 20) << "send_copyup " << this << " " << m_oid << " " << m_object_off << "~" << m_object_len << dendl;
    if (!m_read_data.is_zero())
      m_copyup.exec("rbd", "copyup", m_read_data);
    add_copyup_ops();

    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(this, NULL, rados_req_cb);
    m_ictx->md_ctx.aio_operate(m_oid, rados_completion, &m_copyup,
			       m_snap_seq, m_snaps);
    rados_completion->release();
  }

  void AioWrite::add_write_ops(librados::ObjectWriteOperation &wr) {
    wr.set_alloc_hint(m_ictx->get_object_size(), m_ictx->get_object_size());
    wr.write(m_object_off, m_write_data);
  }
}
