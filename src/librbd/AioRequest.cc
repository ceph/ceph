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
			 librados::snap_t snap_id,
			 Context *completion,
			 bool hide_enoent) :
    m_ictx(ictx), m_ioctx(&ictx->data_ctx), m_oid(oid), m_object_no(objectno),
    m_object_off(off), m_object_len(len), m_snap_id(snap_id),
    m_completion(completion), m_parent_completion(NULL),
    m_hide_enoent(hide_enoent) {}

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
	     m_parent_completion);
  }

  /** read **/

  bool AioRead::should_complete(int r)
  {
    ldout(m_ictx->cct, 20) << "should_complete " << this << " " << m_oid << " " << m_object_off << "~" << m_object_len
			   << " r = " << r << dendl;

    if (!m_tried_parent && r == -ENOENT) {
      RWLock::RLocker l(m_ictx->snap_lock);
      RWLock::RLocker l2(m_ictx->parent_lock);

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
	read_from_parent(image_extents);
	return false;
      }
    }

    return true;
  }

  void AioRead::send() {
    ldout(m_ictx->cct, 20) << "send " << this << " " << m_oid << " "
                           << m_object_off << "~" << m_object_len << dendl;

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

    r = m_ioctx->aio_operate(m_oid, rados_completion, &op, flags, NULL);
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
    : AioRequest(ictx, oid, object_no, object_off, len, snap_id, completion,
		 hide_enoent),
      m_state(LIBRBD_AIO_WRITE_FLAT), m_snap_seq(snapc.seq.val)
  {
    m_object_image_extents = objectx;
    m_parent_overlap = object_overlap;

    // TODO: find a way to make this less stupid
    for (std::vector<snapid_t>::const_iterator it = snapc.snaps.begin();
	 it != snapc.snaps.end(); ++it) {
      m_snaps.push_back(it->val);
    }
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
	  read_from_parent(m_object_image_extents);
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

  void AbstractWrite::send() {
    ldout(m_ictx->cct, 20) << "send " << this << " " << m_oid << " "
                           << m_object_off << "~" << m_object_len << dendl;
    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(this, NULL, rados_req_cb);
    int r;
    assert(m_write.size());
    r = m_ioctx->aio_operate(m_oid, rados_completion, &m_write,
			     m_snap_seq, m_snaps);
    assert(r == 0);
    rados_completion->release();
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
