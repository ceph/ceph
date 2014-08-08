// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/Mutex.h"
#include "common/RWLock.h"

#include "include/interval_set.h"
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

    m_parent_completion->read_bl = &m_read_data; 
    m_parent_completion->get();
    m_parent_completion->init_time(m_ictx->cct, AIO_TYPE_READ);

    interval_set<uint64_t> read_subsets;
    for (vector<pair<uint64_t,uint64_t> >::const_iterator p = image_extents.begin();
         p != image_extents.end();
         ++p) {
      read_subsets.insert(p->first, p->second);
    }

    ImageCtx *parent = m_ictx->parent;
    ldout(m_ictx->cct, 20) << " read_subsets size " << read_subsets.size() << dendl;
    while (!ictx_check(parent)) {
      map<object_t,vector<ObjectExtent> > object_extents;

      // reverse map this object extent onto the parent
      uint64_t buffer_ofs = 0;
      for (interval_set<uint64_t>::const_iterator p = read_subsets.begin();
           p != read_subsets.end(); ++p) {
        ldout(m_ictx->cct, 20) << " read_subsets start " << p.get_start() << " len " << p.get_len() << dendl;
        Striper::file_to_extents(parent->cct, parent->format_string, &parent->layout,
                                 p.get_start(), p.get_len(), 0, object_extents, buffer_ofs);
        buffer_ofs += p.get_len();
      }

      for (map<object_t, vector<ObjectExtent> >::iterator p = object_extents.begin();
           p != object_extents.end(); ++p) {
        assert(!p->second.empty());
        uint64_t objectno;
        uint64_t count = 0;
        vector<pair<uint64_t, uint64_t> > file_extents;
        for (vector<ObjectExtent>::iterator q = p->second.begin(); q != p->second.end(); ++q) {
          objectno = q->objectno;
          count += q->length;
          file_extents.insert(file_extents.end(), q->file_extents.begin(), q->file_extents.end());
        }
        
        if (parent->image_index.is_parent(objectno)) {
          uint64_t image_overlap = 0;
          int r = parent->get_parent_overlap(parent->snap_id, &image_overlap);
          assert(0 == r);
          uint64_t object_overlap = parent->prune_parent_extents(file_extents, image_overlap);
          // Need read from parent again, continue loop
          if (object_overlap) {
            continue;
          }
        }

        send_aio_read(parent, p->second, m_parent_completion);
        parent->perfcounter->inc(l_librbd_aio_rd);
        parent->perfcounter->inc(l_librbd_aio_rd_bytes, count);
        for (vector<pair<uint64_t, uint64_t> >::iterator q = file_extents.begin();
             q != file_extents.end(); ++q) {
          ldout(m_ictx->cct, 0) << " read_subsets erase start " << q->first << " len " << q->second << dendl;
          read_subsets.erase(q->first, q->second);
        }
      }

      if (read_subsets.empty())
        break;
      parent = parent->parent;
    }

    m_parent_completion->finish_adding_requests(m_ictx->cct);
    m_parent_completion->put();
  }

  /** read **/

  bool AioRead::should_complete(int r)
  {
    ldout(m_ictx->cct, 20) << "should_complete " << this << " oid " << m_oid << " objectno " << m_object_no << " off " << m_object_off << "~" << m_object_len
		           << " m_tried_parent " << m_tried_parent << " r = " << r << dendl;

    if (!m_tried_parent && r == -ENOENT) {
      // Only when image index don't know the object location info, read op
      // will reach here and popluate it
      
      RWLock::RLocker l(m_ictx->snap_lock);
      RWLock::RLocker l2(m_ictx->parent_lock);

      uint64_t image_overlap = 0;
      r = m_ictx->get_parent_overlap(m_snap_id, &image_overlap);
      if (r < 0) {
	assert(0 == "FIXME");
      }
      uint64_t object_overlap = m_ictx->prune_parent_extents(m_file_extents, image_overlap);
      if (object_overlap) {
	m_tried_parent = true;
	read_from_parent(m_file_extents);
	return false;
      }
    }

    if (!m_tried_parent && r >= 0)
      m_ictx->image_index.mark_local(m_object_no);
    return true;
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
    if (!m_ictx->image_index.is_local(m_object_no)) {
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

    if (finished)
      m_ictx->image_index.mark_local(m_object_no);

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
