// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/Mutex.h"

#include "librbd/AioCompletion.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"

#include "librbd/AioRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AioRequest: "

namespace librbd {

  AioRequest::AioRequest() :
    m_ictx(NULL), m_image_ofs(0), m_block_ofs(0), m_len(0),
    m_snap_id(CEPH_NOSNAP), m_completion(NULL), m_parent_completion(NULL),
    m_hide_enoent(false) {}
  AioRequest::AioRequest(ImageCtx *ictx, const std::string &oid,
			 uint64_t image_ofs, size_t len,
			 librados::snap_t snap_id,
			 Context *completion,
			 bool hide_enoent) {
    m_ictx = ictx;
    m_ioctx.dup(ictx->data_ctx);
    m_ioctx.snap_set_read(snap_id);
    m_oid = oid;
    m_image_ofs = image_ofs;
    m_block_ofs = get_block_ofs(ictx->order, image_ofs);
    m_len = len;
    m_snap_id = snap_id;
    m_completion = completion;
    m_parent_completion = NULL;
    m_hide_enoent = hide_enoent;
  }

  AioRequest::~AioRequest() {
    if (m_parent_completion) {
      m_parent_completion->release();
      m_parent_completion = NULL;
    }
  }

  void AioRequest::read_from_parent(uint64_t image_ofs, size_t len)
  {
    ldout(m_ictx->cct, 20) << "read_from_parent this = " << this << dendl;

    assert(!m_parent_completion);
    assert(m_ictx->parent_lock.is_locked());

    m_parent_completion = aio_create_completion_internal(this, rbd_req_cb);
    aio_read(m_ictx->parent, image_ofs, len, m_read_data.c_str(),
	     m_parent_completion);
  }

  bool AioRead::should_complete(int r)
  {
    ldout(m_ictx->cct, 20) << "read should_complete: r = " << r << dendl;

    if (!m_tried_parent && r == -ENOENT) {
      Mutex::Locker l(m_ictx->snap_lock);
      Mutex::Locker l2(m_ictx->parent_lock);
      size_t len = m_ictx->parent_io_len(m_image_ofs, m_len, m_snap_id);
      if (len) {
	m_tried_parent = true;
	// zero the buffer so we have the full requested length result,
	// even if we actually read less due to overlap
	ceph::buffer::ptr bp(len);
	bp.zero();
	m_read_data.append(bp);
	// fill in single extent for sparse read callback
	m_ext_map[m_block_ofs] = len;
	read_from_parent(m_image_ofs, len);
	return false;
      }
    }

    return true;
  }

  int AioRead::send() {
    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(this, rados_req_cb, NULL);
    int r;
    if (m_sparse) {
      r = m_ioctx.aio_sparse_read(m_oid, rados_completion, &m_ext_map,
				  &m_read_data, m_len, m_block_ofs);
    } else {
      r = m_ioctx.aio_read(m_oid, rados_completion, &m_read_data,
			   m_len, m_block_ofs);
    }
    rados_completion->release();
    return r;
  }

  AbstractWrite::AbstractWrite() :
    m_state(LIBRBD_AIO_WRITE_FINAL), m_has_parent(false) {}
  AbstractWrite::AbstractWrite(ImageCtx *ictx, const std::string &oid,
			       uint64_t image_ofs, size_t len,
			       librados::snap_t snap_id, Context *completion,
			       bool has_parent, const ::SnapContext &snapc,
			       bool hide_enoent)
    : AioRequest(ictx, oid, image_ofs, len, snap_id, completion, hide_enoent)
  {
    m_state = LIBRBD_AIO_WRITE_FINAL;
    m_has_parent = has_parent;
    // TODO: find a way to make this less stupid
    std::vector<librados::snap_t> snaps;
    for (std::vector<snapid_t>::const_iterator it = snapc.snaps.begin();
	 it != snapc.snaps.end(); ++it) {
      snaps.push_back(it->val);
    }
    m_ioctx.selfmanaged_snap_set_write_ctx(snapc.seq.val, snaps);
  }

  void AbstractWrite::guard_write()
  {
    if (m_has_parent) {
      m_state = LIBRBD_AIO_WRITE_CHECK_EXISTS;
      m_read.stat(NULL, NULL, NULL);
    }
    ldout(m_ictx->cct, 20) << __func__ << " m_has_parent = " << m_has_parent
			   << " m_state = " << m_state << " check exists = "
			   << LIBRBD_AIO_WRITE_CHECK_EXISTS << dendl;
      
  }

  bool AbstractWrite::should_complete(int r)
  {
    ldout(m_ictx->cct, 20) << "write " << this << " should_complete: r = "
			   << r << dendl;

    bool finished = true;
    switch (m_state) {
    case LIBRBD_AIO_WRITE_CHECK_EXISTS:
      ldout(m_ictx->cct, 20) << "WRITE_CHECK_EXISTS" << dendl;
      if (r < 0 && r != -ENOENT) {
	ldout(m_ictx->cct, 20) << "error checking for object existence" << dendl;
	break;
      }
      finished = false;
      if (r == -ENOENT) {
	Mutex::Locker l(m_ictx->snap_lock);
	Mutex::Locker l2(m_ictx->parent_lock);
	// copyup the entire object up to the overlap point
	uint64_t block_begin = m_image_ofs - m_block_ofs;
	size_t len = m_ictx->parent_io_len(block_begin,
					   get_block_size(m_ictx->order),
					   m_snap_id);
	if (len) {
	  ldout(m_ictx->cct, 20) << "reading from parent" << dendl;
	  m_state = LIBRBD_AIO_WRITE_COPYUP;
	  ceph::buffer::ptr bp(len);
	  m_read_data.append(bp);
	  read_from_parent(block_begin, len);
	  break;
	}
      }
      ldout(m_ictx->cct, 20) << "no need to read from parent" << dendl;
      m_state = LIBRBD_AIO_WRITE_FINAL;
      send();
      break;
    case LIBRBD_AIO_WRITE_COPYUP:
      ldout(m_ictx->cct, 20) << "WRITE_COPYUP" << dendl;
      m_state = LIBRBD_AIO_WRITE_FINAL;
      if (r < 0)
	return should_complete(r);
      send_copyup();
      finished = false;
      break;
    case LIBRBD_AIO_WRITE_FINAL:
      ldout(m_ictx->cct, 20) << "WRITE_FINAL" << dendl;
      // nothing to do
      break;
    default:
      lderr(m_ictx->cct) << "invalid request state: " << m_state << dendl;
      assert(0);
    }

    return finished;
  }

  int AbstractWrite::send() {
    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(this, NULL, rados_req_cb);
    int r;
    if (m_state == LIBRBD_AIO_WRITE_CHECK_EXISTS) {
      assert(m_read.size());
      r = m_ioctx.aio_operate(m_oid, rados_completion, &m_read, &m_read_data);
    } else {
      assert(m_write.size());
      r = m_ioctx.aio_operate(m_oid, rados_completion, &m_write);
    }
    rados_completion->release();
    return r;
  }

  void AbstractWrite::send_copyup() {
    m_copyup.exec("rbd", "copyup", m_read_data);
    add_copyup_ops();

    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(this, NULL, rados_req_cb);
    m_ictx->md_ctx.aio_operate(m_oid, rados_completion, &m_copyup);
    rados_completion->release();
  }
}
