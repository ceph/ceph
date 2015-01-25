// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Mutex.h"

#include "librbd/AioCompletion.h"
#include "librbd/ImageCtx.h"

#include "librbd/AioRequest.h"
#include "librbd/CopyupRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::CopyupRequest: "

namespace librbd {

  CopyupRequest::CopyupRequest(ImageCtx *ictx, const std::string &oid,
                               uint64_t objectno,
			       vector<pair<uint64_t,uint64_t> >& image_extents)
    : m_ictx(ictx), m_oid(oid), m_object_no(objectno),
      m_image_extents(image_extents)
  {
  }

  CopyupRequest::~CopyupRequest() {
    assert(m_ictx->copyup_list_lock.is_locked());
    assert(m_pending_requests.empty());

    map<uint64_t, CopyupRequest*>::iterator it =
      m_ictx->copyup_list.find(m_object_no);
    assert(it != m_ictx->copyup_list.end());
    m_ictx->copyup_list.erase(it);

    if (m_ictx->copyup_list.empty()) {
      m_ictx->copyup_list_cond.Signal();
    }
  }

  ceph::bufferlist& CopyupRequest::get_copyup_data() {
    return m_copyup_data;
  }

  void CopyupRequest::append_request(AioRequest *req) {
    ldout(m_ictx->cct, 20) << __func__ << " " << this << ": " << req << dendl;
    m_pending_requests.push_back(req);
  }

  void CopyupRequest::complete_all(int r) {
    while (!m_pending_requests.empty()) {
      vector<AioRequest *>::iterator it = m_pending_requests.begin();
      AioRequest *req = *it;
      ldout(m_ictx->cct, 20) << __func__ << " completing request " << req
			     << dendl;
      req->complete(r);
      m_pending_requests.erase(it);
    }
  }

  void CopyupRequest::send_copyup(int r) {
    ldout(m_ictx->cct, 20) << __func__ << " " << this
			   << ": oid " << m_oid
			   << ", r " << r << dendl;

    m_ictx->snap_lock.get_read();
    ::SnapContext snapc = m_ictx->snapc;
    m_ictx->snap_lock.put_read();

    std::vector<librados::snap_t> snaps;
    snaps.insert(snaps.end(), snapc.snaps.begin(), snapc.snaps.end());

    r = m_ictx->update_object_map(m_object_no, OBJECT_EXISTS);
    if (r < 0) {
      lderr(m_ictx->cct) << __func__ << " " << this
		         << ": failed to update object map:"
			 << cpp_strerror(r) << dendl;
      return;
    }

    librados::ObjectWriteOperation copyup_op;
    copyup_op.exec("rbd", "copyup", m_copyup_data);

    librados::AioCompletion *comp =
      librados::Rados::aio_create_completion(NULL, NULL, NULL);
    m_ictx->md_ctx.aio_operate(m_oid, comp, &copyup_op, snapc.seq.val, snaps);
    comp->release();
  }

  void CopyupRequest::read_from_parent()
  {
    AioCompletion *comp = aio_create_completion_internal(
      this, &CopyupRequest::read_from_parent_cb);
    ldout(m_ictx->cct, 20) << __func__ << " " << this
                           << ": completion " << comp
			   << ", oid " << m_oid
                           << ", extents " << m_image_extents
                           << dendl;

    int r = aio_read(m_ictx->parent, m_image_extents, NULL, &m_copyup_data,
		     comp, 0);
    if (r < 0) {
      comp->release();
      delete this;
    }
  }

  void CopyupRequest::queue_read_from_parent()
  {
    // TODO: once the ObjectCacher allows reentrant read requests, the finisher
    // should be eliminated
    ldout(m_ictx->cct, 20) << __func__ << " " << this
			   << ": oid " << m_oid << " "
			   << ", extents " << m_image_extents << dendl;
    C_ReadFromParent *ctx = new C_ReadFromParent(this);
    m_ictx->copyup_finisher->queue(ctx);
  }

  void CopyupRequest::read_from_parent_cb(completion_t cb, void *arg)
  {
    CopyupRequest *req = reinterpret_cast<CopyupRequest *>(arg);
    AioCompletion *comp = reinterpret_cast<AioCompletion *>(cb);

    ldout(req->m_ictx->cct, 20) << __func__ << " " << req
				<< ": oid " << req->m_oid
			        << ", extents " << req->m_image_extents << dendl;

    // If this entry is created by a read request, then copyup operation will
    // be performed asynchronously. Perform cleaning up from copyup callback.
    // If this entry is created by a write request, then copyup operation will
    // be performed synchronously by AioWrite. After extracting data, perform
    // cleaning up here
    Mutex::Locker l(req->m_ictx->copyup_list_lock);
    if (req->m_pending_requests.empty()) {
      req->send_copyup(comp->get_return_value());
    } else {
      req->complete_all(comp->get_return_value());
    }
    delete req;
  }
}
