// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Mutex.h"

#include "librbd/AioCompletion.h"
#include "librbd/AioRequest.h"
#include "librbd/CopyupRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/ObjectMap.h"

#include <boost/bind.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::CopyupRequest: "

namespace librbd {

  CopyupRequest::CopyupRequest(ImageCtx *ictx, const std::string &oid,
                               uint64_t objectno,
			       vector<pair<uint64_t,uint64_t> >& image_extents)
    : m_ictx(ictx), m_oid(oid), m_object_no(objectno),
      m_image_extents(image_extents), m_state(STATE_READ_FROM_PARENT)
  {
    m_async_op.start_op(*m_ictx);
  }

  CopyupRequest::~CopyupRequest() {
    assert(m_pending_requests.empty());
    m_async_op.finish_op();
  }

  void CopyupRequest::append_request(AioRequest *req) {
    ldout(m_ictx->cct, 20) << __func__ << " " << this << ": " << req << dendl;
    m_pending_requests.push_back(req);
  }

  void CopyupRequest::complete_requests(int r) {
    while (!m_pending_requests.empty()) {
      vector<AioRequest *>::iterator it = m_pending_requests.begin();
      AioRequest *req = *it;
      ldout(m_ictx->cct, 20) << __func__ << " completing request " << req
			     << dendl;
      req->complete(r);
      m_pending_requests.erase(it);
    }
  }

  bool CopyupRequest::send_copyup() {
    m_ictx->snap_lock.get_read();
    ::SnapContext snapc = m_ictx->snapc;
    m_ictx->snap_lock.put_read();

    std::vector<librados::snap_t> snaps;
    snaps.insert(snaps.end(), snapc.snaps.begin(), snapc.snaps.end());

    librados::ObjectWriteOperation copyup_op;
    if (!m_copyup_data.is_zero()) {
      copyup_op.exec("rbd", "copyup", m_copyup_data);
    }

    // merge all pending write ops into this single RADOS op
    for (size_t i=0; i<m_pending_requests.size(); ++i) {
      AioRequest *req = m_pending_requests[i];
      ldout(m_ictx->cct, 20) << __func__ << " add_copyup_ops " << req << dendl;
      req->add_copyup_ops(&copyup_op);
    }

    if (copyup_op.size() == 0) {
      return true;
    }

    ldout(m_ictx->cct, 20) << __func__ << " " << this
			   << ": oid " << m_oid << dendl;
    m_state = STATE_COPYUP;

    librados::AioCompletion *comp =
      librados::Rados::aio_create_completion(create_callback_context(), NULL,
                                             rados_ctx_cb);
    int r = m_ictx->md_ctx.aio_operate(m_oid, comp, &copyup_op, snapc.seq.val,
                                       snaps);
    assert(r == 0);
    comp->release();
    return false;
  }

  void CopyupRequest::send()
  {
    m_state = STATE_READ_FROM_PARENT;
    AioCompletion *comp = aio_create_completion_internal(
      create_callback_context(), rbd_ctx_cb);

    ldout(m_ictx->cct, 20) << __func__ << " " << this
                           << ": completion " << comp
			   << ", oid " << m_oid
                           << ", extents " << m_image_extents
                           << dendl;
    aio_read(m_ictx->parent, m_image_extents, NULL, &m_copyup_data, comp, 0);
  }

  void CopyupRequest::queue_send()
  {
    // TODO: once the ObjectCacher allows reentrant read requests, the finisher
    // should be eliminated
    ldout(m_ictx->cct, 20) << __func__ << " " << this
			   << ": oid " << m_oid << " "
			   << ", extents " << m_image_extents << dendl;
    FunctionContext *ctx = new FunctionContext(
      boost::bind(&CopyupRequest::send, this));
    m_ictx->copyup_finisher->queue(ctx);
  }

  void CopyupRequest::complete(int r)
  {
    if (should_complete(r)) {
      delete this;
    }
  }

  bool CopyupRequest::should_complete(int r)
  {
    CephContext *cct = m_ictx->cct;
    ldout(cct, 20) << __func__ << " " << this
		   << ": oid " << m_oid
		   << ", extents " << m_image_extents
		   << ", r " << r << dendl;

    switch (m_state) {
    case STATE_READ_FROM_PARENT:
      ldout(cct, 20) << "READ_FROM_PARENT" << dendl;
      remove_from_list();
      if (r >= 0) {
        return send_object_map();
      } else if (r == -ENOENT) {
        return send_copyup();
      }
      break;

    case STATE_OBJECT_MAP:
      ldout(cct, 20) << "OBJECT_MAP" << dendl;
      if (r == 0) {
	return send_copyup();
      }
      break;

    case STATE_COPYUP:
      ldout(cct, 20) << "COPYUP" << dendl;
      complete_requests(r);
      return true;

    default:
      lderr(cct) << "invalid state: " << m_state << dendl;
      assert(false);
      break;
    }

    if (r < 0) {
      complete_requests(r);
      return true;
    }
    return false;
  }

  void CopyupRequest::remove_from_list()
  {
    Mutex::Locker l(m_ictx->copyup_list_lock);

    map<uint64_t, CopyupRequest*>::iterator it =
      m_ictx->copyup_list.find(m_object_no);
    assert(it != m_ictx->copyup_list.end());
    m_ictx->copyup_list.erase(it);
  }

  bool CopyupRequest::send_object_map() {
    bool copyup = true;
    {
      RWLock::RLocker owner_locker(m_ictx->owner_lock);
      RWLock::RLocker snap_locker(m_ictx->snap_lock);
      if (m_ictx->object_map.enabled()) {
        if (!m_ictx->image_watcher->is_lock_owner()) {
         ldout(m_ictx->cct, 20) << "exclusive lock not held for copyup request"
                                << dendl;
          assert(m_pending_requests.empty());
          return true;
        }

        RWLock::WLocker object_map_locker(m_ictx->object_map_lock);
        if (m_ictx->object_map[m_object_no] != OBJECT_EXISTS) {
          ldout(m_ictx->cct, 20) << __func__ << " " << this
			         << ": oid " << m_oid
                                 << ", extents " << m_image_extents
                                 << dendl;
          m_state = STATE_OBJECT_MAP;

          Context *ctx = create_callback_context();
          bool sent = m_ictx->object_map.aio_update(m_object_no, OBJECT_EXISTS,
                                                    boost::optional<uint8_t>(),
                                                    ctx);
          assert(sent);
          copyup = false;
        }
      }
    }

    // avoid possible recursive lock attempts
    if (copyup) {
      // no object map update required
      return send_copyup();
    }
    return false;
  }

  Context *CopyupRequest::create_callback_context()
  {
    return new FunctionContext(boost::bind(&CopyupRequest::complete, this, _1));
  }
}
