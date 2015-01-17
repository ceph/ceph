// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"
#include "common/dout.h"
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
                               uint64_t objectno, bool send_copyup)
    : m_ictx(ictx), m_oid(oid), m_object_no(objectno),
      m_lock("librbd::CopyupRequest::m_lock"), m_ready(false),
      m_send_copyup(send_copyup), m_parent_completion(NULL),
      m_copyup_completion(NULL) {}

  CopyupRequest::~CopyupRequest() {
    assert(m_pending_requests.empty());

    m_ictx->copyup_list_lock.Lock();
    ldout(m_ictx->cct, 20) << __func__ << " removing the slot " << dendl;
    map<uint64_t, CopyupRequest*>::iterator it =
      m_ictx->copyup_list.find(m_object_no);
    assert(it != m_ictx->copyup_list.end());
    it->second = NULL;
    m_ictx->copyup_list.erase(it);

    if (m_ictx->copyup_list.empty())
      m_ictx->copyup_list_cond.Signal();

    ldout(m_ictx->cct, 20) <<  __func__ << " remove the slot " << m_object_no
                           << " in copyup_list, size = " << m_ictx->copyup_list.size()
                           << dendl;

    m_ictx->copyup_list_lock.Unlock();
  }

  void CopyupRequest::set_ready() {
    m_ready = true;
  }

  bool CopyupRequest::is_ready() {
    return m_ready;
  }

  bool CopyupRequest::should_send_copyup() {
    return m_send_copyup;
  }

  ceph::bufferlist& CopyupRequest::get_copyup_data() {
    return m_copyup_data;
  }

  Mutex& CopyupRequest::get_lock() {
    return m_lock;
  }

  void CopyupRequest::append_request(AioRequest *req) {
    assert(!m_ready);
    m_pending_requests.push_back(req);
  }

  void CopyupRequest::complete_all(int r) {
    assert(m_ready);

    while (!m_pending_requests.empty()) {
      vector<AioRequest *>::iterator it = m_pending_requests.begin();
      AioRequest *req = *it;
      req->complete(r);
      m_pending_requests.erase(it);
    }
  }

  void CopyupRequest::send_copyup(int r) {
    ldout(m_ictx->cct, 20) << __func__ << dendl;

    m_ictx->snap_lock.get_read();
    ::SnapContext snapc = m_ictx->snapc;
    m_ictx->snap_lock.put_read();

    std::vector<librados::snap_t> snaps;
    for (std::vector<snapid_t>::const_iterator it = snapc.snaps.begin();
         it != snapc.snaps.end(); ++it) {
      snaps.push_back(it->val);
    }

    librados::ObjectWriteOperation copyup_op;
    copyup_op.exec("rbd", "copyup", m_copyup_data);

    m_copyup_completion = librados::Rados::aio_create_completion(this, NULL, rbd_copyup_cb);
    m_ictx->md_ctx.aio_operate(m_oid, m_copyup_completion, &copyup_op,
                               snapc.seq.val, snaps);
    m_copyup_completion->release();
  }

  void CopyupRequest::read_from_parent(vector<pair<uint64_t,uint64_t> >& image_extents)
  {
    m_parent_completion = aio_create_completion_internal(this, rbd_read_from_parent_cb);
    ldout(m_ictx->cct, 20) << __func__ << " this = " << this
                           << " parent completion " << m_parent_completion
                           << " extents " << image_extents
                           << dendl;
    aio_read(m_ictx->parent, image_extents, NULL, &m_copyup_data, m_parent_completion, 0);
  }

  void CopyupRequest::queue_read_from_parent(vector<pair<uint64_t,uint64_t> >& image_extents)
  {
    // TODO: once the ObjectCacher allows reentrant read requests, the finisher
    // should be eliminated
    C_ReadFromParent *ctx = new C_ReadFromParent(this, image_extents);
    m_ictx->copyup_finisher->queue(ctx);
  }

  void CopyupRequest::rbd_read_from_parent_cb(completion_t cb, void *arg)
  {
    CopyupRequest *req = reinterpret_cast<CopyupRequest *>(arg);
    AioCompletion *comp = reinterpret_cast<AioCompletion *>(cb);

    ldout(req->m_ictx->cct, 20) << __func__ << dendl;

    req->m_lock.Lock();
    req->set_ready();
    req->complete_all(comp->get_return_value());
    req->m_lock.Unlock();

    // If this entry is created by a read request, then copyup operation will
    // be performed asynchronously. Perform cleaning up from copyup callback.
    // If this entry is created by a write request, then copyup operation will
    // be performed synchronously by AioWrite. After extracting data, perform
    // cleaning up here
    if (req->should_send_copyup())
      req->send_copyup(comp->get_return_value());
    else
      delete req;
  }

  void CopyupRequest::rbd_copyup_cb(rados_completion_t c, void *arg)
  {
    CopyupRequest *req = reinterpret_cast<CopyupRequest *>(arg);

    ldout(req->m_ictx->cct, 20) << __func__ << dendl;
    delete req;
  }
}
