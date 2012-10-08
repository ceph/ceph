// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <errno.h>

#include "common/ceph_context.h"
#include "common/dout.h"
#include "common/Mutex.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"

#include "librbd/AioRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"
#include "librbd/LibrbdWriteback.h"

#include "include/assert.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbdwriteback: "

namespace librbd {

  class C_Request : public Context {
  public:
    C_Request(CephContext *cct, Context *c, Mutex *l)
      : m_cct(cct), m_ctx(c), m_lock(l) {}
    virtual ~C_Request() {}
    void set_req(AioRequest *req);
    virtual void finish(int r) {
      ldout(m_cct, 20) << "aio_cb completing " << dendl;
      {
	Mutex::Locker l(*m_lock);
	m_ctx->complete(r);
      }
      ldout(m_cct, 20) << "aio_cb finished" << dendl;
    }
  private:
    CephContext *m_cct;
    Context *m_ctx;
    Mutex *m_lock;
  };

  class C_Read : public Context {
  public:
    C_Read(Context *real_context, bufferlist *pbl)
      : m_ctx(real_context), m_req(NULL), m_out_bl(pbl) {}
    virtual ~C_Read() {}
    virtual void finish(int r) {
      if (r >= 0)
	*m_out_bl = m_req->data();
      m_ctx->complete(r);
    }
    void set_req(AioRead *req) {
      m_req = req;
    }
  private:
    Context *m_ctx;
    AioRead *m_req;
    bufferlist *m_out_bl;
  };

  LibrbdWriteback::LibrbdWriteback(ImageCtx *ictx, Mutex& lock)
    : m_tid(0), m_lock(lock), m_ictx(ictx)
  {
  }

  tid_t LibrbdWriteback::read(const object_t& oid,
			      const object_locator_t& oloc,
			      uint64_t off, uint64_t len, snapid_t snapid,
			      bufferlist *pbl, uint64_t trunc_size,
			      __u32 trunc_seq, Context *onfinish)
  {
    C_Request *req_comp = new C_Request(m_ictx->cct, onfinish, &m_lock);
    C_Read *read_comp = new C_Read(req_comp, pbl);
    uint64_t object_no = oid_to_object_no(oid.name, m_ictx->object_prefix);
    vector<pair<uint64_t,uint64_t> > ex(1);
    ex[0] = make_pair(off, len);
    AioRead *req = new AioRead(m_ictx, oid.name,
			       object_no, off, len, ex,
			       snapid, false, read_comp);
    read_comp->set_req(req);
    req->send();
    return ++m_tid;
  }

  tid_t LibrbdWriteback::write(const object_t& oid,
			       const object_locator_t& oloc,
			       uint64_t off, uint64_t len,
			       const SnapContext& snapc,
			       const bufferlist &bl, utime_t mtime,
			       uint64_t trunc_size, __u32 trunc_seq,
			       Context *oncommit)
  {
    m_ictx->snap_lock.Lock();
    librados::snap_t snap_id = m_ictx->snap_id;
    m_ictx->parent_lock.Lock();
    uint64_t overlap = 0;
    m_ictx->get_parent_overlap(snap_id, &overlap);
    m_ictx->parent_lock.Unlock();
    m_ictx->snap_lock.Unlock();

    uint64_t object_no = oid_to_object_no(oid.name, m_ictx->object_prefix);
    
    // reverse map this object extent onto the parent
    vector<pair<uint64_t,uint64_t> > objectx;
    Striper::extent_to_file(m_ictx->cct, &m_ictx->layout,
			  object_no, 0, m_ictx->layout.fl_object_size,
			  objectx);
    uint64_t object_overlap = m_ictx->prune_parent_extents(objectx, overlap);

    C_Request *req_comp = new C_Request(m_ictx->cct, oncommit, &m_lock);
    AioWrite *req = new AioWrite(m_ictx, oid.name,
				 object_no, off, objectx, object_overlap,
				 bl, snapc, snap_id,
				 req_comp);
    req->send();
    return ++m_tid;
  }
}
