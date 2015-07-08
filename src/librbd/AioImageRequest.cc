// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/AioImageRequest.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioObjectRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AioImageRequest: "

namespace librbd {

void AioImageRequest::read(
    ImageCtx *ictx, AioCompletion *c,
    const std::vector<std::pair<uint64_t,uint64_t> > &extents,
    char *buf, bufferlist *pbl, int op_flags) {
  AioImageRead req(*ictx, c, extents, buf, pbl, op_flags);
  req.send();
}

void AioImageRequest::read(ImageCtx *ictx, AioCompletion *c, uint64_t off,
                           size_t len, char *buf, bufferlist *pbl,
                           int op_flags) {
  AioImageRead req(*ictx, c, off, len, buf, pbl, op_flags);
  req.send();
}

void AioImageRequest::write(ImageCtx *ictx, AioCompletion *c, uint64_t off,
                            size_t len, const char *buf, int op_flags) {
  AioImageWrite req(*ictx, c, off, len, buf, op_flags);
  req.send();
}

void AioImageRequest::discard(ImageCtx *ictx, AioCompletion *c, uint64_t off,
                              uint64_t len) {
  AioImageDiscard req(*ictx, c, off, len);
  req.send();
}

void AioImageRequest::flush(ImageCtx *ictx, AioCompletion *c) {
  AioImageFlush req(*ictx, c);
  req.send();
}

void AioImageRequest::send() {
  assert(m_image_ctx.owner_lock.is_locked());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << get_request_type() << ": ictx=" << &m_image_ctx << ", "
                 << "completion=" << m_aio_comp <<  dendl;

  m_aio_comp->get();
  int r = ictx_check(&m_image_ctx, m_image_ctx.owner_lock);
  if (r < 0) {
    m_aio_comp->fail(cct, r);
    return;
  }

  execute_request();
}


void AioImageRead::execute_request() {
  CephContext *cct = m_image_ctx.cct;

  if (m_image_ctx.object_cacher && m_image_ctx.readahead_max_bytes > 0 &&
      !(m_op_flags & LIBRADOS_OP_FLAG_FADVISE_RANDOM)) {
    readahead(&m_image_ctx, m_image_extents);
  }

  librados::snap_t snap_id;
  map<object_t,vector<ObjectExtent> > object_extents;
  uint64_t buffer_ofs = 0;
  {
    // prevent image size from changing between computing clip and recording
    // pending async operation
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    snap_id = m_image_ctx.snap_id;

    // map
    for (vector<pair<uint64_t,uint64_t> >::const_iterator p =
           m_image_extents.begin();
         p != m_image_extents.end(); ++p) {
      uint64_t len = p->second;
      int r = clip_io(&m_image_ctx, p->first, &len);
      if (r < 0) {
        m_aio_comp->fail(cct, r);
        return;
      }
      if (len == 0) {
        continue;
      }

      Striper::file_to_extents(cct, m_image_ctx.format_string,
                               &m_image_ctx.layout, p->first, len, 0,
                               object_extents, buffer_ofs);
      buffer_ofs += len;
    }

    m_aio_comp->init_time(&m_image_ctx, AIO_TYPE_READ);
  }

  m_aio_comp->read_buf = m_buf;
  m_aio_comp->read_buf_len = buffer_ofs;
  m_aio_comp->read_bl = m_pbl;

  for (map<object_t,vector<ObjectExtent> >::iterator p = object_extents.begin();
       p != object_extents.end(); ++p) {
    for (vector<ObjectExtent>::iterator q = p->second.begin();
         q != p->second.end(); ++q) {
      ldout(cct, 20) << " oid " << q->oid << " " << q->offset << "~"
                     << q->length << " from " << q->buffer_extents
                     << dendl;

      C_AioRead *req_comp = new C_AioRead(cct, m_aio_comp);
      AioObjectRead *req = new AioObjectRead(&m_image_ctx, q->oid.name,
                                             q->objectno, q->offset, q->length,
                                             q->buffer_extents, snap_id, true,
                                             req_comp, m_op_flags);
      req_comp->set_req(req);

      if (m_image_ctx.object_cacher) {
        C_CacheRead *cache_comp = new C_CacheRead(&m_image_ctx, req);
        m_image_ctx.aio_read_from_cache(q->oid, q->objectno, &req->data(),
                                        q->length, q->offset,
                                        cache_comp, m_op_flags);
      } else {
        req->send();
      }
    }
  }

  m_aio_comp->finish_adding_requests(cct);
  m_aio_comp->put();

  m_image_ctx.perfcounter->inc(l_librbd_rd);
  m_image_ctx.perfcounter->inc(l_librbd_rd_bytes, buffer_ofs);
}

void AioImageWrite::execute_request() {
  assert(!m_image_ctx.image_watcher->is_lock_supported() ||
          m_image_ctx.image_watcher->is_lock_owner());

  CephContext *cct = m_image_ctx.cct;

  RWLock::RLocker md_locker(m_image_ctx.md_lock);

  uint64_t clip_len = m_len;
  ::SnapContext snapc;
  {
    // prevent image size from changing between computing clip and recording
    // pending async operation
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      m_aio_comp->fail(cct, -EROFS);
      return;
    }

    int r = clip_io(&m_image_ctx, m_off, &clip_len);
    if (r < 0) {
      m_aio_comp->fail(cct, r);
      return;
    }

    snapc = m_image_ctx.snapc;
    m_aio_comp->init_time(&m_image_ctx, AIO_TYPE_WRITE);
  }

  // map
  vector<ObjectExtent> extents;
  if (m_len > 0) {
    Striper::file_to_extents(cct, m_image_ctx.format_string,
                             &m_image_ctx.layout, m_off, clip_len, 0, extents);
  }

  for (vector<ObjectExtent>::iterator p = extents.begin();
       p != extents.end(); ++p) {
    ldout(cct, 20) << " oid " << p->oid << " " << p->offset << "~" << p->length
                   << " from " << p->buffer_extents << dendl;
    // assemble extent
    bufferlist bl;
    for (vector<pair<uint64_t,uint64_t> >::iterator q =
           p->buffer_extents.begin();
         q != p->buffer_extents.end(); ++q) {
      bl.append(m_buf + q->first, q->second);
    }

    C_AioRequest *req_comp = new C_AioRequest(cct, m_aio_comp);
    if (m_image_ctx.object_cacher) {
      m_image_ctx.write_to_cache(p->oid, bl, p->length, p->offset, req_comp,
                                 m_op_flags);
    } else {
      AioObjectWrite *req = new AioObjectWrite(&m_image_ctx, p->oid.name,
                                               p->objectno, p->offset, bl,
                                               snapc, req_comp);

      req->set_op_flags(m_op_flags);
      req->send();
    }
  }

  m_aio_comp->finish_adding_requests(cct);
  m_aio_comp->put();

  m_image_ctx.perfcounter->inc(l_librbd_wr);
  m_image_ctx.perfcounter->inc(l_librbd_wr_bytes, clip_len);
}

void AioImageDiscard::execute_request() {
  assert(!m_image_ctx.image_watcher->is_lock_supported() ||
          m_image_ctx.image_watcher->is_lock_owner());

  CephContext *cct = m_image_ctx.cct;

  RWLock::RLocker md_locker(m_image_ctx.md_lock);

  uint64_t clip_len = m_len;
  ::SnapContext snapc;
  {
    // prevent image size from changing between computing clip and recording
    // pending async operation
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      m_aio_comp->fail(cct, -EROFS);
      return;
    }

    int r = clip_io(&m_image_ctx, m_off, &clip_len);
    if (r < 0) {
      m_aio_comp->fail(cct, r);
      return;
    }

    snapc = m_image_ctx.snapc;
    m_aio_comp->init_time(&m_image_ctx, AIO_TYPE_DISCARD);
  }

  // map
  vector<ObjectExtent> extents;
  if (m_len > 0) {
    Striper::file_to_extents(cct, m_image_ctx.format_string,
                             &m_image_ctx.layout, m_off, clip_len, 0, extents);
  }

  for (vector<ObjectExtent>::iterator p = extents.begin();
       p != extents.end(); ++p) {
    ldout(cct, 20) << " oid " << p->oid << " " << p->offset << "~" << p->length
                   << " from " << p->buffer_extents << dendl;
    C_AioRequest *req_comp = new C_AioRequest(cct, m_aio_comp);
    AioObjectRequest *req;

    if (p->length == m_image_ctx.layout.fl_object_size) {
      req = new AioObjectRemove(&m_image_ctx, p->oid.name, p->objectno, snapc,
                                req_comp);
    } else if (p->offset + p->length == m_image_ctx.layout.fl_object_size) {
      req = new AioObjectTruncate(&m_image_ctx, p->oid.name, p->objectno,
                                  p->offset, snapc, req_comp);
    } else {
      if(cct->_conf->rbd_skip_partial_discard) {
        delete req_comp;
        continue;
      }
      req = new AioObjectZero(&m_image_ctx, p->oid.name, p->objectno, p->offset,
                              p->length, snapc, req_comp);
    }
    req->send();
  }

  if (m_image_ctx.object_cacher) {
    Mutex::Locker l(m_image_ctx.cache_lock);
    m_image_ctx.object_cacher->discard_set(m_image_ctx.object_set, extents);
  }

  m_aio_comp->finish_adding_requests(cct);
  m_aio_comp->put();

  m_image_ctx.perfcounter->inc(l_librbd_discard);
  m_image_ctx.perfcounter->inc(l_librbd_discard_bytes, clip_len);
}

void AioImageFlush::execute_request() {
  CephContext *cct = m_image_ctx.cct;

  // TODO race condition between registering op and submitting to cache
  //      (might not be flushed -- backport needed)
  C_AioRequest *flush_ctx = new C_AioRequest(cct, m_aio_comp);
  m_image_ctx.flush_async_operations(flush_ctx);

  m_aio_comp->init_time(&m_image_ctx, AIO_TYPE_FLUSH);
  C_AioRequest *req_comp = new C_AioRequest(cct, m_aio_comp);
  if (m_image_ctx.object_cacher != NULL) {
    m_image_ctx.flush_cache_aio(req_comp);
  } else {
    librados::AioCompletion *rados_completion =
      librados::Rados::aio_create_completion(req_comp, NULL, rados_ctx_cb);
    m_image_ctx.data_ctx.aio_flush_async(rados_completion);
    rados_completion->release();
  }

  m_aio_comp->finish_adding_requests(cct);
  m_aio_comp->put();

  m_image_ctx.perfcounter->inc(l_librbd_aio_flush);
}

} // namespace librbd
