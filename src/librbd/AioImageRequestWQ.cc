// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/AioImageRequestWQ.h"
#include "librbd/AioImageRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AioImageRequestWQ: "

namespace librbd {

ssize_t AioImageRequestWQ::read(uint64_t off, size_t len, char *buf,
                                int op_flags) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "read " << &m_image_ctx << " off = " << off << " len = "
                 << len << dendl;

  std::vector<std::pair<uint64_t,uint64_t> > image_extents;
  image_extents.push_back(make_pair(off, len));

  C_SaferCond cond;
  AioCompletion *c = aio_create_completion_internal(&cond, rbd_ctx_cb);
  aio_read(c, off, len, buf, NULL, op_flags);
  return cond.wait();
}

ssize_t AioImageRequestWQ::write(uint64_t off, size_t len, const char *buf,
                                 int op_flags) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "write " << &m_image_ctx << " off = " << off << " len = "
                 << len << dendl;

  m_image_ctx.snap_lock.get_read();
  int r = clip_io(&m_image_ctx, off, &len);
  m_image_ctx.snap_lock.put_read();
  if (r < 0) {
    return r;
  }

  C_SaferCond cond;
  AioCompletion *c = aio_create_completion_internal(&cond, rbd_ctx_cb);
  aio_write(c, off, len, buf, op_flags);

  r = cond.wait();
  if (r < 0) {
    return r;
  }
  return len;
}

int AioImageRequestWQ::discard(uint64_t off, uint64_t len) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "discard " << &m_image_ctx << " off = " << off << " len = "
                 << len << dendl;

  m_image_ctx.snap_lock.get_read();
  int r = clip_io(&m_image_ctx, off, &len);
  m_image_ctx.snap_lock.put_read();
  if (r < 0) {
    return r;
  }

  C_SaferCond cond;
  AioCompletion *c = aio_create_completion_internal(&cond, rbd_ctx_cb);
  aio_discard(c, off, len);

  r = cond.wait();
  if (r < 0) {
    return r;
  }
  return len;
}

void AioImageRequestWQ::aio_read(AioCompletion *c, uint64_t off, size_t len,
                                 char *buf, bufferlist *pbl, int op_flags) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "aio_read: ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << ", off=" << off << ", len=" << len
                 << "flags=" << op_flags << dendl;

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.non_blocking_aio) {
    queue(new AioImageRead(m_image_ctx, c, off, len, buf, pbl, op_flags));
  } else {
    AioImageRequest::read(&m_image_ctx, c, off, len, buf, pbl, op_flags);
  }
}

void AioImageRequestWQ::aio_write(AioCompletion *c, uint64_t off, size_t len,
                                  const char *buf, int op_flags) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "aio_write: ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << ", off=" << off << ", len=" << len
                 << "flags=" << op_flags << dendl;

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  bool lock_required = is_lock_required();
  if (m_image_ctx.non_blocking_aio || lock_required) {
    queue(new AioImageWrite(m_image_ctx, c, off, len, buf, op_flags));
  } else {
    AioImageRequest::write(&m_image_ctx, c, off, len, buf, op_flags);
  }
}

void AioImageRequestWQ::aio_discard(AioCompletion *c, uint64_t off,
                                    uint64_t len) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "aio_discard: ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << ", off=" << off << ", len=" << len
                 << dendl;

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  bool lock_required = is_lock_required();
  if (m_image_ctx.non_blocking_aio || lock_required) {
    queue(new AioImageDiscard(m_image_ctx, c, off, len));
  } else {
    AioImageRequest::discard(&m_image_ctx, c, off, len);
  }
}

void AioImageRequestWQ::aio_flush(AioCompletion *c) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "aio_flush: ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << dendl;

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.non_blocking_aio) {
    queue(new AioImageFlush(m_image_ctx, c));
  } else {
    AioImageRequest::flush(&m_image_ctx, c);
  }
}

bool AioImageRequestWQ::writes_empty() const {
  Mutex::Locker locker(m_lock);
  return (m_queued_writes > 0);
}

void *AioImageRequestWQ::_void_dequeue() {
  AioImageRequest *peek_item = front();
  if (peek_item == NULL) {
    return NULL;
  }

  {
    if (peek_item->is_write_op()) {
      Mutex::Locker locker(m_lock);
      if (m_writes_suspended) {
        return NULL;
      }
      ++m_in_progress_writes;
    }
  }

  AioImageRequest *item = reinterpret_cast<AioImageRequest *>(
    ThreadPool::PointerWQ<AioImageRequest>::_void_dequeue());
  assert(peek_item == item);
  return item;
}

void AioImageRequestWQ::process(AioImageRequest *req) {
  {
    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
    req->send();
  }

  {
    Mutex::Locker locker(m_lock);
    if (req->is_write_op()) {
      assert(m_queued_writes > 0);
      --m_queued_writes;

      assert(m_in_progress_writes > 0);
      if (--m_in_progress_writes == 0) {
        m_cond.Signal();
      }
    }
  }
  delete req;
}

bool AioImageRequestWQ::is_lock_required() {
  assert(m_image_ctx.owner_lock.is_locked());
  if (m_image_ctx.image_watcher == NULL) {
    return false;
  }
  return (m_image_ctx.image_watcher->is_lock_supported() &&
          !m_image_ctx.image_watcher->is_lock_owner());
}

void AioImageRequestWQ::queue(AioImageRequest *req) {
  {
    Mutex::Locker locker(m_lock);
    if (req->is_write_op()) {
      ++m_queued_writes;
    }
  }
  ThreadPool::PointerWQ<AioImageRequest>::queue(req);
}

} // namespace librbd
