// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/AioImageRequestWQ.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioImageRequest.h"
#include "librbd/ImageCtx.h"
#include "librbd/internal.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AioImageRequestWQ: "

namespace librbd {

AioImageRequestWQ::AioImageRequestWQ(ImageCtx *image_ctx, const string &name,
                                     time_t ti, ThreadPool *tp)
  : ThreadPool::PointerWQ<AioImageRequest>(name, ti, 0, tp),
    m_image_ctx(*image_ctx), m_lock("AioImageRequestWQ::m_lock"),
    m_write_blockers(0), m_in_progress_writes(0), m_queued_writes(0),
    m_lock_listener(this), m_blocking_writes(false) {

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << ": ictx=" << image_ctx << dendl;
}

ssize_t AioImageRequestWQ::read(uint64_t off, uint64_t len, char *buf,
                                int op_flags) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "read: ictx=" << &m_image_ctx << ", off=" << off << ", "
                 << "len = " << len << dendl;

  std::vector<std::pair<uint64_t,uint64_t> > image_extents;
  image_extents.push_back(make_pair(off, len));

  C_SaferCond cond;
  AioCompletion *c = aio_create_completion_internal(&cond, rbd_ctx_cb);
  aio_read(c, off, len, buf, NULL, op_flags, false);
  return cond.wait();
}

ssize_t AioImageRequestWQ::write(uint64_t off, uint64_t len, const char *buf,
                                 int op_flags) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "write: ictx=" << &m_image_ctx << ", off=" << off << ", "
                 << "len = " << len << dendl;

  m_image_ctx.snap_lock.get_read();
  int r = clip_io(&m_image_ctx, off, &len);
  m_image_ctx.snap_lock.put_read();
  if (r < 0) {
    return r;
  }

  C_SaferCond cond;
  AioCompletion *c = aio_create_completion_internal(&cond, rbd_ctx_cb);
  aio_write(c, off, len, buf, op_flags, false);

  r = cond.wait();
  if (r < 0) {
    return r;
  }
  return len;
}

int AioImageRequestWQ::discard(uint64_t off, uint64_t len) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "discard: ictx=" << &m_image_ctx << ", off=" << off << ", "
                 << "len = " << len << dendl;

  m_image_ctx.snap_lock.get_read();
  int r = clip_io(&m_image_ctx, off, &len);
  m_image_ctx.snap_lock.put_read();
  if (r < 0) {
    return r;
  }

  C_SaferCond cond;
  AioCompletion *c = aio_create_completion_internal(&cond, rbd_ctx_cb);
  aio_discard(c, off, len, false);

  r = cond.wait();
  if (r < 0) {
    return r;
  }
  return len;
}

void AioImageRequestWQ::aio_read(AioCompletion *c, uint64_t off, uint64_t len,
                                 char *buf, bufferlist *pbl, int op_flags, bool native_async) {
  c->init_time(&m_image_ctx, librbd::AIO_TYPE_READ);
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "aio_read: ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << ", off=" << off << ", "
                 << "len=" << len << ", " << "flags=" << op_flags << dendl;

  if (native_async && m_image_ctx.event_socket.is_valid())
    c->set_event_notify(true);

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.non_blocking_aio) {
    queue(new AioImageRead(m_image_ctx, c, off, len, buf, pbl, op_flags));
  } else {
    AioImageRequest::aio_read(&m_image_ctx, c, off, len, buf, pbl, op_flags);
  }
}

void AioImageRequestWQ::aio_write(AioCompletion *c, uint64_t off, uint64_t len,
                                  const char *buf, int op_flags, bool native_async) {
  c->init_time(&m_image_ctx, librbd::AIO_TYPE_WRITE);
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "aio_write: ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << ", off=" << off << ", "
                 << "len=" << len << ", flags=" << op_flags << dendl;

  if (native_async && m_image_ctx.event_socket.is_valid())
    c->set_event_notify(true);

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.non_blocking_aio || is_journal_required() ||
      writes_blocked()) {
    queue(new AioImageWrite(m_image_ctx, c, off, len, buf, op_flags));
  } else {
    AioImageRequest::aio_write(&m_image_ctx, c, off, len, buf, op_flags);
  }
}

void AioImageRequestWQ::aio_discard(AioCompletion *c, uint64_t off,
                                    uint64_t len, bool native_async) {
  c->init_time(&m_image_ctx, librbd::AIO_TYPE_DISCARD);
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "aio_discard: ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << ", off=" << off << ", len=" << len
                 << dendl;

  if (native_async && m_image_ctx.event_socket.is_valid())
    c->set_event_notify(true);

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.non_blocking_aio || is_journal_required() ||
      writes_blocked()) {
    queue(new AioImageDiscard(m_image_ctx, c, off, len));
  } else {
    AioImageRequest::aio_discard(&m_image_ctx, c, off, len);
  }
}

void AioImageRequestWQ::aio_flush(AioCompletion *c, bool native_async) {
  c->init_time(&m_image_ctx, librbd::AIO_TYPE_FLUSH);
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "aio_flush: ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << dendl;

  if (native_async && m_image_ctx.event_socket.is_valid())
    c->set_event_notify(true);

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.non_blocking_aio || is_journal_required() ||
      writes_blocked() || !writes_empty()) {
    queue(new AioImageFlush(m_image_ctx, c));
  } else {
    AioImageRequest::aio_flush(&m_image_ctx, c);
  }
}

void AioImageRequestWQ::block_writes() {
  C_SaferCond cond_ctx;
  block_writes(&cond_ctx);
  cond_ctx.wait();
}

void AioImageRequestWQ::block_writes(Context *on_blocked) {
  CephContext *cct = m_image_ctx.cct;

  {
    Mutex::Locker locker(m_lock);
    ++m_write_blockers;
    ldout(cct, 5) << __func__ << ": " << &m_image_ctx << ", "
                  << "num=" << m_write_blockers << dendl;
    if (!m_write_blocker_contexts.empty() || m_in_progress_writes > 0) {
      m_write_blocker_contexts.push_back(on_blocked);
      return;
    }
  }

  m_image_ctx.op_work_queue->queue(on_blocked);
}

void AioImageRequestWQ::unblock_writes() {
  CephContext *cct = m_image_ctx.cct;

  bool wake_up = false;
  {
    Mutex::Locker locker(m_lock);
    assert(m_write_blockers > 0);
    --m_write_blockers;

    ldout(cct, 5) << __func__ << ": " << &m_image_ctx << ", "
                  << "num=" << m_write_blockers << dendl;
    if (m_write_blockers == 0) {
      wake_up = true;
    }
  }

  if (wake_up) {
    signal();
  }
}

void AioImageRequestWQ::register_lock_listener() {
  m_image_ctx.image_watcher->register_listener(&m_lock_listener);
}

void *AioImageRequestWQ::_void_dequeue() {
  AioImageRequest *peek_item = front();
  if (peek_item == NULL) {
    return NULL;
  }

  {
    if (peek_item->is_write_op()) {
      Mutex::Locker locker(m_lock);
      if (m_write_blockers > 0) {
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
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << __func__ << ": ictx=" << &m_image_ctx << ", "
                 << "req=" << req << dendl;

  {
    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
    req->send();
  }

  bool writes_blocked = false;
  {
    Mutex::Locker locker(m_lock);
    if (req->is_write_op()) {
      assert(m_queued_writes > 0);
      --m_queued_writes;

      assert(m_in_progress_writes > 0);
      if (--m_in_progress_writes == 0 && !m_write_blocker_contexts.empty()) {
        writes_blocked = true;
      }
    }
  }

  if (writes_blocked) {
    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
    m_image_ctx.flush(new C_BlockedWrites(this));
  }
  delete req;
}

bool AioImageRequestWQ::is_journal_required() const {
  RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
  return (m_image_ctx.journal != NULL);
}

bool AioImageRequestWQ::is_lock_required() const {
  assert(m_image_ctx.owner_lock.is_locked());
  if (m_image_ctx.image_watcher == NULL) {
    return false;
  }

  return (m_image_ctx.image_watcher->is_lock_supported() &&
          !m_image_ctx.image_watcher->is_lock_owner());
}

void AioImageRequestWQ::queue(AioImageRequest *req) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << __func__ << ": ictx=" << &m_image_ctx << ", "
                 << "req=" << req << dendl;

  assert(m_image_ctx.owner_lock.is_locked());

  bool first_write_op = false;
  {
    Mutex::Locker locker(m_lock);
    if (req->is_write_op()) {
      if (++m_queued_writes == 1) {
        first_write_op = true;
      }
    }
  }
  ThreadPool::PointerWQ<AioImageRequest>::queue(req);

  if (is_lock_required() && first_write_op) {
    m_image_ctx.image_watcher->request_lock();
  }
}

void AioImageRequestWQ::handle_lock_updated(
    ImageWatcher::LockUpdateState state) {
  assert(m_image_ctx.owner_lock.is_locked());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << __func__ << ": ictx=" << &m_image_ctx << ", "
                 << "state=" << state << dendl;

  if ((state == ImageWatcher::LOCK_UPDATE_STATE_NOT_SUPPORTED ||
       state == ImageWatcher::LOCK_UPDATE_STATE_LOCKED) && m_blocking_writes) {
    m_blocking_writes = false;
    unblock_writes();
  } else if (state == ImageWatcher::LOCK_UPDATE_STATE_RELEASING &&
             !m_blocking_writes) {
    m_blocking_writes = true;
    block_writes();
  } else if (state == ImageWatcher::LOCK_UPDATE_STATE_UNLOCKED) {
    assert(m_blocking_writes);
    assert(writes_blocked());
  } else if (state == ImageWatcher::LOCK_UPDATE_STATE_NOTIFICATION &&
             !writes_empty()) {
    m_image_ctx.image_watcher->request_lock();
  }
}

void AioImageRequestWQ::handle_blocked_writes(int r) {
  Contexts contexts;
  {
    Mutex::Locker locker(m_lock);
    contexts.swap(m_write_blocker_contexts);
  }

  for (auto ctx : contexts) {
    ctx->complete(0);
  }
}

} // namespace librbd
