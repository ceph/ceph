// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/AioImageRequestWQ.h"
#include "common/errno.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioImageRequest.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AioImageRequestWQ: "

namespace librbd {

AioImageRequestWQ::AioImageRequestWQ(ImageCtx *image_ctx, const string &name,
                                     time_t ti, ThreadPool *tp)
  : ThreadPool::PointerWQ<AioImageRequest<> >(name, ti, 0, tp),
    m_image_ctx(*image_ctx),
    m_lock(util::unique_lock_name("AioImageRequestWQ::m_lock", this)),
    m_write_blockers(0), m_in_progress_writes(0), m_queued_reads(0),
    m_queued_writes(0), m_in_flight_ops(0), m_refresh_in_progress(false),
    m_shutdown(false), m_on_shutdown(nullptr) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << ": ictx=" << image_ctx << dendl;
  tp->add_work_queue(this);
}

ssize_t AioImageRequestWQ::read(uint64_t off, uint64_t len, char *buf,
                                int op_flags) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "read: ictx=" << &m_image_ctx << ", off=" << off << ", "
                 << "len = " << len << dendl;

  std::vector<std::pair<uint64_t,uint64_t> > image_extents;
  image_extents.push_back(make_pair(off, len));

  C_SaferCond cond;
  AioCompletion *c = AioCompletion::create(&cond);
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
    lderr(cct) << "invalid IO request: " << cpp_strerror(r) << dendl;
    return r;
  }

  C_SaferCond cond;
  AioCompletion *c = AioCompletion::create(&cond);
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
    lderr(cct) << "invalid IO request: " << cpp_strerror(r) << dendl;
    return r;
  }

  C_SaferCond cond;
  AioCompletion *c = AioCompletion::create(&cond);
  aio_discard(c, off, len, false);

  r = cond.wait();
  if (r < 0) {
    return r;
  }
  return len;
}

void AioImageRequestWQ::aio_read(AioCompletion *c, uint64_t off, uint64_t len,
                                 char *buf, bufferlist *pbl, int op_flags,
                                 bool native_async) {
  c->init_time(&m_image_ctx, librbd::AIO_TYPE_READ);
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "aio_read: ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << ", off=" << off << ", "
                 << "len=" << len << ", " << "flags=" << op_flags << dendl;

  if (native_async && m_image_ctx.event_socket.is_valid()) {
    c->set_event_notify(true);
  }

  if (!start_in_flight_op(c)) {
    return;
  }

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);

  // if journaling is enabled -- we need to replay the journal because
  // it might contain an uncommitted write
  bool lock_required;
  {
    RWLock::RLocker locker(m_lock);
    lock_required = m_require_lock_on_read;
  }

  if (m_image_ctx.non_blocking_aio || writes_blocked() || !writes_empty() ||
      lock_required) {
    queue(new AioImageRead(m_image_ctx, c, off, len, buf, pbl, op_flags));
  } else {
    AioImageRequest<>::aio_read(&m_image_ctx, c, off, len, buf, pbl, op_flags);
    finish_in_flight_op();
  }
}

void AioImageRequestWQ::aio_write(AioCompletion *c, uint64_t off, uint64_t len,
                                  const char *buf, int op_flags,
                                  bool native_async) {
  c->init_time(&m_image_ctx, librbd::AIO_TYPE_WRITE);
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "aio_write: ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << ", off=" << off << ", "
                 << "len=" << len << ", flags=" << op_flags << dendl;

  if (native_async && m_image_ctx.event_socket.is_valid()) {
    c->set_event_notify(true);
  }

  if (!start_in_flight_op(c)) {
    return;
  }

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.non_blocking_aio || writes_blocked()) {
    queue(new AioImageWrite(m_image_ctx, c, off, len, buf, op_flags));
  } else {
    AioImageRequest<>::aio_write(&m_image_ctx, c, off, len, buf, op_flags);
    finish_in_flight_op();
  }
}

void AioImageRequestWQ::aio_discard(AioCompletion *c, uint64_t off,
                                    uint64_t len, bool native_async) {
  c->init_time(&m_image_ctx, librbd::AIO_TYPE_DISCARD);
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "aio_discard: ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << ", off=" << off << ", len=" << len
                 << dendl;

  if (native_async && m_image_ctx.event_socket.is_valid()) {
    c->set_event_notify(true);
  }

  if (!start_in_flight_op(c)) {
    return;
  }

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.non_blocking_aio || writes_blocked()) {
    queue(new AioImageDiscard(m_image_ctx, c, off, len));
  } else {
    AioImageRequest<>::aio_discard(&m_image_ctx, c, off, len);
    finish_in_flight_op();
  }
}

void AioImageRequestWQ::aio_flush(AioCompletion *c, bool native_async) {
  c->init_time(&m_image_ctx, librbd::AIO_TYPE_FLUSH);
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "aio_flush: ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << dendl;

  if (native_async && m_image_ctx.event_socket.is_valid()) {
    c->set_event_notify(true);
  }

  if (!start_in_flight_op(c)) {
    return;
  }

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.non_blocking_aio || writes_blocked() || !writes_empty()) {
    queue(new AioImageFlush(m_image_ctx, c));
  } else {
    AioImageRequest<>::aio_flush(&m_image_ctx, c);
    finish_in_flight_op();
  }
}

void AioImageRequestWQ::shut_down(Context *on_shutdown) {
  assert(m_image_ctx.owner_lock.is_locked());

  {
    RWLock::WLocker locker(m_lock);
    assert(!m_shutdown);
    m_shutdown = true;

    CephContext *cct = m_image_ctx.cct;
    ldout(cct, 5) << __func__ << ": in_flight=" << m_in_flight_ops.read()
                  << dendl;
    if (m_in_flight_ops.read() > 0) {
      m_on_shutdown = on_shutdown;
      return;
    }
  }

  // ensure that all in-flight IO is flushed
  m_image_ctx.flush(on_shutdown);
}

bool AioImageRequestWQ::is_lock_request_needed() const {
  RWLock::RLocker locker(m_lock);
  return (m_queued_writes.read() > 0 ||
          (m_require_lock_on_read && m_queued_reads.read() > 0));
}

int AioImageRequestWQ::block_writes() {
  C_SaferCond cond_ctx;
  block_writes(&cond_ctx);
  return cond_ctx.wait();
}

void AioImageRequestWQ::block_writes(Context *on_blocked) {
  assert(m_image_ctx.owner_lock.is_locked());
  CephContext *cct = m_image_ctx.cct;

  {
    RWLock::WLocker locker(m_lock);
    ++m_write_blockers;
    ldout(cct, 5) << __func__ << ": " << &m_image_ctx << ", "
                  << "num=" << m_write_blockers << dendl;
    if (!m_write_blocker_contexts.empty() || m_in_progress_writes.read() > 0) {
      m_write_blocker_contexts.push_back(on_blocked);
      return;
    }
  }

  // ensure that all in-flight IO is flushed
  m_image_ctx.flush(on_blocked);
}

void AioImageRequestWQ::unblock_writes() {
  CephContext *cct = m_image_ctx.cct;

  bool wake_up = false;
  {
    RWLock::WLocker locker(m_lock);
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

void AioImageRequestWQ::set_require_lock_on_read() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << __func__ << dendl;

  RWLock::WLocker locker(m_lock);
  m_require_lock_on_read = true;
}

void AioImageRequestWQ::clear_require_lock_on_read() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << __func__ << dendl;

  {
    RWLock::WLocker locker(m_lock);
    if (!m_require_lock_on_read) {
      return;
    }

    m_require_lock_on_read = false;
  }
  signal();
}

void *AioImageRequestWQ::_void_dequeue() {
  AioImageRequest<> *peek_item = front();

  // no IO ops available or refresh in-progress (IO stalled)
  if (peek_item == nullptr || m_refresh_in_progress) {
    return nullptr;
  }

  bool refresh_required = m_image_ctx.state->is_refresh_required();
  {
    RWLock::RLocker locker(m_lock);
    if (peek_item->is_write_op()) {
      if (m_write_blockers > 0) {
        return nullptr;
      }

      // refresh will requeue the op -- don't count it as in-progress
      if (!refresh_required) {
        m_in_progress_writes.inc();
      }
    } else if (m_require_lock_on_read) {
      return nullptr;
    }
  }

  AioImageRequest<> *item = reinterpret_cast<AioImageRequest<> *>(
    ThreadPool::PointerWQ<AioImageRequest<> >::_void_dequeue());
  assert(peek_item == item);

  if (refresh_required) {
    ldout(m_image_ctx.cct, 15) << "image refresh required: delaying IO " << item
                               << dendl;

    // stall IO until the refresh completes
    m_refresh_in_progress = true;

    get_pool_lock().Unlock();
    m_image_ctx.state->refresh(new C_RefreshFinish(this, item));
    get_pool_lock().Lock();
    return nullptr;
  }
  return item;
}

void AioImageRequestWQ::process(AioImageRequest<> *req) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << __func__ << ": ictx=" << &m_image_ctx << ", "
                 << "req=" << req << dendl;

  {
    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
    req->send();
  }

  finish_queued_op(req);
  if (req->is_write_op()) {
    finish_in_progress_write();
  }
  delete req;

  finish_in_flight_op();
}

void AioImageRequestWQ::finish_queued_op(AioImageRequest<> *req) {
  RWLock::RLocker locker(m_lock);
  if (req->is_write_op()) {
    assert(m_queued_writes.read() > 0);
    m_queued_writes.dec();
  } else {
    assert(m_queued_reads.read() > 0);
    m_queued_reads.dec();
  }
}

void AioImageRequestWQ::finish_in_progress_write() {
  bool writes_blocked = false;
  {
    RWLock::RLocker locker(m_lock);
    assert(m_in_progress_writes.read() > 0);
    if (m_in_progress_writes.dec() == 0 &&
        !m_write_blocker_contexts.empty()) {
      writes_blocked = true;
    }
  }

  if (writes_blocked) {
    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
    m_image_ctx.flush(new C_BlockedWrites(this));
  }
}

int AioImageRequestWQ::start_in_flight_op(AioCompletion *c) {
  RWLock::RLocker locker(m_lock);

  if (m_shutdown) {
    CephContext *cct = m_image_ctx.cct;
    lderr(cct) << "IO received on closed image" << dendl;

    c->get();
    c->fail(cct, -ESHUTDOWN);
    return false;
  }

  m_in_flight_ops.inc();
  return true;
}

void AioImageRequestWQ::finish_in_flight_op() {
  {
    RWLock::RLocker locker(m_lock);
    if (m_in_flight_ops.dec() > 0 || !m_shutdown) {
      return;
    }
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << __func__ << ": completing shut down" << dendl;

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  assert(m_on_shutdown != nullptr);
  m_image_ctx.flush(m_on_shutdown);
}

bool AioImageRequestWQ::is_lock_required() const {
  assert(m_image_ctx.owner_lock.is_locked());
  if (m_image_ctx.exclusive_lock == NULL) {
    return false;
  }

  return (!m_image_ctx.exclusive_lock->is_lock_owner());
}

void AioImageRequestWQ::queue(AioImageRequest<> *req) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << __func__ << ": ictx=" << &m_image_ctx << ", "
                 << "req=" << req << dendl;

  assert(m_image_ctx.owner_lock.is_locked());
  bool write_op = req->is_write_op();
  if (write_op) {
    m_queued_writes.inc();
  } else {
    m_queued_reads.inc();
  }

  ThreadPool::PointerWQ<AioImageRequest<> >::queue(req);

  if ((write_op && is_lock_required()) ||
      (!write_op && m_require_lock_on_read)) {
    m_image_ctx.exclusive_lock->request_lock(nullptr);
  }
}

void AioImageRequestWQ::handle_refreshed(int r, AioImageRequest<> *req) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 15) << "resuming IO after image refresh: r=" << r << ", "
                 << "req=" << req << dendl;
  if (r < 0) {
    req->fail(r);
    delete req;

    finish_queued_op(req);
    finish_in_flight_op();
  } else {
    // since IO was stalled for refresh -- original IO order is preserved
    // if we requeue this op for work queue processing
    requeue(req);
  }

  m_refresh_in_progress = false;
  signal();

  // refresh might have enabled exclusive lock -- IO stalled until
  // we acquire the lock
  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (is_lock_required() && is_lock_request_needed()) {
    m_image_ctx.exclusive_lock->request_lock(nullptr);
  }
}

void AioImageRequestWQ::handle_blocked_writes(int r) {
  Contexts contexts;
  {
    RWLock::WLocker locker(m_lock);
    contexts.swap(m_write_blocker_contexts);
  }

  for (auto ctx : contexts) {
    ctx->complete(0);
  }
}

} // namespace librbd
