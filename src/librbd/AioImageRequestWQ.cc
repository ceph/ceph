// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/AioImageRequestWQ.h"
#include "common/errno.h"
#include "librbd/AioCompletion.h"
#include "librbd/AioImageRequest.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/exclusive_lock/Policy.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::AioImageRequestWQ: "

namespace librbd {

template <typename I>
struct AioImageRequestWQ<I>::C_AcquireLock : public Context {
  AioImageRequestWQ *work_queue;
  AioImageRequest<I> *image_request;

  C_AcquireLock(AioImageRequestWQ *work_queue, AioImageRequest<I> *image_request)
    : work_queue(work_queue), image_request(image_request) {
  }

  void finish(int r) override {
    work_queue->handle_acquire_lock(r, image_request);
  }
};

template <typename I>
struct AioImageRequestWQ<I>::C_BlockedWrites : public Context {
  AioImageRequestWQ *work_queue;
  C_BlockedWrites(AioImageRequestWQ *_work_queue)
    : work_queue(_work_queue) {
  }

  void finish(int r) override {
    work_queue->handle_blocked_writes(r);
  }
};

template <typename I>
struct AioImageRequestWQ<I>::C_RefreshFinish : public Context {
  AioImageRequestWQ *work_queue;
  AioImageRequest<I> *image_request;

  C_RefreshFinish(AioImageRequestWQ *work_queue,
                  AioImageRequest<I> *image_request)
    : work_queue(work_queue), image_request(image_request) {
  }
  void finish(int r) override {
    work_queue->handle_refreshed(r, image_request);
  }
};

template <typename I>
AioImageRequestWQ<I>::AioImageRequestWQ(I *image_ctx, const string &name,
				        time_t ti, ThreadPool *tp)
  : ThreadPool::PointerWQ<AioImageRequest<I> >(name, ti, 0, tp),
    m_image_ctx(*image_ctx),
    m_lock(util::unique_lock_name("AioImageRequestWQ::m_lock", this)) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << this << " " << ": ictx=" << image_ctx << dendl;
  this->register_work_queue();
}

template <typename I>
ssize_t AioImageRequestWQ<I>::read(uint64_t off, uint64_t len, char *buf,
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

template <typename I>
ssize_t AioImageRequestWQ<I>::write(uint64_t off, uint64_t len, const char *buf,
				    int op_flags) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "write: ictx=" << &m_image_ctx << ", off=" << off << ", "
                 << "len = " << len << dendl;

  m_image_ctx.snap_lock.get_read();
  int r = clip_io(util::get_image_ctx(&m_image_ctx), off, &len);
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

template <typename I>
int AioImageRequestWQ<I>::discard(uint64_t off, uint64_t len) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "discard: ictx=" << &m_image_ctx << ", off=" << off << ", "
                 << "len = " << len << dendl;

  m_image_ctx.snap_lock.get_read();
  int r = clip_io(util::get_image_ctx(&m_image_ctx), off, &len);
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

template <typename I>
void AioImageRequestWQ<I>::aio_read(AioCompletion *c, uint64_t off, uint64_t len,
				    char *buf, bufferlist *pbl, int op_flags,
				    bool native_async) {
  c->init_time(util::get_image_ctx(&m_image_ctx), librbd::AIO_TYPE_READ);
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "aio_read: ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << ", off=" << off << ", "
                 << "len=" << len << ", " << "flags=" << op_flags << dendl;

  if (native_async && m_image_ctx.event_socket.is_valid()) {
    c->set_event_notify(true);
  }

  if (!start_in_flight_io(c)) {
    return;
  }

  // if journaling is enabled -- we need to replay the journal because
  // it might contain an uncommitted write
  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.non_blocking_aio || writes_blocked() || !writes_empty() ||
      require_lock_on_read()) {
    queue(AioImageRequest<I>::create_read_request(m_image_ctx, c, off, len,
						  buf, pbl, op_flags));
  } else {
    c->start_op();
    AioImageRequest<I>::aio_read(&m_image_ctx, c, off, len, buf, pbl, op_flags);
    finish_in_flight_io();
  }
}

template <typename I>
void AioImageRequestWQ<I>::aio_write(AioCompletion *c, uint64_t off, uint64_t len,
				     const char *buf, int op_flags,
				     bool native_async) {
  c->init_time(util::get_image_ctx(&m_image_ctx), librbd::AIO_TYPE_WRITE);
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "aio_write: ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << ", off=" << off << ", "
                 << "len=" << len << ", flags=" << op_flags << dendl;

  if (native_async && m_image_ctx.event_socket.is_valid()) {
    c->set_event_notify(true);
  }

  if (!start_in_flight_io(c)) {
    return;
  }

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.non_blocking_aio || writes_blocked()) {
    queue(AioImageRequest<I>::create_write_request(m_image_ctx, c, off, len,
						   buf, op_flags));
  } else {
    c->start_op();
    AioImageRequest<I>::aio_write(&m_image_ctx, c, off, len, buf, op_flags);
    finish_in_flight_io();
  }
}

template <typename I>
void AioImageRequestWQ<I>::aio_discard(AioCompletion *c, uint64_t off,
				       uint64_t len, bool native_async) {
  c->init_time(util::get_image_ctx(&m_image_ctx), librbd::AIO_TYPE_DISCARD);
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "aio_discard: ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << ", off=" << off << ", len=" << len
                 << dendl;

  if (native_async && m_image_ctx.event_socket.is_valid()) {
    c->set_event_notify(true);
  }

  if (!start_in_flight_io(c)) {
    return;
  }

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.non_blocking_aio || writes_blocked()) {
    queue(AioImageRequest<I>::create_discard_request(m_image_ctx, c, off,
						     len));
  } else {
    c->start_op();
    AioImageRequest<I>::aio_discard(&m_image_ctx, c, off, len);
    finish_in_flight_io();
  }
}

template <typename I>
void AioImageRequestWQ<I>::aio_flush(AioCompletion *c, bool native_async) {
  c->init_time(util::get_image_ctx(&m_image_ctx), librbd::AIO_TYPE_FLUSH);
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "aio_flush: ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << dendl;

  if (native_async && m_image_ctx.event_socket.is_valid()) {
    c->set_event_notify(true);
  }

  if (!start_in_flight_io(c)) {
    return;
  }

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.non_blocking_aio || writes_blocked() || !writes_empty()) {
    queue(AioImageRequest<I>::create_flush_request(m_image_ctx, c));
  } else {
    AioImageRequest<I>::aio_flush(&m_image_ctx, c);
    finish_in_flight_io();
  }
}

template <typename I>
void AioImageRequestWQ<I>::shut_down(Context *on_shutdown) {
  assert(m_image_ctx.owner_lock.is_locked());

  {
    RWLock::WLocker locker(m_lock);
    assert(!m_shutdown);
    m_shutdown = true;

    CephContext *cct = m_image_ctx.cct;
    ldout(cct, 5) << __func__ << ": in_flight=" << m_in_flight_ios.read()
                  << dendl;
    if (m_in_flight_ios.read() > 0) {
      m_on_shutdown = on_shutdown;
      return;
    }
  }

  // ensure that all in-flight IO is flushed
  m_image_ctx.flush(on_shutdown);
}

template <typename I>
int AioImageRequestWQ<I>::block_writes() {
  C_SaferCond cond_ctx;
  block_writes(&cond_ctx);
  return cond_ctx.wait();
}

template <typename I>
void AioImageRequestWQ<I>::block_writes(Context *on_blocked) {
  assert(m_image_ctx.owner_lock.is_locked());
  CephContext *cct = m_image_ctx.cct;

  {
    RWLock::WLocker locker(m_lock);
    ++m_write_blockers;
    ldout(cct, 5) << __func__ << ": " << &m_image_ctx << ", "
                  << "num=" << m_write_blockers << dendl;
    if (!m_write_blocker_contexts.empty() || m_in_flight_writes.read() > 0) {
      m_write_blocker_contexts.push_back(on_blocked);
      return;
    }
  }

  // ensure that all in-flight IO is flushed
  m_image_ctx.flush(on_blocked);
}

template <typename I>
void AioImageRequestWQ<I>::unblock_writes() {
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
    this->signal();
  }
}

template <typename I>
void AioImageRequestWQ<I>::set_require_lock(AioDirection aio_direction,
					    bool enabled) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  bool wake_up = false;
  {
    switch (aio_direction) {
    case AIO_DIRECTION_READ:
      wake_up = (enabled != m_require_lock_on_read);
      m_require_lock_on_read = enabled;
      break;
    case AIO_DIRECTION_WRITE:
      wake_up = (enabled != m_require_lock_on_write);
      m_require_lock_on_write = enabled;
      break;
    case AIO_DIRECTION_BOTH:
      wake_up = (enabled != m_require_lock_on_read ||
                 enabled != m_require_lock_on_write);
      m_require_lock_on_read = enabled;
      m_require_lock_on_write = enabled;
      break;
    }
  }

  // wake up the thread pool whenever the state changes so that
  // we can re-request the lock if required
  if (wake_up) {
    this->signal();
  }
}

template <typename I>
void *AioImageRequestWQ<I>::_void_dequeue() {
  CephContext *cct = m_image_ctx.cct;
  AioImageRequest<I> *peek_item = this->front();

  // no queued IO requests or all IO is blocked/stalled
  if (peek_item == nullptr || m_io_blockers.read() > 0) {
    return nullptr;
  }

  bool lock_required;
  bool refresh_required = m_image_ctx.state->is_refresh_required();
  {
    RWLock::RLocker locker(m_lock);
    bool write_op = peek_item->is_write_op();
    lock_required = is_lock_required(write_op);
    if (write_op) {
      if (!lock_required && m_write_blockers > 0) {
        // missing lock is not the write blocker
        return nullptr;
      }

      if (!lock_required && !refresh_required) {
        // completed ops will requeue the IO -- don't count it as in-progress
        m_in_flight_writes.inc();
      }
    }
  }

  AioImageRequest<I> *item = reinterpret_cast<AioImageRequest<I> *>(
    ThreadPool::PointerWQ<AioImageRequest<I> >::_void_dequeue());
  assert(peek_item == item);

  if (lock_required) {
    this->get_pool_lock().Unlock();
    m_image_ctx.owner_lock.get_read();
    if (m_image_ctx.exclusive_lock != nullptr) {
      ldout(cct, 5) << "exclusive lock required: delaying IO " << item << dendl;
      if (!m_image_ctx.get_exclusive_lock_policy()->may_auto_request_lock()) {
        lderr(cct) << "op requires exclusive lock" << dendl;
        fail_in_flight_io(-EROFS, item);

        // wake up the IO since we won't be returning a request to process
        this->signal();
      } else {
        // stall IO until the acquire completes
        m_io_blockers.inc();
        m_image_ctx.exclusive_lock->request_lock(new C_AcquireLock(this, item));
      }
    } else {
      // raced with the exclusive lock being disabled
      lock_required = false;
    }
    m_image_ctx.owner_lock.put_read();
    this->get_pool_lock().Lock();

    if (lock_required) {
      return nullptr;
    }
  }

  if (refresh_required) {
    ldout(cct, 5) << "image refresh required: delaying IO " << item << dendl;

    // stall IO until the refresh completes
    m_io_blockers.inc();

    this->get_pool_lock().Unlock();
    m_image_ctx.state->refresh(new C_RefreshFinish(this, item));
    this->get_pool_lock().Lock();
    return nullptr;
  }

  item->start_op();
  return item;
}

template <typename I>
void AioImageRequestWQ<I>::process(AioImageRequest<I> *req) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << __func__ << ": ictx=" << &m_image_ctx << ", "
                 << "req=" << req << dendl;

  {
    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
    req->send();
  }

  finish_queued_io(req);
  if (req->is_write_op()) {
    finish_in_flight_write();
  }
  delete req;

  finish_in_flight_io();
}

template <typename I>
void AioImageRequestWQ<I>::finish_queued_io(AioImageRequest<I> *req) {
  RWLock::RLocker locker(m_lock);
  if (req->is_write_op()) {
    assert(m_queued_writes.read() > 0);
    m_queued_writes.dec();
  } else {
    assert(m_queued_reads.read() > 0);
    m_queued_reads.dec();
  }
}

template <typename I>
void AioImageRequestWQ<I>::finish_in_flight_write() {
  bool writes_blocked = false;
  {
    RWLock::RLocker locker(m_lock);
    assert(m_in_flight_writes.read() > 0);
    if (m_in_flight_writes.dec() == 0 &&
        !m_write_blocker_contexts.empty()) {
      writes_blocked = true;
    }
  }

  if (writes_blocked) {
    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
    m_image_ctx.flush(new C_BlockedWrites(this));
  }
}

template <typename I>
int AioImageRequestWQ<I>::start_in_flight_io(AioCompletion *c) {
  RWLock::RLocker locker(m_lock);

  if (m_shutdown) {
    CephContext *cct = m_image_ctx.cct;
    lderr(cct) << "IO received on closed image" << dendl;

    c->get();
    c->fail(-ESHUTDOWN);
    return false;
  }

  m_in_flight_ios.inc();
  return true;
}

template <typename I>
void AioImageRequestWQ<I>::finish_in_flight_io() {
  {
    RWLock::RLocker locker(m_lock);
    if (m_in_flight_ios.dec() > 0 || !m_shutdown) {
      return;
    }
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << __func__ << ": completing shut down" << dendl;

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  assert(m_on_shutdown != nullptr);
  m_image_ctx.flush(m_on_shutdown);
}

template <typename I>
void AioImageRequestWQ<I>::fail_in_flight_io(int r, AioImageRequest<I> *req) {
  this->process_finish();
  req->fail(r);
  finish_queued_io(req);
  delete req;
  finish_in_flight_io();
}

template <typename I>
bool AioImageRequestWQ<I>::is_lock_required(bool write_op) const {
  assert(m_lock.is_locked());
  return ((write_op && m_require_lock_on_write) ||
          (!write_op && m_require_lock_on_read));
}

template <typename I>
void AioImageRequestWQ<I>::queue(AioImageRequest<I> *req) {
  assert(m_image_ctx.owner_lock.is_locked());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << __func__ << ": ictx=" << &m_image_ctx << ", "
                 << "req=" << req << dendl;

  if (req->is_write_op()) {
    m_queued_writes.inc();
  } else {
    m_queued_reads.inc();
  }

  ThreadPool::PointerWQ<AioImageRequest<I> >::queue(req);
}

template <typename I>
void AioImageRequestWQ<I>::handle_acquire_lock(int r, AioImageRequest<I> *req) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << "r=" << r << ", " << "req=" << req << dendl;

  if (r < 0) {
    fail_in_flight_io(r, req);
  } else {
    // since IO was stalled for acquire -- original IO order is preserved
    // if we requeue this op for work queue processing
    this->requeue(req);
  }

  assert(m_io_blockers.read() > 0);
  m_io_blockers.dec();
  this->signal();
}

template <typename I>
void AioImageRequestWQ<I>::handle_refreshed(int r, AioImageRequest<I> *req) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << "resuming IO after image refresh: r=" << r << ", "
                << "req=" << req << dendl;
  if (r < 0) {
    fail_in_flight_io(r, req);
  } else {
    // since IO was stalled for refresh -- original IO order is preserved
    // if we requeue this op for work queue processing
    this->requeue(req);
  }

  assert(m_io_blockers.read() > 0);
  m_io_blockers.dec();
  this->signal();
}

template <typename I>
void AioImageRequestWQ<I>::handle_blocked_writes(int r) {
  Contexts contexts;
  {
    RWLock::WLocker locker(m_lock);
    contexts.swap(m_write_blocker_contexts);
  }

  for (auto ctx : contexts) {
    ctx->complete(0);
  }
}

template class librbd::AioImageRequestWQ<librbd::ImageCtx>;

} // namespace librbd
