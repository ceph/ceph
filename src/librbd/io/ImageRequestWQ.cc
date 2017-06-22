// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/ImageRequestWQ.h"
#include "common/errno.h"
#include "common/zipkin_trace.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Utils.h"
#include "librbd/exclusive_lock/Policy.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::ImageRequestWQ: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace io {

template <typename I>
ImageRequestWQ<I>::ImageRequestWQ(I *image_ctx, const string &name,
				  time_t ti, ThreadPool *tp)
  : ThreadPool::PointerWQ<ImageRequest<I> >(name, ti, 0, tp),
    m_image_ctx(*image_ctx),
    m_lock(util::unique_lock_name("ImageRequestWQ<I>::m_lock", this)) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << "ictx=" << image_ctx << dendl;
  tp->add_work_queue(this);
}

template <typename I>
ssize_t ImageRequestWQ<I>::read(uint64_t off, uint64_t len,
				ReadResult &&read_result, int op_flags) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "ictx=" << &m_image_ctx << ", off=" << off << ", "
                 << "len = " << len << dendl;

  C_SaferCond cond;
  AioCompletion *c = AioCompletion::create(&cond);
  aio_read(c, off, len, std::move(read_result), op_flags, false);
  return cond.wait();
}

template <typename I>
ssize_t ImageRequestWQ<I>::write(uint64_t off, uint64_t len,
				 bufferlist &&bl, int op_flags) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "ictx=" << &m_image_ctx << ", off=" << off << ", "
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
  aio_write(c, off, len, std::move(bl), op_flags, false);

  r = cond.wait();
  if (r < 0) {
    return r;
  }
  return len;
}

template <typename I>
ssize_t ImageRequestWQ<I>::discard(uint64_t off, uint64_t len,
				   bool skip_partial_discard) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "ictx=" << &m_image_ctx << ", off=" << off << ", "
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
  aio_discard(c, off, len, skip_partial_discard, false);

  r = cond.wait();
  if (r < 0) {
    return r;
  }
  return len;
}

template <typename I>
ssize_t ImageRequestWQ<I>::writesame(uint64_t off, uint64_t len,
				     bufferlist &&bl, int op_flags) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "ictx=" << &m_image_ctx << ", off=" << off << ", "
                 << "len = " << len << ", data_len " << bl.length() << dendl;

  m_image_ctx.snap_lock.get_read();
  int r = clip_io(util::get_image_ctx(&m_image_ctx), off, &len);
  m_image_ctx.snap_lock.put_read();
  if (r < 0) {
    lderr(cct) << "invalid IO request: " << cpp_strerror(r) << dendl;
    return r;
  }

  C_SaferCond cond;
  AioCompletion *c = AioCompletion::create(&cond);
  aio_writesame(c, off, len, std::move(bl), op_flags, false);

  r = cond.wait();
  if (r < 0) {
    return r;
  }
  return len;
}

template <typename I>
void ImageRequestWQ<I>::aio_read(AioCompletion *c, uint64_t off, uint64_t len,
				 ReadResult &&read_result, int op_flags,
				 bool native_async) {
  CephContext *cct = m_image_ctx.cct;
  ZTracer::Trace trace;
  if (cct->_conf->rbd_blkin_trace_all) {
    trace.init("wq: read", &m_image_ctx.trace_endpoint);
    trace.event("start");
  }

  c->init_time(&m_image_ctx, AIO_TYPE_READ);
  ldout(cct, 20) << "ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << ", off=" << off << ", "
                 << "len=" << len << ", " << "flags=" << op_flags << dendl;

  if (native_async && m_image_ctx.event_socket.is_valid()) {
    c->set_event_notify(true);
  }

  if (!start_in_flight_io(c)) {
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
    queue(new ImageReadRequest<I>(m_image_ctx, c, {{off, len}},
				  std::move(read_result), op_flags, trace));
  } else {
    c->start_op();
    ImageRequest<I>::aio_read(&m_image_ctx, c, {{off, len}},
			      std::move(read_result), op_flags, trace);
    finish_in_flight_io();
  }
  trace.event("finish");
}

template <typename I>
void ImageRequestWQ<I>::aio_write(AioCompletion *c, uint64_t off, uint64_t len,
				  bufferlist &&bl, int op_flags,
				  bool native_async) {
  CephContext *cct = m_image_ctx.cct;
  ZTracer::Trace trace;
  if (cct->_conf->rbd_blkin_trace_all) {
    trace.init("wq: write", &m_image_ctx.trace_endpoint);
    trace.event("init");
  }

  c->init_time(&m_image_ctx, AIO_TYPE_WRITE);
  ldout(cct, 20) << "ictx=" << &m_image_ctx << ", "
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
    queue(new ImageWriteRequest<I>(m_image_ctx, c, {{off, len}},
				   std::move(bl), op_flags, trace));
  } else {
    c->start_op();
    ImageRequest<I>::aio_write(&m_image_ctx, c, {{off, len}},
			       std::move(bl), op_flags, trace);
    finish_in_flight_io();
  }
  trace.event("finish");
}

template <typename I>
void ImageRequestWQ<I>::aio_discard(AioCompletion *c, uint64_t off,
				    uint64_t len, bool skip_partial_discard,
				    bool native_async) {
  CephContext *cct = m_image_ctx.cct;
  ZTracer::Trace trace;
  if (cct->_conf->rbd_blkin_trace_all) {
    trace.init("wq: discard", &m_image_ctx.trace_endpoint);
    trace.event("init");
  }

  c->init_time(&m_image_ctx, AIO_TYPE_DISCARD);
  ldout(cct, 20) << "ictx=" << &m_image_ctx << ", "
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
    queue(new ImageDiscardRequest<I>(m_image_ctx, c, off, len,
				     skip_partial_discard, trace));
  } else {
    c->start_op();
    ImageRequest<I>::aio_discard(&m_image_ctx, c, off, len,
				 skip_partial_discard, trace);
    finish_in_flight_io();
  }
  trace.event("finish");
}

template <typename I>
void ImageRequestWQ<I>::aio_flush(AioCompletion *c, bool native_async) {
  CephContext *cct = m_image_ctx.cct;
  ZTracer::Trace trace;
  if (cct->_conf->rbd_blkin_trace_all) {
    trace.init("wq: flush", &m_image_ctx.trace_endpoint);
    trace.event("init");
  }

  c->init_time(&m_image_ctx, AIO_TYPE_FLUSH);
  ldout(cct, 20) << "ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << dendl;

  if (native_async && m_image_ctx.event_socket.is_valid()) {
    c->set_event_notify(true);
  }

  if (!start_in_flight_io(c)) {
    return;
  }

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.non_blocking_aio || writes_blocked() || !writes_empty()) {
    queue(new ImageFlushRequest<I>(m_image_ctx, c, trace));
  } else {
    ImageRequest<I>::aio_flush(&m_image_ctx, c, trace);
    finish_in_flight_io();
  }
  trace.event("finish");
}

template <typename I>
void ImageRequestWQ<I>::aio_writesame(AioCompletion *c, uint64_t off,
				      uint64_t len, bufferlist &&bl,
				      int op_flags, bool native_async) {
  CephContext *cct = m_image_ctx.cct;
  ZTracer::Trace trace;
  if (cct->_conf->rbd_blkin_trace_all) {
    trace.init("wq: writesame", &m_image_ctx.trace_endpoint);
    trace.event("init");
  }

  c->init_time(&m_image_ctx, AIO_TYPE_WRITESAME);
  ldout(cct, 20) << "ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << ", off=" << off << ", "
                 << "len=" << len << ", data_len = " << bl.length() << ", "
                 << "flags=" << op_flags << dendl;

  if (native_async && m_image_ctx.event_socket.is_valid()) {
    c->set_event_notify(true);
  }

  if (!start_in_flight_io(c)) {
    return;
  }

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.non_blocking_aio || writes_blocked()) {
    queue(new ImageWriteSameRequest<I>(m_image_ctx, c, off, len, std::move(bl),
				       op_flags, trace));
  } else {
    c->start_op();
    ImageRequest<I>::aio_writesame(&m_image_ctx, c, off, len, std::move(bl),
				   op_flags, trace);
    finish_in_flight_io();
  }
  trace.event("finish");
}

template <typename I>
void ImageRequestWQ<I>::shut_down(Context *on_shutdown) {
  assert(m_image_ctx.owner_lock.is_locked());

  {
    RWLock::WLocker locker(m_lock);
    assert(!m_shutdown);
    m_shutdown = true;

    CephContext *cct = m_image_ctx.cct;
    ldout(cct, 5) << __func__ << ": in_flight=" << m_in_flight_ios.load()
                  << dendl;
    if (m_in_flight_ios > 0) {
      m_on_shutdown = on_shutdown;
      return;
    }
  }

  // ensure that all in-flight IO is flushed
  m_image_ctx.flush(on_shutdown);
}

template <typename I>
bool ImageRequestWQ<I>::is_lock_request_needed() const {
  RWLock::RLocker locker(m_lock);
  return (m_queued_writes > 0 ||
          (m_require_lock_on_read && m_queued_reads > 0));
}

template <typename I>
int ImageRequestWQ<I>::block_writes() {
  C_SaferCond cond_ctx;
  block_writes(&cond_ctx);
  return cond_ctx.wait();
}

template <typename I>
void ImageRequestWQ<I>::block_writes(Context *on_blocked) {
  assert(m_image_ctx.owner_lock.is_locked());
  CephContext *cct = m_image_ctx.cct;

  {
    RWLock::WLocker locker(m_lock);
    ++m_write_blockers;
    ldout(cct, 5) << &m_image_ctx << ", " << "num="
                  << m_write_blockers << dendl;
    if (!m_write_blocker_contexts.empty() || m_in_flight_writes > 0) {
      m_write_blocker_contexts.push_back(on_blocked);
      return;
    }
  }

  // ensure that all in-flight IO is flushed
  m_image_ctx.flush(on_blocked);
}

template <typename I>
void ImageRequestWQ<I>::unblock_writes() {
  CephContext *cct = m_image_ctx.cct;

  bool wake_up = false;
  {
    RWLock::WLocker locker(m_lock);
    assert(m_write_blockers > 0);
    --m_write_blockers;

    ldout(cct, 5) << &m_image_ctx << ", " << "num="
                  << m_write_blockers << dendl;
    if (m_write_blockers == 0) {
      wake_up = true;
    }
  }

  if (wake_up) {
    this->signal();
  }
}

template <typename I>
void ImageRequestWQ<I>::set_require_lock(Direction direction, bool enabled) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  bool wake_up = false;
  {
    RWLock::WLocker locker(m_lock);
    switch (direction) {
    case DIRECTION_READ:
      wake_up = (enabled != m_require_lock_on_read);
      m_require_lock_on_read = enabled;
      break;
    case DIRECTION_WRITE:
      wake_up = (enabled != m_require_lock_on_write);
      m_require_lock_on_write = enabled;
      break;
    case DIRECTION_BOTH:
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
void *ImageRequestWQ<I>::_void_dequeue() {
  ImageRequest<I> *peek_item = this->front();

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
        m_in_flight_writes++;
      }
    } else if (m_require_lock_on_read) {
      return nullptr;
    }
  }

  ImageRequest<I> *item = reinterpret_cast<ImageRequest<I> *>(
    ThreadPool::PointerWQ<ImageRequest<I> >::_void_dequeue());
  assert(peek_item == item);

  if (refresh_required) {
    ldout(m_image_ctx.cct, 15) << "image refresh required: delaying IO " << item
                               << dendl;

    // stall IO until the refresh completes
    m_refresh_in_progress = true;

    this->get_pool_lock().Unlock();
    m_image_ctx.state->refresh(new C_RefreshFinish(this, item));
    this->get_pool_lock().Lock();
    return nullptr;
  }

  item->start_op();
  return item;
}

template <typename I>
void ImageRequestWQ<I>::process(ImageRequest<I> *req) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "ictx=" << &m_image_ctx << ", "
                 << "req=" << req << dendl;

  req->send();

  finish_queued_io(req);
  if (req->is_write_op()) {
    finish_in_flight_write();
  }
  delete req;

  finish_in_flight_io();
}

template <typename I>
void ImageRequestWQ<I>::finish_queued_io(ImageRequest<I> *req) {
  RWLock::RLocker locker(m_lock);
  if (req->is_write_op()) {
    assert(m_queued_writes > 0);
    m_queued_writes--;
  } else {
    assert(m_queued_reads > 0);
    m_queued_reads--;
  }
}

template <typename I>
void ImageRequestWQ<I>::finish_in_flight_write() {
  bool writes_blocked = false;
  {
    RWLock::RLocker locker(m_lock);
    assert(m_in_flight_writes > 0);
    if (--m_in_flight_writes == 0 &&
        !m_write_blocker_contexts.empty()) {
      writes_blocked = true;
    }
  }

  if (writes_blocked) {
    m_image_ctx.flush(new C_BlockedWrites(this));
  }
}

template <typename I>
int ImageRequestWQ<I>::start_in_flight_io(AioCompletion *c) {
  RWLock::RLocker locker(m_lock);

  if (m_shutdown) {
    CephContext *cct = m_image_ctx.cct;
    lderr(cct) << "IO received on closed image" << dendl;

    c->fail(-ESHUTDOWN);
    return false;
  }

  m_in_flight_ios++;
  return true;
}

template <typename I>
void ImageRequestWQ<I>::finish_in_flight_io() {
  Context *on_shutdown;
  {
    RWLock::RLocker locker(m_lock);
    if (--m_in_flight_ios > 0 || !m_shutdown) {
      return;
    }
    on_shutdown = m_on_shutdown;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << "completing shut down" << dendl;

  assert(on_shutdown != nullptr);
  m_image_ctx.flush(on_shutdown);
}

template <typename I>
bool ImageRequestWQ<I>::is_lock_required() const {
  assert(m_image_ctx.owner_lock.is_locked());
  if (m_image_ctx.exclusive_lock == NULL) {
    return false;
  }

  return (!m_image_ctx.exclusive_lock->is_lock_owner());
}

template <typename I>
void ImageRequestWQ<I>::queue(ImageRequest<I> *req) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "ictx=" << &m_image_ctx << ", "
                 << "req=" << req << dendl;

  assert(m_image_ctx.owner_lock.is_locked());
  bool write_op = req->is_write_op();
  bool lock_required = (m_image_ctx.exclusive_lock != nullptr &&
                        ((write_op && is_lock_required()) ||
                          (!write_op && m_require_lock_on_read)));

  if (lock_required && !m_image_ctx.get_exclusive_lock_policy()->may_auto_request_lock()) {
    lderr(cct) << "op requires exclusive lock" << dendl;
    req->fail(-EROFS);
    delete req;
    finish_in_flight_io();
    return;
  }

  if (write_op) {
    m_queued_writes++;
  } else {
    m_queued_reads++;
  }

  ThreadPool::PointerWQ<ImageRequest<I> >::queue(req);

  if (lock_required) {
    m_image_ctx.exclusive_lock->acquire_lock(nullptr);
  }
}

template <typename I>
void ImageRequestWQ<I>::handle_refreshed(int r, ImageRequest<I> *req) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 15) << "resuming IO after image refresh: r=" << r << ", "
                 << "req=" << req << dendl;
  if (r < 0) {
    this->process_finish();
    req->fail(r);
    finish_queued_io(req);
    delete req;
    finish_in_flight_io();
  } else {
    // since IO was stalled for refresh -- original IO order is preserved
    // if we requeue this op for work queue processing
    this->requeue(req);
  }

  m_refresh_in_progress = false;
  this->signal();

  // refresh might have enabled exclusive lock -- IO stalled until
  // we acquire the lock
  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (is_lock_required() && is_lock_request_needed()) {
    m_image_ctx.exclusive_lock->acquire_lock(nullptr);
  }
}

template <typename I>
void ImageRequestWQ<I>::handle_blocked_writes(int r) {
  Contexts contexts;
  {
    RWLock::WLocker locker(m_lock);
    contexts.swap(m_write_blocker_contexts);
  }

  for (auto ctx : contexts) {
    ctx->complete(0);
  }
}

template class librbd::io::ImageRequestWQ<librbd::ImageCtx>;

} // namespace io
} // namespace librbd
