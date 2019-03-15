// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/io/ImageRequestWQ.h"
#include "common/errno.h"
#include "common/zipkin_trace.h"
#include "common/Cond.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/internal.h"
#include "librbd/Utils.h"
#include "librbd/exclusive_lock/Policy.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/io/ImageRequest.h"
#include "librbd/io/ImageDispatchSpec.h"
#include "common/EventTrace.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::io::ImageRequestWQ: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace io {

namespace {

template <typename I>
void flush_image(I& image_ctx, Context* on_finish) {
  auto aio_comp = librbd::io::AioCompletion::create(
    on_finish, util::get_image_ctx(&image_ctx), librbd::io::AIO_TYPE_FLUSH);
  auto req = librbd::io::ImageDispatchSpec<I>::create_flush_request(
    image_ctx, aio_comp, librbd::io::FLUSH_SOURCE_INTERNAL, {});
  req->send();
  delete req;
}

} // anonymous namespace

template <typename I>
struct ImageRequestWQ<I>::C_AcquireLock : public Context {
  ImageRequestWQ *work_queue;
  ImageDispatchSpec<I> *image_request;

  C_AcquireLock(ImageRequestWQ *work_queue, ImageDispatchSpec<I> *image_request)
    : work_queue(work_queue), image_request(image_request) {
  }

  void finish(int r) override {
    work_queue->handle_acquire_lock(r, image_request);
  }
};

template <typename I>
struct ImageRequestWQ<I>::C_BlockedWrites : public Context {
  ImageRequestWQ *work_queue;
  explicit C_BlockedWrites(ImageRequestWQ *_work_queue)
    : work_queue(_work_queue) {
  }

  void finish(int r) override {
    work_queue->handle_blocked_writes(r);
  }
};

template <typename I>
struct ImageRequestWQ<I>::C_RefreshFinish : public Context {
  ImageRequestWQ *work_queue;
  ImageDispatchSpec<I> *image_request;

  C_RefreshFinish(ImageRequestWQ *work_queue,
                  ImageDispatchSpec<I> *image_request)
    : work_queue(work_queue), image_request(image_request) {
  }
  void finish(int r) override {
    work_queue->handle_refreshed(r, image_request);
  }
};

static std::map<uint64_t, std::string> throttle_flags = {
  { RBD_QOS_IOPS_THROTTLE,       "rbd_qos_iops_throttle"       },
  { RBD_QOS_BPS_THROTTLE,        "rbd_qos_bps_throttle"        },
  { RBD_QOS_READ_IOPS_THROTTLE,  "rbd_qos_read_iops_throttle"  },
  { RBD_QOS_WRITE_IOPS_THROTTLE, "rbd_qos_write_iops_throttle" },
  { RBD_QOS_READ_BPS_THROTTLE,   "rbd_qos_read_bps_throttle"   },
  { RBD_QOS_WRITE_BPS_THROTTLE,  "rbd_qos_write_bps_throttle"  }
};

template <typename I>
ImageRequestWQ<I>::ImageRequestWQ(I *image_ctx, const string &name,
				  time_t ti, ThreadPool *tp)
  : ThreadPool::PointerWQ<ImageDispatchSpec<I> >(name, ti, 0, tp),
    m_image_ctx(*image_ctx),
    m_lock(util::unique_lock_name("ImageRequestWQ<I>::m_lock", this)) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << "ictx=" << image_ctx << dendl;

  SafeTimer *timer;
  Mutex *timer_lock;
  ImageCtx::get_timer_instance(cct, &timer, &timer_lock);

  for (auto flag : throttle_flags) {
    m_throttles.push_back(make_pair(
      flag.first,
      new TokenBucketThrottle(cct, flag.second, 0, 0, timer, timer_lock)));
  }

  this->register_work_queue();
}

template <typename I>
ImageRequestWQ<I>::~ImageRequestWQ() {
  for (auto t : m_throttles) {
    delete t.second;
  }
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
				   uint32_t discard_granularity_bytes) {
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
  aio_discard(c, off, len, discard_granularity_bytes, false);

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
ssize_t ImageRequestWQ<I>::compare_and_write(uint64_t off, uint64_t len,
                                             bufferlist &&cmp_bl,
                                             bufferlist &&bl,
                                             uint64_t *mismatch_off,
                                             int op_flags){
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "compare_and_write ictx=" << &m_image_ctx << ", off="
                 << off << ", " << "len = " << len << dendl;

  m_image_ctx.snap_lock.get_read();
  int r = clip_io(util::get_image_ctx(&m_image_ctx), off, &len);
  m_image_ctx.snap_lock.put_read();
  if (r < 0) {
    lderr(cct) << "invalid IO request: " << cpp_strerror(r) << dendl;
    return r;
  }

  C_SaferCond cond;
  AioCompletion *c = AioCompletion::create(&cond);
  aio_compare_and_write(c, off, len, std::move(cmp_bl), std::move(bl),
                        mismatch_off, op_flags, false);

  r = cond.wait();
  if (r < 0) {
    return r;
  }

  return len;
}

template <typename I>
int ImageRequestWQ<I>::flush() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "ictx=" << &m_image_ctx << dendl;

  C_SaferCond cond;
  AioCompletion *c = AioCompletion::create(&cond);
  aio_flush(c, false);

  int r = cond.wait();
  if (r < 0) {
    return r;
  }

  return 0;
}

template <typename I>
void ImageRequestWQ<I>::aio_read(AioCompletion *c, uint64_t off, uint64_t len,
				 ReadResult &&read_result, int op_flags,
				 bool native_async) {
  CephContext *cct = m_image_ctx.cct;
  FUNCTRACE(cct);
  ZTracer::Trace trace;
  if (m_image_ctx.blkin_trace_all) {
    trace.init("wq: read", &m_image_ctx.trace_endpoint);
    trace.event("start");
  }

  c->init_time(util::get_image_ctx(&m_image_ctx), AIO_TYPE_READ);
  ldout(cct, 20) << "ictx=" << &m_image_ctx << ", "
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
    queue(ImageDispatchSpec<I>::create_read_request(
            m_image_ctx, c, {{off, len}}, std::move(read_result), op_flags,
            trace));
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
  FUNCTRACE(cct);
  ZTracer::Trace trace;
  if (m_image_ctx.blkin_trace_all) {
    trace.init("wq: write", &m_image_ctx.trace_endpoint);
    trace.event("init");
  }

  c->init_time(util::get_image_ctx(&m_image_ctx), AIO_TYPE_WRITE);
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
    queue(ImageDispatchSpec<I>::create_write_request(
            m_image_ctx, c, {{off, len}}, std::move(bl), op_flags, trace));
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
				    uint64_t len,
                                    uint32_t discard_granularity_bytes,
				    bool native_async) {
  CephContext *cct = m_image_ctx.cct;
  FUNCTRACE(cct);
  ZTracer::Trace trace;
  if (m_image_ctx.blkin_trace_all) {
    trace.init("wq: discard", &m_image_ctx.trace_endpoint);
    trace.event("init");
  }

  c->init_time(util::get_image_ctx(&m_image_ctx), AIO_TYPE_DISCARD);
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
    queue(ImageDispatchSpec<I>::create_discard_request(
            m_image_ctx, c, off, len, discard_granularity_bytes, trace));
  } else {
    c->start_op();
    ImageRequest<I>::aio_discard(&m_image_ctx, c, {{off, len}},
                                 discard_granularity_bytes, trace);
    finish_in_flight_io();
  }
  trace.event("finish");
}

template <typename I>
void ImageRequestWQ<I>::aio_flush(AioCompletion *c, bool native_async) {
  CephContext *cct = m_image_ctx.cct;
  FUNCTRACE(cct);
  ZTracer::Trace trace;
  if (m_image_ctx.blkin_trace_all) {
    trace.init("wq: flush", &m_image_ctx.trace_endpoint);
    trace.event("init");
  }

  c->init_time(util::get_image_ctx(&m_image_ctx), AIO_TYPE_FLUSH);
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
    queue(ImageDispatchSpec<I>::create_flush_request(
            m_image_ctx, c, FLUSH_SOURCE_USER, trace));
  } else {
    ImageRequest<I>::aio_flush(&m_image_ctx, c, FLUSH_SOURCE_USER, trace);
    finish_in_flight_io();
  }
  trace.event("finish");
}

template <typename I>
void ImageRequestWQ<I>::aio_writesame(AioCompletion *c, uint64_t off,
				      uint64_t len, bufferlist &&bl,
				      int op_flags, bool native_async) {
  CephContext *cct = m_image_ctx.cct;
  FUNCTRACE(cct);
  ZTracer::Trace trace;
  if (m_image_ctx.blkin_trace_all) {
    trace.init("wq: writesame", &m_image_ctx.trace_endpoint);
    trace.event("init");
  }

  c->init_time(util::get_image_ctx(&m_image_ctx), AIO_TYPE_WRITESAME);
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
    queue(ImageDispatchSpec<I>::create_write_same_request(
            m_image_ctx, c, off, len, std::move(bl), op_flags, trace));
  } else {
    c->start_op();
    ImageRequest<I>::aio_writesame(&m_image_ctx, c, {{off, len}}, std::move(bl),
				   op_flags, trace);
    finish_in_flight_io();
  }
  trace.event("finish");
}

template <typename I>
void ImageRequestWQ<I>::aio_compare_and_write(AioCompletion *c,
                                              uint64_t off, uint64_t len,
                                              bufferlist &&cmp_bl,
                                              bufferlist &&bl,
                                              uint64_t *mismatch_off,
                                              int op_flags, bool native_async) {
  CephContext *cct = m_image_ctx.cct;
  FUNCTRACE(cct);
  ZTracer::Trace trace;
  if (m_image_ctx.blkin_trace_all) {
    trace.init("wq: compare_and_write", &m_image_ctx.trace_endpoint);
    trace.event("init");
  }

  c->init_time(util::get_image_ctx(&m_image_ctx), AIO_TYPE_COMPARE_AND_WRITE);
  ldout(cct, 20) << "ictx=" << &m_image_ctx << ", "
                 << "completion=" << c << ", off=" << off << ", "
                 << "len=" << len << dendl;

  if (native_async && m_image_ctx.event_socket.is_valid()) {
    c->set_event_notify(true);
  }

  if (!start_in_flight_io(c)) {
    return;
  }

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.non_blocking_aio || writes_blocked()) {
    queue(ImageDispatchSpec<I>::create_compare_and_write_request(
            m_image_ctx, c, {{off, len}}, std::move(cmp_bl), std::move(bl),
            mismatch_off, op_flags, trace));
  } else {
    c->start_op();
    ImageRequest<I>::aio_compare_and_write(&m_image_ctx, c, {{off, len}},
                                           std::move(cmp_bl), std::move(bl),
                                           mismatch_off, op_flags, trace);
    finish_in_flight_io();
  }
  trace.event("finish");
}

template <typename I>
void ImageRequestWQ<I>::shut_down(Context *on_shutdown) {
  ceph_assert(m_image_ctx.owner_lock.is_locked());

  {
    RWLock::WLocker locker(m_lock);
    ceph_assert(!m_shutdown);
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
  flush_image(m_image_ctx, on_shutdown);
}

template <typename I>
int ImageRequestWQ<I>::block_writes() {
  C_SaferCond cond_ctx;
  block_writes(&cond_ctx);
  return cond_ctx.wait();
}

template <typename I>
void ImageRequestWQ<I>::block_writes(Context *on_blocked) {
  ceph_assert(m_image_ctx.owner_lock.is_locked());
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
  flush_image(m_image_ctx, on_blocked);
}

template <typename I>
void ImageRequestWQ<I>::unblock_writes() {
  CephContext *cct = m_image_ctx.cct;

  bool wake_up = false;
  Contexts waiter_contexts;
  {
    RWLock::WLocker locker(m_lock);
    ceph_assert(m_write_blockers > 0);
    --m_write_blockers;

    ldout(cct, 5) << &m_image_ctx << ", " << "num="
                  << m_write_blockers << dendl;
    if (m_write_blockers == 0) {
      wake_up = true;
      std::swap(waiter_contexts, m_unblocked_write_waiter_contexts);
    }
  }

  if (wake_up) {
    for (auto ctx : waiter_contexts) {
      ctx->complete(0);
    }
    this->signal();
  }
}

template <typename I>
void ImageRequestWQ<I>::wait_on_writes_unblocked(Context *on_unblocked) {
  ceph_assert(m_image_ctx.owner_lock.is_locked());
  CephContext *cct = m_image_ctx.cct;

  {
    RWLock::WLocker locker(m_lock);
    ldout(cct, 20) << &m_image_ctx << ", " << "write_blockers="
                   << m_write_blockers << dendl;
    if (!m_unblocked_write_waiter_contexts.empty() || m_write_blockers > 0) {
      m_unblocked_write_waiter_contexts.push_back(on_unblocked);
      return;
    }
  }

  on_unblocked->complete(0);
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
void ImageRequestWQ<I>::apply_qos_schedule_tick_min(uint64_t tick){
  for (auto pair : m_throttles) {
    pair.second->set_schedule_tick_min(tick);
  }
}

template <typename I>
void ImageRequestWQ<I>::apply_qos_limit(const uint64_t flag,
                                        uint64_t limit,
                                        uint64_t burst) {
  CephContext *cct = m_image_ctx.cct;
  TokenBucketThrottle *throttle = nullptr;
  for (auto pair : m_throttles) {
    if (flag == pair.first) {
      throttle = pair.second;
      break;
    }
  }
  ceph_assert(throttle != nullptr);

  int r = throttle->set_limit(limit, burst);
  if (r < 0) {
    lderr(cct) << throttle->get_name() << ": invalid qos parameter: "
               << "burst(" << burst << ") is less than "
               << "limit(" << limit << ")" << dendl;
    // if apply failed, we should at least make sure the limit works.
    throttle->set_limit(limit, 0);
  }

  if (limit)
    m_qos_enabled_flag |= flag;
  else
    m_qos_enabled_flag &= ~flag;
}

template <typename I>
void ImageRequestWQ<I>::handle_throttle_ready(int r, ImageDispatchSpec<I> *item, uint64_t flag) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 15) << "r=" << r << ", " << "req=" << item << dendl;

  ceph_assert(m_io_throttled.load() > 0);
  item->set_throttled(flag);
  if (item->were_all_throttled()) {
    this->requeue_back(item);
    --m_io_throttled;
    this->signal();
  }
}

template <typename I>
bool ImageRequestWQ<I>::needs_throttle(ImageDispatchSpec<I> *item) { 
  uint64_t tokens = 0;
  uint64_t flag = 0;
  bool blocked = false;
  TokenBucketThrottle* throttle = nullptr;

  for (auto t : m_throttles) {
    flag = t.first;
    if (item->was_throttled(flag))
      continue;

    if (!(m_qos_enabled_flag & flag)) {
      item->set_throttled(flag);
      continue;
    }

    throttle = t.second;
    if (item->tokens_requested(flag, &tokens) &&
        throttle->get<ImageRequestWQ<I>, ImageDispatchSpec<I>,
	      &ImageRequestWQ<I>::handle_throttle_ready>(
		tokens, this, item, flag)) {
      blocked = true;
    } else {
      item->set_throttled(flag);
    }
  }
  return blocked;
}

template <typename I>
void *ImageRequestWQ<I>::_void_dequeue() {
  CephContext *cct = m_image_ctx.cct;
  ImageDispatchSpec<I> *peek_item = this->front();

  // no queued IO requests or all IO is blocked/stalled
  if (peek_item == nullptr || m_io_blockers.load() > 0) {
    return nullptr;
  }

  if (needs_throttle(peek_item)) {
    ldout(cct, 15) << "throttling IO " << peek_item << dendl;

    ++m_io_throttled;
    // dequeue the throttled item
    ThreadPool::PointerWQ<ImageDispatchSpec<I> >::_void_dequeue();
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
        m_in_flight_writes++;
      }
    }
  }

  auto item = reinterpret_cast<ImageDispatchSpec<I> *>(
    ThreadPool::PointerWQ<ImageDispatchSpec<I> >::_void_dequeue());
  ceph_assert(peek_item == item);

  if (lock_required) {
    this->get_pool_lock().unlock();
    m_image_ctx.owner_lock.get_read();
    if (m_image_ctx.exclusive_lock != nullptr) {
      ldout(cct, 5) << "exclusive lock required: delaying IO " << item << dendl;
      if (!m_image_ctx.get_exclusive_lock_policy()->may_auto_request_lock()) {
        lderr(cct) << "op requires exclusive lock" << dendl;
        fail_in_flight_io(m_image_ctx.exclusive_lock->get_unlocked_op_error(),
                          item);

        // wake up the IO since we won't be returning a request to process
        this->signal();
      } else {
        // stall IO until the acquire completes
        ++m_io_blockers;
        m_image_ctx.exclusive_lock->acquire_lock(new C_AcquireLock(this, item));
      }
    } else {
      // raced with the exclusive lock being disabled
      lock_required = false;
    }
    m_image_ctx.owner_lock.put_read();
    this->get_pool_lock().lock();

    if (lock_required) {
      return nullptr;
    }
  }

  if (refresh_required) {
    ldout(cct, 5) << "image refresh required: delaying IO " << item << dendl;

    // stall IO until the refresh completes
    ++m_io_blockers;

    this->get_pool_lock().unlock();
    m_image_ctx.state->refresh(new C_RefreshFinish(this, item));
    this->get_pool_lock().lock();
    return nullptr;
  }

  item->start_op();
  return item;
}

template <typename I>
void ImageRequestWQ<I>::process(ImageDispatchSpec<I> *req) {
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
void ImageRequestWQ<I>::finish_queued_io(ImageDispatchSpec<I> *req) {
  RWLock::RLocker locker(m_lock);
  if (req->is_write_op()) {
    ceph_assert(m_queued_writes > 0);
    m_queued_writes--;
  } else {
    ceph_assert(m_queued_reads > 0);
    m_queued_reads--;
  }
}

template <typename I>
void ImageRequestWQ<I>::finish_in_flight_write() {
  bool writes_blocked = false;
  {
    RWLock::RLocker locker(m_lock);
    ceph_assert(m_in_flight_writes > 0);
    if (--m_in_flight_writes == 0 &&
        !m_write_blocker_contexts.empty()) {
      writes_blocked = true;
    }
  }

  if (writes_blocked) {
    flush_image(m_image_ctx, new C_BlockedWrites(this));
  }
}

template <typename I>
int ImageRequestWQ<I>::start_in_flight_io(AioCompletion *c) {
  RWLock::RLocker locker(m_lock);

  if (m_shutdown) {
    CephContext *cct = m_image_ctx.cct;
    lderr(cct) << "IO received on closed image" << dendl;

    c->get();
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

  ceph_assert(on_shutdown != nullptr);
  flush_image(m_image_ctx, on_shutdown);
}

template <typename I>
void ImageRequestWQ<I>::fail_in_flight_io(
    int r, ImageDispatchSpec<I> *req) {
  this->process_finish();
  req->fail(r);
  finish_queued_io(req);
  delete req;
  finish_in_flight_io();
}

template <typename I>
bool ImageRequestWQ<I>::is_lock_required(bool write_op) const {
  ceph_assert(m_lock.is_locked());
  return ((write_op && m_require_lock_on_write) ||
          (!write_op && m_require_lock_on_read));
}

template <typename I>
void ImageRequestWQ<I>::queue(ImageDispatchSpec<I> *req) {
  ceph_assert(m_image_ctx.owner_lock.is_locked());

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "ictx=" << &m_image_ctx << ", "
                 << "req=" << req << dendl;

  if (req->is_write_op()) {
    m_queued_writes++;
  } else {
    m_queued_reads++;
  }

  ThreadPool::PointerWQ<ImageDispatchSpec<I> >::queue(req);
}

template <typename I>
void ImageRequestWQ<I>::handle_acquire_lock(
    int r, ImageDispatchSpec<I> *req) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << "r=" << r << ", " << "req=" << req << dendl;

  if (r < 0) {
    fail_in_flight_io(r, req);
  } else {
    // since IO was stalled for acquire -- original IO order is preserved
    // if we requeue this op for work queue processing
    this->requeue_front(req);
  }

  ceph_assert(m_io_blockers.load() > 0);
  --m_io_blockers;
  this->signal();
}

template <typename I>
void ImageRequestWQ<I>::handle_refreshed(
    int r, ImageDispatchSpec<I> *req) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 5) << "resuming IO after image refresh: r=" << r << ", "
                << "req=" << req << dendl;
  if (r < 0) {
    fail_in_flight_io(r, req);
  } else {
    // since IO was stalled for refresh -- original IO order is preserved
    // if we requeue this op for work queue processing
    this->requeue_front(req);
  }

  ceph_assert(m_io_blockers.load() > 0);
  --m_io_blockers;
  this->signal();
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
