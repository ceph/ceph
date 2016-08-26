// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/ImageWatcher.h"
#include "librbd/AioCompletion.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Operations.h"
#include "librbd/TaskFinisher.h"
#include "librbd/Utils.h"
#include "librbd/exclusive_lock/Policy.h"
#include "librbd/image_watcher/Notifier.h"
#include "librbd/image_watcher/NotifyLockOwner.h"
#include "librbd/image_watcher/RewatchRequest.h"
#include "include/encoding.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include <boost/bind.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ImageWatcher: "

namespace librbd {

using namespace image_watcher;
using namespace watch_notify;
using util::create_async_context_callback;
using util::create_context_callback;
using util::create_rados_safe_callback;

namespace {

struct C_UnwatchAndFlush : public Context {
  librados::Rados rados;
  Context *on_finish;
  bool flushing = false;
  int ret_val = 0;

  C_UnwatchAndFlush(librados::IoCtx &io_ctx, Context *on_finish)
    : rados(io_ctx), on_finish(on_finish) {
  }

  virtual void complete(int r) override {
    if (ret_val == 0 && r < 0) {
      ret_val = r;
    }

    if (!flushing) {
      flushing = true;

      librados::AioCompletion *aio_comp = create_rados_safe_callback(this);
      r = rados.aio_watch_flush(aio_comp);
      assert(r == 0);
      aio_comp->release();
    } else {
      Context::complete(ret_val);
    }
  }

  virtual void finish(int r) override {
    on_finish->complete(r);
  }
};

} // anonymous namespace

static const double	RETRY_DELAY_SECONDS = 1.0;

template <typename I>
ImageWatcher<I>::ImageWatcher(I &image_ctx)
  : m_image_ctx(image_ctx),
    m_watch_lock(util::unique_lock_name("librbd::ImageWatcher::m_watch_lock", this)),
    m_watch_ctx(*this), m_watch_handle(0),
    m_watch_state(WATCH_STATE_UNREGISTERED),
    m_task_finisher(new TaskFinisher<Task>(*m_image_ctx.cct)),
    m_async_request_lock(util::unique_lock_name("librbd::ImageWatcher::m_async_request_lock", this)),
    m_owner_client_id_lock(util::unique_lock_name("librbd::ImageWatcher::m_owner_client_id_lock", this)),
    m_notifier(image_ctx)
{
}

template <typename I>
ImageWatcher<I>::~ImageWatcher()
{
  delete m_task_finisher;
  {
    RWLock::RLocker l(m_watch_lock);
    assert(m_watch_state != WATCH_STATE_REGISTERED);
  }
}

template <typename I>
void ImageWatcher<I>::register_watch(Context *on_finish) {
  ldout(m_image_ctx.cct, 10) << this << " registering image watcher" << dendl;

  RWLock::RLocker watch_locker(m_watch_lock);
  assert(m_watch_state == WATCH_STATE_UNREGISTERED);
  librados::AioCompletion *aio_comp = create_rados_safe_callback(
    new C_RegisterWatch(this, on_finish));
  int r = m_image_ctx.md_ctx.aio_watch(m_image_ctx.header_oid, aio_comp,
                                       &m_watch_handle, &m_watch_ctx);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void ImageWatcher<I>::handle_register_watch(int r) {
  RWLock::WLocker watch_locker(m_watch_lock);
  assert(m_watch_state == WATCH_STATE_UNREGISTERED);
  if (r < 0) {
    m_watch_handle = 0;
  } else if (r >= 0) {
    m_watch_state = WATCH_STATE_REGISTERED;
  }
}

template <typename I>
void ImageWatcher<I>::unregister_watch(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " unregistering image watcher" << dendl;

  cancel_async_requests();

  C_Gather *gather_ctx = nullptr;
  {
    RWLock::WLocker watch_locker(m_watch_lock);
    if (m_watch_state == WATCH_STATE_REWATCHING) {
      ldout(cct, 10) << this << " delaying unregister until rewatch completed"
                     << dendl;

      assert(m_unregister_watch_ctx == nullptr);
      m_unregister_watch_ctx = new FunctionContext([this, on_finish](int r) {
          unregister_watch(on_finish);
        });
      return;
    }

    gather_ctx = new C_Gather(m_image_ctx.cct, create_async_context_callback(
      m_image_ctx, on_finish));
    if (m_watch_state == WATCH_STATE_REGISTERED) {
      m_watch_state = WATCH_STATE_UNREGISTERED;

      librados::AioCompletion *aio_comp = create_rados_safe_callback(
        new C_UnwatchAndFlush(m_image_ctx.md_ctx, gather_ctx->new_sub()));
      int r = m_image_ctx.md_ctx.aio_unwatch(m_watch_handle, aio_comp);
      assert(r == 0);
      aio_comp->release();
    } else if (m_watch_state == WATCH_STATE_ERROR) {
      m_watch_state = WATCH_STATE_UNREGISTERED;
    }
  }

  assert(gather_ctx != nullptr);
  m_task_finisher->cancel_all(gather_ctx->new_sub());
  gather_ctx->activate();
}

template <typename I>
void ImageWatcher<I>::flush(Context *on_finish) {
  m_notifier.flush(on_finish);
}

template <typename I>
void ImageWatcher<I>::schedule_async_progress(const AsyncRequestId &request,
					      uint64_t offset, uint64_t total) {
  FunctionContext *ctx = new FunctionContext(
    boost::bind(&ImageWatcher<I>::notify_async_progress, this, request, offset,
                total));
  m_task_finisher->queue(Task(TASK_CODE_ASYNC_PROGRESS, request), ctx);
}

template <typename I>
int ImageWatcher<I>::notify_async_progress(const AsyncRequestId &request,
				           uint64_t offset, uint64_t total) {
  ldout(m_image_ctx.cct, 20) << this << " remote async request progress: "
			     << request << " @ " << offset
			     << "/" << total << dendl;

  bufferlist bl;
  ::encode(NotifyMessage(AsyncProgressPayload(request, offset, total)), bl);
  m_notifier.notify(bl, nullptr, nullptr);
  return 0;
}

template <typename I>
void ImageWatcher<I>::schedule_async_complete(const AsyncRequestId &request,
                                              int r) {
  FunctionContext *ctx = new FunctionContext(
    boost::bind(&ImageWatcher<I>::notify_async_complete, this, request, r));
  m_task_finisher->queue(ctx);
}

template <typename I>
void ImageWatcher<I>::notify_async_complete(const AsyncRequestId &request,
                                            int r) {
  ldout(m_image_ctx.cct, 20) << this << " remote async request finished: "
			     << request << " = " << r << dendl;

  bufferlist bl;
  ::encode(NotifyMessage(AsyncCompletePayload(request, r)), bl);
  m_notifier.notify(bl, nullptr, new FunctionContext(
    boost::bind(&ImageWatcher<I>::handle_async_complete, this, request, r,
                _1)));
}

template <typename I>
void ImageWatcher<I>::handle_async_complete(const AsyncRequestId &request,
                                            int r, int ret_val) {
  ldout(m_image_ctx.cct, 20) << this << " " << __func__ << ": "
                             << "request=" << request << ", r=" << ret_val
                             << dendl;
  if (ret_val < 0) {
    lderr(m_image_ctx.cct) << this << " failed to notify async complete: "
			   << cpp_strerror(ret_val) << dendl;
    if (ret_val == -ETIMEDOUT) {
      schedule_async_complete(request, r);
    }
  } else {
    RWLock::WLocker async_request_locker(m_async_request_lock);
    m_async_pending.erase(request);
  }
}

template <typename I>
void ImageWatcher<I>::notify_flatten(uint64_t request_id,
                                     ProgressContext &prog_ctx,
                                     Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  bufferlist bl;
  ::encode(NotifyMessage(FlattenPayload(async_request_id)), bl);
  notify_async_request(async_request_id, std::move(bl), prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_resize(uint64_t request_id, uint64_t size,
			            bool allow_shrink,
                                    ProgressContext &prog_ctx,
                                    Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  bufferlist bl;
  ::encode(NotifyMessage(ResizePayload(size, allow_shrink, async_request_id)), bl);
  notify_async_request(async_request_id, std::move(bl), prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_snap_create(const std::string &snap_name,
                                         Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  bufferlist bl;
  ::encode(NotifyMessage(SnapCreatePayload(snap_name)), bl);
  notify_lock_owner(std::move(bl), on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_snap_rename(const snapid_t &src_snap_id,
				         const std::string &dst_snap_name,
                                      Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  bufferlist bl;
  ::encode(NotifyMessage(SnapRenamePayload(src_snap_id, dst_snap_name)), bl);
  notify_lock_owner(std::move(bl), on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_snap_remove(const std::string &snap_name,
                                         Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  bufferlist bl;
  ::encode(NotifyMessage(SnapRemovePayload(snap_name)), bl);
  notify_lock_owner(std::move(bl), on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_snap_protect(const std::string &snap_name,
                                          Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  bufferlist bl;
  ::encode(NotifyMessage(SnapProtectPayload(snap_name)), bl);
  notify_lock_owner(std::move(bl), on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_snap_unprotect(const std::string &snap_name,
                                            Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  bufferlist bl;
  ::encode(NotifyMessage(SnapUnprotectPayload(snap_name)), bl);
  notify_lock_owner(std::move(bl), on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_rebuild_object_map(uint64_t request_id,
                                                ProgressContext &prog_ctx,
                                                Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  bufferlist bl;
  ::encode(NotifyMessage(RebuildObjectMapPayload(async_request_id)), bl);
  notify_async_request(async_request_id, std::move(bl), prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_rename(const std::string &image_name,
                                    Context *on_finish) {
  assert(m_image_ctx.owner_lock.is_locked());
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  bufferlist bl;
  ::encode(NotifyMessage(RenamePayload(image_name)), bl);
  notify_lock_owner(std::move(bl), on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_header_update(Context *on_finish) {
  ldout(m_image_ctx.cct, 10) << this << ": " << __func__ << dendl;

  // supports legacy (empty buffer) clients
  bufferlist bl;
  ::encode(NotifyMessage(HeaderUpdatePayload()), bl);
  m_notifier.notify(bl, nullptr, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_header_update(librados::IoCtx &io_ctx,
				           const std::string &oid) {
  // supports legacy (empty buffer) clients
  bufferlist bl;
  ::encode(NotifyMessage(HeaderUpdatePayload()), bl);
  io_ctx.notify2(oid, bl, image_watcher::Notifier::NOTIFY_TIMEOUT, nullptr);
}

template <typename I>
void ImageWatcher<I>::schedule_cancel_async_requests() {
  FunctionContext *ctx = new FunctionContext(
    boost::bind(&ImageWatcher<I>::cancel_async_requests, this));
  m_task_finisher->queue(TASK_CODE_CANCEL_ASYNC_REQUESTS, ctx);
}

template <typename I>
void ImageWatcher<I>::cancel_async_requests() {
  RWLock::WLocker l(m_async_request_lock);
  for (std::map<AsyncRequestId, AsyncRequest>::iterator iter =
	 m_async_requests.begin();
       iter != m_async_requests.end(); ++iter) {
    iter->second.first->complete(-ERESTART);
  }
  m_async_requests.clear();
}

template <typename I>
void ImageWatcher<I>::set_owner_client_id(const ClientId& client_id) {
  assert(m_owner_client_id_lock.is_locked());
  m_owner_client_id = client_id;
  ldout(m_image_ctx.cct, 10) << this << " current lock owner: "
                             << m_owner_client_id << dendl;
}

template <typename I>
ClientId ImageWatcher<I>::get_client_id() {
  RWLock::RLocker l(m_watch_lock);
  return ClientId(m_image_ctx.md_ctx.get_instance_id(), m_watch_handle);
}

template <typename I>
void ImageWatcher<I>::notify_acquired_lock() {
  ldout(m_image_ctx.cct, 10) << this << " notify acquired lock" << dendl;

  ClientId client_id = get_client_id();
  {
    Mutex::Locker owner_client_id_locker(m_owner_client_id_lock);
    set_owner_client_id(client_id);
  }

  bufferlist bl;
  ::encode(NotifyMessage(AcquiredLockPayload(client_id)), bl);
  m_notifier.notify(bl, nullptr, nullptr);
}

template <typename I>
void ImageWatcher<I>::notify_released_lock() {
  ldout(m_image_ctx.cct, 10) << this << " notify released lock" << dendl;

  {
    Mutex::Locker owner_client_id_locker(m_owner_client_id_lock);
    set_owner_client_id(ClientId());
  }

  bufferlist bl;
  ::encode(NotifyMessage(ReleasedLockPayload(get_client_id())), bl);
  m_notifier.notify(bl, nullptr, nullptr);
}

template <typename I>
void ImageWatcher<I>::schedule_request_lock(bool use_timer, int timer_delay) {
  assert(m_image_ctx.owner_lock.is_locked());

  if (m_image_ctx.exclusive_lock == nullptr) {
    // exclusive lock dynamically disabled via image refresh
    return;
  }
  assert(m_image_ctx.exclusive_lock &&
         !m_image_ctx.exclusive_lock->is_lock_owner());

  RWLock::RLocker watch_locker(m_watch_lock);
  if (m_watch_state == WATCH_STATE_REGISTERED) {
    ldout(m_image_ctx.cct, 15) << this << " requesting exclusive lock" << dendl;

    FunctionContext *ctx = new FunctionContext(
      boost::bind(&ImageWatcher<I>::notify_request_lock, this));
    if (use_timer) {
      if (timer_delay < 0) {
        timer_delay = RETRY_DELAY_SECONDS;
      }
      m_task_finisher->add_event_after(TASK_CODE_REQUEST_LOCK, timer_delay,
                                       ctx);
    } else {
      m_task_finisher->queue(TASK_CODE_REQUEST_LOCK, ctx);
    }
  }
}

template <typename I>
void ImageWatcher<I>::notify_request_lock() {
  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  RWLock::RLocker snap_locker(m_image_ctx.snap_lock);

  // ExclusiveLock state machine can be dynamically disabled
  if (m_image_ctx.exclusive_lock == nullptr) {
    return;
  }
  assert(!m_image_ctx.exclusive_lock->is_lock_owner());

  ldout(m_image_ctx.cct, 10) << this << " notify request lock" << dendl;

  bufferlist bl;
  ::encode(NotifyMessage(RequestLockPayload(get_client_id(), false)), bl);
  notify_lock_owner(std::move(bl), create_context_callback<
    ImageWatcher, &ImageWatcher<I>::handle_request_lock>(this));
}

template <typename I>
void ImageWatcher<I>::handle_request_lock(int r) {
  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  RWLock::RLocker snap_locker(m_image_ctx.snap_lock);

  // ExclusiveLock state machine cannot transition -- but can be
  // dynamically disabled
  if (m_image_ctx.exclusive_lock == nullptr) {
    return;
  }

  if (r == -ETIMEDOUT) {
    ldout(m_image_ctx.cct, 5) << this << " timed out requesting lock: retrying"
                              << dendl;

    // treat this is a dead client -- so retest acquiring the lock
    m_image_ctx.exclusive_lock->handle_peer_notification();
  } else if (r < 0) {
    lderr(m_image_ctx.cct) << this << " error requesting lock: "
                           << cpp_strerror(r) << dendl;
    schedule_request_lock(true);
  } else {
    // lock owner acked -- but resend if we don't see them release the lock
    int retry_timeout = m_image_ctx.cct->_conf->client_notify_timeout;
    ldout(m_image_ctx.cct, 15) << this << " will retry in " << retry_timeout
                               << " seconds" << dendl;
    schedule_request_lock(true, retry_timeout);
  }
}

template <typename I>
void ImageWatcher<I>::notify_lock_owner(bufferlist &&bl, Context *on_finish) {
  assert(on_finish != nullptr);
  assert(m_image_ctx.owner_lock.is_locked());
  NotifyLockOwner *notify_lock_owner = NotifyLockOwner::create(
    m_image_ctx, m_notifier, std::move(bl), on_finish);
  notify_lock_owner->send();
}

template <typename I>
Context *ImageWatcher<I>::remove_async_request(const AsyncRequestId &id) {
  RWLock::WLocker async_request_locker(m_async_request_lock);
  auto it = m_async_requests.find(id);
  if (it != m_async_requests.end()) {
    Context *on_complete = it->second.first;
    m_async_requests.erase(it);
    return on_complete;
  }
  return nullptr;
}

template <typename I>
void ImageWatcher<I>::schedule_async_request_timed_out(const AsyncRequestId &id) {
  ldout(m_image_ctx.cct, 20) << "scheduling async request time out: " << id
                             << dendl;

  Context *ctx = new FunctionContext(boost::bind(
    &ImageWatcher<I>::async_request_timed_out, this, id));

  Task task(TASK_CODE_ASYNC_REQUEST, id);
  m_task_finisher->cancel(task);

  m_task_finisher->add_event_after(task, m_image_ctx.request_timed_out_seconds, ctx);
}

template <typename I>
void ImageWatcher<I>::async_request_timed_out(const AsyncRequestId &id) {
  Context *on_complete = remove_async_request(id);
  if (on_complete != nullptr) {
    ldout(m_image_ctx.cct, 5) << "async request timed out: " << id << dendl;
    m_image_ctx.op_work_queue->queue(on_complete, -ETIMEDOUT);
  }
}

template <typename I>
void ImageWatcher<I>::notify_async_request(const AsyncRequestId &async_request_id,
				           bufferlist &&in,
				        ProgressContext& prog_ctx,
                                        Context *on_finish) {
  assert(on_finish != nullptr);
  assert(m_image_ctx.owner_lock.is_locked());

  ldout(m_image_ctx.cct, 10) << this << " async request: " << async_request_id
                             << dendl;

  Context *on_notify = new FunctionContext([this, async_request_id](int r) {
    if (r < 0) {
      // notification failed -- don't expect updates
      Context *on_complete = remove_async_request(async_request_id);
      if (on_complete != nullptr) {
        on_complete->complete(r);
      }
    }
  });
  Context *on_complete = new FunctionContext(
    [this, async_request_id, on_finish](int r) {
      m_task_finisher->cancel(Task(TASK_CODE_ASYNC_REQUEST, async_request_id));
      on_finish->complete(r);
    });

  {
    RWLock::WLocker async_request_locker(m_async_request_lock);
    m_async_requests[async_request_id] = AsyncRequest(on_complete, &prog_ctx);
  }

  schedule_async_request_timed_out(async_request_id);
  notify_lock_owner(std::move(in), on_notify);
}

template <typename I>
int ImageWatcher<I>::prepare_async_request(const AsyncRequestId& async_request_id,
                                           bool* new_request, Context** ctx,
                                           ProgressContext** prog_ctx) {
  if (async_request_id.client_id == get_client_id()) {
    return -ERESTART;
  } else {
    RWLock::WLocker l(m_async_request_lock);
    if (m_async_pending.count(async_request_id) == 0) {
      m_async_pending.insert(async_request_id);
      *new_request = true;
      *prog_ctx = new RemoteProgressContext(*this, async_request_id);
      *ctx = new RemoteContext(*this, async_request_id, *prog_ctx);
    } else {
      *new_request = false;
    }
  }
  return 0;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const HeaderUpdatePayload &payload,
			             C_NotifyAck *ack_ctx) {
  ldout(m_image_ctx.cct, 10) << this << " image header updated" << dendl;

  m_image_ctx.state->handle_update_notification();
  m_image_ctx.perfcounter->inc(l_librbd_notify);
  if (ack_ctx != nullptr) {
    m_image_ctx.state->flush_update_watchers(new C_ResponseMessage(ack_ctx));
    return false;
  }
  return true;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const AcquiredLockPayload &payload,
                                     C_NotifyAck *ack_ctx) {
  ldout(m_image_ctx.cct, 10) << this << " image exclusively locked announcement"
                             << dendl;

  bool cancel_async_requests = true;
  if (payload.client_id.is_valid()) {
    Mutex::Locker owner_client_id_locker(m_owner_client_id_lock);
    if (payload.client_id == m_owner_client_id) {
      cancel_async_requests = false;
    }
    set_owner_client_id(payload.client_id);
  }

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr) {
    // potentially wake up the exclusive lock state machine now that
    // a lock owner has advertised itself
    m_image_ctx.exclusive_lock->handle_peer_notification();
  }
  if (cancel_async_requests &&
      (m_image_ctx.exclusive_lock == nullptr ||
       !m_image_ctx.exclusive_lock->is_lock_owner())) {
    schedule_cancel_async_requests();
  }
  return true;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const ReleasedLockPayload &payload,
                                     C_NotifyAck *ack_ctx) {
  ldout(m_image_ctx.cct, 10) << this << " exclusive lock released" << dendl;

  bool cancel_async_requests = true;
  if (payload.client_id.is_valid()) {
    Mutex::Locker l(m_owner_client_id_lock);
    if (payload.client_id != m_owner_client_id) {
      ldout(m_image_ctx.cct, 10) << this << " unexpected owner: "
                                 << payload.client_id << " != "
                                 << m_owner_client_id << dendl;
      cancel_async_requests = false;
    } else {
      set_owner_client_id(ClientId());
    }
  }

  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (cancel_async_requests &&
      (m_image_ctx.exclusive_lock == nullptr ||
       !m_image_ctx.exclusive_lock->is_lock_owner())) {
    schedule_cancel_async_requests();
  }

  // alert the exclusive lock state machine that the lock is available
  if (m_image_ctx.exclusive_lock != nullptr &&
      !m_image_ctx.exclusive_lock->is_lock_owner()) {
    m_task_finisher->cancel(TASK_CODE_REQUEST_LOCK);
    m_image_ctx.exclusive_lock->handle_peer_notification();
  }
  return true;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const RequestLockPayload &payload,
                                     C_NotifyAck *ack_ctx) {
  ldout(m_image_ctx.cct, 10) << this << " exclusive lock requested" << dendl;
  if (payload.client_id == get_client_id()) {
    return true;
  }

  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr &&
      m_image_ctx.exclusive_lock->is_lock_owner()) {
    int r = 0;
    bool accept_request = m_image_ctx.exclusive_lock->accept_requests(&r);

    // need to send something back so the client can detect a missing leader
    ::encode(ResponseMessage(r), ack_ctx->out);

    if (accept_request) {
      Mutex::Locker owner_client_id_locker(m_owner_client_id_lock);
      if (!m_owner_client_id.is_valid()) {
        return true;
      }

      ldout(m_image_ctx.cct, 10) << this << " queuing release of exclusive lock"
                                 << dendl;
      m_image_ctx.get_exclusive_lock_policy()->lock_requested(payload.force);
    }
  }
  return true;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const AsyncProgressPayload &payload,
                                     C_NotifyAck *ack_ctx) {
  RWLock::RLocker l(m_async_request_lock);
  std::map<AsyncRequestId, AsyncRequest>::iterator req_it =
    m_async_requests.find(payload.async_request_id);
  if (req_it != m_async_requests.end()) {
    ldout(m_image_ctx.cct, 20) << this << " request progress: "
			       << payload.async_request_id << " @ "
			       << payload.offset << "/" << payload.total
			       << dendl;
    schedule_async_request_timed_out(payload.async_request_id);
    req_it->second.second->update_progress(payload.offset, payload.total);
  }
  return true;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const AsyncCompletePayload &payload,
                                     C_NotifyAck *ack_ctx) {
  Context *on_complete = remove_async_request(payload.async_request_id);
  if (on_complete != nullptr) {
    ldout(m_image_ctx.cct, 10) << this << " request finished: "
                               << payload.async_request_id << "="
			       << payload.result << dendl;
    on_complete->complete(payload.result);
  }
  return true;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const FlattenPayload &payload,
				     C_NotifyAck *ack_ctx) {

  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr) {
    int r;
    if (m_image_ctx.exclusive_lock->accept_requests(&r)) {
      bool new_request;
      Context *ctx;
      ProgressContext *prog_ctx;
      r = prepare_async_request(payload.async_request_id, &new_request,
                                &ctx, &prog_ctx);
      if (new_request) {
        ldout(m_image_ctx.cct, 10) << this << " remote flatten request: "
				   << payload.async_request_id << dendl;
        m_image_ctx.operations->execute_flatten(*prog_ctx, ctx);
      }

      ::encode(ResponseMessage(r), ack_ctx->out);
    } else if (r < 0) {
      ::encode(ResponseMessage(r), ack_ctx->out);
    }
  }
  return true;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const ResizePayload &payload,
				     C_NotifyAck *ack_ctx) {
  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr) {
    int r;
    if (m_image_ctx.exclusive_lock->accept_requests(&r)) {
      bool new_request;
      Context *ctx;
      ProgressContext *prog_ctx;
      r = prepare_async_request(payload.async_request_id, &new_request,
                                &ctx, &prog_ctx);
      if (new_request) {
        ldout(m_image_ctx.cct, 10) << this << " remote resize request: "
				   << payload.async_request_id << " "
				   << payload.size << " "
				   << payload.allow_shrink << dendl;
        m_image_ctx.operations->execute_resize(payload.size, payload.allow_shrink, *prog_ctx, ctx, 0);
      }

      ::encode(ResponseMessage(r), ack_ctx->out);
    } else if (r < 0) {
      ::encode(ResponseMessage(r), ack_ctx->out);
    }
  }
  return true;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const SnapCreatePayload &payload,
			             C_NotifyAck *ack_ctx) {
  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr) {
    int r;
    if (m_image_ctx.exclusive_lock->accept_requests(&r)) {
      ldout(m_image_ctx.cct, 10) << this << " remote snap_create request: "
			         << payload.snap_name << dendl;

      m_image_ctx.operations->execute_snap_create(payload.snap_name,
                                                  new C_ResponseMessage(ack_ctx),
                                                  0, false);
      return false;
    } else if (r < 0) {
      ::encode(ResponseMessage(r), ack_ctx->out);
    }
  }
  return true;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const SnapRenamePayload &payload,
			             C_NotifyAck *ack_ctx) {
  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr) {
    int r;
    if (m_image_ctx.exclusive_lock->accept_requests(&r)) {
      ldout(m_image_ctx.cct, 10) << this << " remote snap_rename request: "
			         << payload.snap_id << " to "
			         << payload.snap_name << dendl;

      m_image_ctx.operations->execute_snap_rename(payload.snap_id,
                                                  payload.snap_name,
                                                  new C_ResponseMessage(ack_ctx));
      return false;
    } else if (r < 0) {
      ::encode(ResponseMessage(r), ack_ctx->out);
    }
  }
  return true;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const SnapRemovePayload &payload,
			             C_NotifyAck *ack_ctx) {
  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr) {
    int r;
    if (m_image_ctx.exclusive_lock->accept_requests(&r)) {
      ldout(m_image_ctx.cct, 10) << this << " remote snap_remove request: "
			         << payload.snap_name << dendl;

      m_image_ctx.operations->execute_snap_remove(payload.snap_name,
                                                  new C_ResponseMessage(ack_ctx));
      return false;
    } else if (r < 0) {
      ::encode(ResponseMessage(r), ack_ctx->out);
    }
  }
  return true;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const SnapProtectPayload& payload,
                                     C_NotifyAck *ack_ctx) {
  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr) {
    int r;
    if (m_image_ctx.exclusive_lock->accept_requests(&r)) {
      ldout(m_image_ctx.cct, 10) << this << " remote snap_protect request: "
                                 << payload.snap_name << dendl;

      m_image_ctx.operations->execute_snap_protect(payload.snap_name,
                                                   new C_ResponseMessage(ack_ctx));
      return false;
    } else if (r < 0) {
      ::encode(ResponseMessage(r), ack_ctx->out);
    }
  }
  return true;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const SnapUnprotectPayload& payload,
                                     C_NotifyAck *ack_ctx) {
  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr) {
    int r;
    if (m_image_ctx.exclusive_lock->accept_requests(&r)) {
      ldout(m_image_ctx.cct, 10) << this << " remote snap_unprotect request: "
                                 << payload.snap_name << dendl;

      m_image_ctx.operations->execute_snap_unprotect(payload.snap_name,
                                                     new C_ResponseMessage(ack_ctx));
      return false;
    } else if (r < 0) {
      ::encode(ResponseMessage(r), ack_ctx->out);
    }
  }
  return true;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const RebuildObjectMapPayload& payload,
                                     C_NotifyAck *ack_ctx) {
  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr) {
    int r;
    if (m_image_ctx.exclusive_lock->accept_requests(&r)) {
      bool new_request;
      Context *ctx;
      ProgressContext *prog_ctx;
      r = prepare_async_request(payload.async_request_id, &new_request,
                                &ctx, &prog_ctx);
      if (new_request) {
        ldout(m_image_ctx.cct, 10) << this
                                   << " remote rebuild object map request: "
                                   << payload.async_request_id << dendl;
        m_image_ctx.operations->execute_rebuild_object_map(*prog_ctx, ctx);
      }

      ::encode(ResponseMessage(r), ack_ctx->out);
    } else if (r < 0) {
      ::encode(ResponseMessage(r), ack_ctx->out);
    }
  }
  return true;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const RenamePayload& payload,
                                     C_NotifyAck *ack_ctx) {
  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr) {
    int r;
    if (m_image_ctx.exclusive_lock->accept_requests(&r)) {
      ldout(m_image_ctx.cct, 10) << this << " remote rename request: "
                                 << payload.image_name << dendl;

      m_image_ctx.operations->execute_rename(payload.image_name,
                                             new C_ResponseMessage(ack_ctx));
      return false;
    } else if (r < 0) {
      ::encode(ResponseMessage(r), ack_ctx->out);
    }
  }
  return true;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const UnknownPayload &payload,
			             C_NotifyAck *ack_ctx) {
  RWLock::RLocker l(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr) {
    int r;
    if (m_image_ctx.exclusive_lock->accept_requests(&r) || r < 0) {
      ::encode(ResponseMessage(-EOPNOTSUPP), ack_ctx->out);
    }
  }
  return true;
}

template <typename I>
void ImageWatcher<I>::process_payload(uint64_t notify_id, uint64_t handle,
                                      const Payload &payload, int r) {
  if (r < 0) {
    bufferlist out_bl;
    acknowledge_notify(notify_id, handle, out_bl);
  } else {
    apply_visitor(HandlePayloadVisitor(this, notify_id, handle), payload);
  }
}

template <typename I>
void ImageWatcher<I>::handle_notify(uint64_t notify_id, uint64_t handle,
			            bufferlist &bl) {
  NotifyMessage notify_message;
  if (bl.length() == 0) {
    // legacy notification for header updates
    notify_message = NotifyMessage(HeaderUpdatePayload());
  } else {
    try {
      bufferlist::iterator iter = bl.begin();
      ::decode(notify_message, iter);
    } catch (const buffer::error &err) {
      lderr(m_image_ctx.cct) << this << " error decoding image notification: "
			     << err.what() << dendl;
      return;
    }
  }

  // if an image refresh is required, refresh before processing the request
  if (notify_message.check_for_refresh() &&
      m_image_ctx.state->is_refresh_required()) {
    m_image_ctx.state->refresh(new C_ProcessPayload(this, notify_id, handle,
                                                    notify_message.payload));
  } else {
    process_payload(notify_id, handle, notify_message.payload, 0);
  }
}

template <typename I>
void ImageWatcher<I>::handle_error(uint64_t handle, int err) {
  lderr(m_image_ctx.cct) << this << " image watch failed: " << handle << ", "
                         << cpp_strerror(err) << dendl;

  {
    Mutex::Locker l(m_owner_client_id_lock);
    set_owner_client_id(ClientId());
  }

  RWLock::WLocker l(m_watch_lock);
  if (m_watch_state == WATCH_STATE_REGISTERED) {
    m_image_ctx.md_ctx.unwatch2(m_watch_handle);
    m_watch_state = WATCH_STATE_ERROR;

    FunctionContext *ctx = new FunctionContext(
      boost::bind(&ImageWatcher<I>::rewatch, this));
    m_task_finisher->queue(TASK_CODE_REREGISTER_WATCH, ctx);
  }
}

template <typename I>
void ImageWatcher<I>::acknowledge_notify(uint64_t notify_id, uint64_t handle,
				         bufferlist &out) {
  m_image_ctx.md_ctx.notify_ack(m_image_ctx.header_oid, notify_id, handle, out);
}

template <typename I>
void ImageWatcher<I>::rewatch() {
  ldout(m_image_ctx.cct, 10) << this << " re-registering image watch" << dendl;

  RWLock::WLocker l(m_watch_lock);
  if (m_watch_state != WATCH_STATE_ERROR) {
    return;
  }
  m_watch_state = WATCH_STATE_REWATCHING;

  Context *ctx = create_context_callback<
    ImageWatcher<I>, &ImageWatcher<I>::handle_rewatch>(this);
  RewatchRequest<I> *req = RewatchRequest<I>::create(m_image_ctx, m_watch_lock,
                                                     &m_watch_ctx,
                                                     &m_watch_handle, ctx);
  req->send();
}

template <typename I>
void ImageWatcher<I>::handle_rewatch(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  WatchState next_watch_state = WATCH_STATE_REGISTERED;
  if (r < 0) {
    next_watch_state = WATCH_STATE_ERROR;
  }

  Context *unregister_watch_ctx = nullptr;
  {
    RWLock::WLocker watch_locker(m_watch_lock);
    assert(m_watch_state == WATCH_STATE_REWATCHING);
    m_watch_state = next_watch_state;

    std::swap(unregister_watch_ctx, m_unregister_watch_ctx);

    // image might have been updated while we didn't have active watch
    handle_payload(HeaderUpdatePayload(), nullptr);
  }

  // wake up pending unregister request
  if (unregister_watch_ctx != nullptr) {
    unregister_watch_ctx->complete(0);
  }
}

template <typename I>
void ImageWatcher<I>::WatchCtx::handle_notify(uint64_t notify_id,
                                              uint64_t handle,
                                              uint64_t notifier_id,
                                              bufferlist& bl) {
  image_watcher.handle_notify(notify_id, handle, bl);
}

template <typename I>
void ImageWatcher<I>::WatchCtx::handle_error(uint64_t handle, int err) {
  image_watcher.handle_error(handle, err);
}

template <typename I>
void ImageWatcher<I>::RemoteContext::finish(int r) {
  m_image_watcher.schedule_async_complete(m_async_request_id, r);
}

template <typename I>
ImageWatcher<I>::C_NotifyAck::C_NotifyAck(ImageWatcher *image_watcher,
                                          uint64_t notify_id, uint64_t handle)
  : image_watcher(image_watcher), notify_id(notify_id), handle(handle) {
  CephContext *cct = image_watcher->m_image_ctx.cct;
  ldout(cct, 10) << this << " C_NotifyAck start: id=" << notify_id << ", "
                 << "handle=" << handle << dendl;
}

template <typename I>
void ImageWatcher<I>::C_NotifyAck::finish(int r) {
  assert(r == 0);
  CephContext *cct = image_watcher->m_image_ctx.cct;
  ldout(cct, 10) << this << " C_NotifyAck finish: id=" << notify_id << ", "
                 << "handle=" << handle << dendl;

  image_watcher->acknowledge_notify(notify_id, handle, out);
}

template <typename I>
void ImageWatcher<I>::C_ResponseMessage::finish(int r) {
  CephContext *cct = notify_ack->image_watcher->m_image_ctx.cct;
  ldout(cct, 10) << this << " C_ResponseMessage: r=" << r << dendl;

  ::encode(ResponseMessage(r), notify_ack->out);
  notify_ack->complete(0);
}

} // namespace librbd

template class librbd::ImageWatcher<librbd::ImageCtx>;
