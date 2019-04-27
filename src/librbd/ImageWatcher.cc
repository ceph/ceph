// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/ImageWatcher.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/Operations.h"
#include "librbd/TaskFinisher.h"
#include "librbd/Types.h"
#include "librbd/Utils.h"
#include "librbd/exclusive_lock/Policy.h"
#include "librbd/image_watcher/NotifyLockOwner.h"
#include "librbd/io/AioCompletion.h"
#include "librbd/watcher/Utils.h"
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
using util::create_rados_callback;
using librbd::watcher::util::HandlePayloadVisitor;

using ceph::encode;
using ceph::decode;

static const double	RETRY_DELAY_SECONDS = 1.0;

template <typename I>
struct ImageWatcher<I>::C_ProcessPayload : public Context {
  ImageWatcher *image_watcher;
  uint64_t notify_id;
  uint64_t handle;
  watch_notify::Payload payload;

  C_ProcessPayload(ImageWatcher *image_watcher_, uint64_t notify_id_,
                   uint64_t handle_, const watch_notify::Payload &payload)
    : image_watcher(image_watcher_), notify_id(notify_id_), handle(handle_),
      payload(payload) {
  }

  void finish(int r) override {
    image_watcher->m_async_op_tracker.start_op();
    if (image_watcher->notifications_blocked()) {
      // requests are blocked -- just ack the notification
      bufferlist bl;
      image_watcher->acknowledge_notify(notify_id, handle, bl);
    } else {
      image_watcher->process_payload(notify_id, handle, payload);
    }
    image_watcher->m_async_op_tracker.finish_op();
  }
};

template <typename I>
ImageWatcher<I>::ImageWatcher(I &image_ctx)
  : Watcher(image_ctx.md_ctx, image_ctx.op_work_queue, image_ctx.header_oid),
    m_image_ctx(image_ctx),
    m_task_finisher(new TaskFinisher<Task>(*m_image_ctx.cct)),
    m_async_request_lock(util::unique_lock_name("librbd::ImageWatcher::m_async_request_lock", this)),
    m_owner_client_id_lock(util::unique_lock_name("librbd::ImageWatcher::m_owner_client_id_lock", this))
{
}

template <typename I>
ImageWatcher<I>::~ImageWatcher()
{
  delete m_task_finisher;
}

template <typename I>
void ImageWatcher<I>::unregister_watch(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " unregistering image watcher" << dendl;

  cancel_async_requests();

  FunctionContext *ctx = new FunctionContext([this, on_finish](int r) {
    m_task_finisher->cancel_all(on_finish);
  });
  Watcher::unregister_watch(ctx);
}

template <typename I>
void ImageWatcher<I>::block_notifies(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " "  << __func__ << dendl;

  on_finish = new FunctionContext([this, on_finish](int r) {
      cancel_async_requests();
      on_finish->complete(r);
    });
  Watcher::block_notifies(on_finish);
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

  send_notify(AsyncProgressPayload(request, offset, total));
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

  send_notify(AsyncCompletePayload(request, r),
    new FunctionContext(boost::bind(&ImageWatcher<I>::handle_async_complete,
                        this, request, r, _1)));
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
  ceph_assert(m_image_ctx.owner_lock.is_locked());
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  notify_async_request(async_request_id, FlattenPayload(async_request_id),
                       prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_resize(uint64_t request_id, uint64_t size,
			            bool allow_shrink,
                                    ProgressContext &prog_ctx,
                                    Context *on_finish) {
  ceph_assert(m_image_ctx.owner_lock.is_locked());
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  notify_async_request(async_request_id,
                       ResizePayload(size, allow_shrink, async_request_id),
                       prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_snap_create(const cls::rbd::SnapshotNamespace &snap_namespace,
					 const std::string &snap_name,
                                         Context *on_finish) {
  ceph_assert(m_image_ctx.owner_lock.is_locked());
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  notify_lock_owner(SnapCreatePayload(snap_namespace, snap_name), on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_snap_rename(const snapid_t &src_snap_id,
				         const std::string &dst_snap_name,
					 Context *on_finish) {
  ceph_assert(m_image_ctx.owner_lock.is_locked());
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  notify_lock_owner(SnapRenamePayload(src_snap_id, dst_snap_name), on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_snap_remove(const cls::rbd::SnapshotNamespace &snap_namespace,
					 const std::string &snap_name,
                                         Context *on_finish) {
  ceph_assert(m_image_ctx.owner_lock.is_locked());
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  notify_lock_owner(SnapRemovePayload(snap_namespace, snap_name), on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_snap_protect(const cls::rbd::SnapshotNamespace &snap_namespace,
					  const std::string &snap_name,
                                          Context *on_finish) {
  ceph_assert(m_image_ctx.owner_lock.is_locked());
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  notify_lock_owner(SnapProtectPayload(snap_namespace, snap_name), on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_snap_unprotect(const cls::rbd::SnapshotNamespace &snap_namespace,
					    const std::string &snap_name,
                                            Context *on_finish) {
  ceph_assert(m_image_ctx.owner_lock.is_locked());
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  notify_lock_owner(SnapUnprotectPayload(snap_namespace, snap_name), on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_rebuild_object_map(uint64_t request_id,
                                                ProgressContext &prog_ctx,
                                                Context *on_finish) {
  ceph_assert(m_image_ctx.owner_lock.is_locked());
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  notify_async_request(async_request_id,
                       RebuildObjectMapPayload(async_request_id),
                       prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_rename(const std::string &image_name,
                                    Context *on_finish) {
  ceph_assert(m_image_ctx.owner_lock.is_locked());
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  notify_lock_owner(RenamePayload(image_name), on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_update_features(uint64_t features, bool enabled,
                                             Context *on_finish) {
  ceph_assert(m_image_ctx.owner_lock.is_locked());
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  notify_lock_owner(UpdateFeaturesPayload(features, enabled), on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_migrate(uint64_t request_id,
                                     ProgressContext &prog_ctx,
                                     Context *on_finish) {
  ceph_assert(m_image_ctx.owner_lock.is_locked());
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  notify_async_request(async_request_id, MigratePayload(async_request_id),
                       prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_sparsify(uint64_t request_id, size_t sparse_size,
                                      ProgressContext &prog_ctx,
                                      Context *on_finish) {
  ceph_assert(m_image_ctx.owner_lock.is_locked());
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  notify_async_request(async_request_id,
                       SparsifyPayload(async_request_id, sparse_size), prog_ctx,
                       on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_header_update(Context *on_finish) {
  ldout(m_image_ctx.cct, 10) << this << ": " << __func__ << dendl;

  // supports legacy (empty buffer) clients
  send_notify(HeaderUpdatePayload(), on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_header_update(librados::IoCtx &io_ctx,
				           const std::string &oid) {
  // supports legacy (empty buffer) clients
  bufferlist bl;
  encode(NotifyMessage(HeaderUpdatePayload()), bl);
  io_ctx.notify2(oid, bl, watcher::Notifier::NOTIFY_TIMEOUT, nullptr);
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
  ceph_assert(m_owner_client_id_lock.is_locked());
  m_owner_client_id = client_id;
  ldout(m_image_ctx.cct, 10) << this << " current lock owner: "
                             << m_owner_client_id << dendl;
}

template <typename I>
ClientId ImageWatcher<I>::get_client_id() {
  RWLock::RLocker l(this->m_watch_lock);
  return ClientId(m_image_ctx.md_ctx.get_instance_id(), this->m_watch_handle);
}

template <typename I>
void ImageWatcher<I>::notify_acquired_lock() {
  ldout(m_image_ctx.cct, 10) << this << " notify acquired lock" << dendl;

  ClientId client_id = get_client_id();
  {
    Mutex::Locker owner_client_id_locker(m_owner_client_id_lock);
    set_owner_client_id(client_id);
  }

  send_notify(AcquiredLockPayload(client_id));
}

template <typename I>
void ImageWatcher<I>::notify_released_lock() {
  ldout(m_image_ctx.cct, 10) << this << " notify released lock" << dendl;

  {
    Mutex::Locker owner_client_id_locker(m_owner_client_id_lock);
    set_owner_client_id(ClientId());
  }

  send_notify(ReleasedLockPayload(get_client_id()));
}

template <typename I>
void ImageWatcher<I>::schedule_request_lock(bool use_timer, int timer_delay) {
  ceph_assert(m_image_ctx.owner_lock.is_locked());

  if (m_image_ctx.exclusive_lock == nullptr) {
    // exclusive lock dynamically disabled via image refresh
    return;
  }
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  RWLock::RLocker watch_locker(this->m_watch_lock);
  if (this->is_registered(this->m_watch_lock)) {
    ldout(m_image_ctx.cct, 15) << this << " requesting exclusive lock" << dendl;

    FunctionContext *ctx = new FunctionContext(
      boost::bind(&ImageWatcher<I>::notify_request_lock, this));
    if (use_timer) {
      if (timer_delay < 0) {
        timer_delay = RETRY_DELAY_SECONDS;
      }
      m_task_finisher->add_event_after(TASK_CODE_REQUEST_LOCK,
                                       timer_delay, ctx);
    } else {
      m_task_finisher->queue(TASK_CODE_REQUEST_LOCK, ctx);
    }
  }
}

template <typename I>
void ImageWatcher<I>::notify_request_lock() {
  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  RWLock::RLocker snap_locker(m_image_ctx.snap_lock);

  // ExclusiveLock state machine can be dynamically disabled or
  // race with task cancel
  if (m_image_ctx.exclusive_lock == nullptr ||
      m_image_ctx.exclusive_lock->is_lock_owner()) {
    return;
  }

  ldout(m_image_ctx.cct, 10) << this << " notify request lock" << dendl;

  notify_lock_owner(RequestLockPayload(get_client_id(), false),
      create_context_callback<
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
    m_image_ctx.exclusive_lock->handle_peer_notification(0);
  } else if (r == -EROFS) {
    ldout(m_image_ctx.cct, 5) << this << " peer will not release lock" << dendl;
    m_image_ctx.exclusive_lock->handle_peer_notification(r);
  } else if (r < 0) {
    lderr(m_image_ctx.cct) << this << " error requesting lock: "
                           << cpp_strerror(r) << dendl;
    schedule_request_lock(true);
  } else {
    // lock owner acked -- but resend if we don't see them release the lock
    int retry_timeout = m_image_ctx.cct->_conf.template get_val<int64_t>(
      "client_notify_timeout");
    ldout(m_image_ctx.cct, 15) << this << " will retry in " << retry_timeout
                               << " seconds" << dendl;
    schedule_request_lock(true, retry_timeout);
  }
}

template <typename I>
void ImageWatcher<I>::notify_lock_owner(const Payload& payload,
                                        Context *on_finish) {
  ceph_assert(on_finish != nullptr);
  ceph_assert(m_image_ctx.owner_lock.is_locked());

  bufferlist bl;
  encode(NotifyMessage(payload), bl);

  NotifyLockOwner *notify_lock_owner = NotifyLockOwner::create(
    m_image_ctx, this->m_notifier, std::move(bl), on_finish);
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

  m_task_finisher->add_event_after(
    task, m_image_ctx.config.template get_val<uint64_t>("rbd_request_timed_out_seconds"),
    ctx);
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
                                           const Payload& payload,
                                           ProgressContext& prog_ctx,
                                           Context *on_finish) {
  ceph_assert(on_finish != nullptr);
  ceph_assert(m_image_ctx.owner_lock.is_locked());

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
  notify_lock_owner(payload, on_notify);
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
    m_image_ctx.exclusive_lock->handle_peer_notification(0);
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
    m_image_ctx.exclusive_lock->handle_peer_notification(0);
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

    if (accept_request) {
      ceph_assert(r == 0);
      Mutex::Locker owner_client_id_locker(m_owner_client_id_lock);
      if (!m_owner_client_id.is_valid()) {
        return true;
      }

      ldout(m_image_ctx.cct, 10) << this << " queuing release of exclusive lock"
                                 << dendl;
      r = m_image_ctx.get_exclusive_lock_policy()->lock_requested(
        payload.force);
    }
    encode(ResponseMessage(r), ack_ctx->out);
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
      if (r == 0 && new_request) {
        ldout(m_image_ctx.cct, 10) << this << " remote flatten request: "
				   << payload.async_request_id << dendl;
        m_image_ctx.operations->execute_flatten(*prog_ctx, ctx);
      }

      encode(ResponseMessage(r), ack_ctx->out);
    } else if (r < 0) {
      encode(ResponseMessage(r), ack_ctx->out);
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
      if (r == 0 && new_request) {
        ldout(m_image_ctx.cct, 10) << this << " remote resize request: "
				   << payload.async_request_id << " "
				   << payload.size << " "
				   << payload.allow_shrink << dendl;
        m_image_ctx.operations->execute_resize(payload.size, payload.allow_shrink, *prog_ctx, ctx, 0);
      }

      encode(ResponseMessage(r), ack_ctx->out);
    } else if (r < 0) {
      encode(ResponseMessage(r), ack_ctx->out);
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

      m_image_ctx.operations->execute_snap_create(payload.snap_namespace,
						  payload.snap_name,
                                                  new C_ResponseMessage(ack_ctx),
                                                  0, false);
      return false;
    } else if (r < 0) {
      encode(ResponseMessage(r), ack_ctx->out);
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
      encode(ResponseMessage(r), ack_ctx->out);
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

      m_image_ctx.operations->execute_snap_remove(payload.snap_namespace,
						  payload.snap_name,
                                                  new C_ResponseMessage(ack_ctx));
      return false;
    } else if (r < 0) {
      encode(ResponseMessage(r), ack_ctx->out);
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

      m_image_ctx.operations->execute_snap_protect(payload.snap_namespace,
						   payload.snap_name,
                                                   new C_ResponseMessage(ack_ctx));
      return false;
    } else if (r < 0) {
      encode(ResponseMessage(r), ack_ctx->out);
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

      m_image_ctx.operations->execute_snap_unprotect(payload.snap_namespace,
						     payload.snap_name,
                                                     new C_ResponseMessage(ack_ctx));
      return false;
    } else if (r < 0) {
      encode(ResponseMessage(r), ack_ctx->out);
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
      if (r == 0 && new_request) {
        ldout(m_image_ctx.cct, 10) << this
                                   << " remote rebuild object map request: "
                                   << payload.async_request_id << dendl;
        m_image_ctx.operations->execute_rebuild_object_map(*prog_ctx, ctx);
      }

      encode(ResponseMessage(r), ack_ctx->out);
    } else if (r < 0) {
      encode(ResponseMessage(r), ack_ctx->out);
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
      encode(ResponseMessage(r), ack_ctx->out);
    }
  }
  return true;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const UpdateFeaturesPayload& payload,
                                     C_NotifyAck *ack_ctx) {
  RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
  if (m_image_ctx.exclusive_lock != nullptr) {
    int r;
    if (m_image_ctx.exclusive_lock->accept_requests(&r)) {
      ldout(m_image_ctx.cct, 10) << this << " remote update_features request: "
                                 << payload.features << " "
                                 << (payload.enabled ? "enabled" : "disabled")
                                 << dendl;

      m_image_ctx.operations->execute_update_features(
        payload.features, payload.enabled, new C_ResponseMessage(ack_ctx), 0);
      return false;
    } else if (r < 0) {
      encode(ResponseMessage(r), ack_ctx->out);
    }
  }
  return true;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const MigratePayload &payload,
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
      if (r == 0 && new_request) {
        ldout(m_image_ctx.cct, 10) << this << " remote migrate request: "
				   << payload.async_request_id << dendl;
        m_image_ctx.operations->execute_migrate(*prog_ctx, ctx);
      }

      encode(ResponseMessage(r), ack_ctx->out);
    } else if (r < 0) {
      encode(ResponseMessage(r), ack_ctx->out);
    }
  }
  return true;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const SparsifyPayload &payload,
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
      if (r == 0 && new_request) {
        ldout(m_image_ctx.cct, 10) << this << " remote sparsify request: "
				   << payload.async_request_id << dendl;
        m_image_ctx.operations->execute_sparsify(payload.sparse_size, *prog_ctx,
                                                 ctx);
      }

      encode(ResponseMessage(r), ack_ctx->out);
    } else if (r < 0) {
      encode(ResponseMessage(r), ack_ctx->out);
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
      encode(ResponseMessage(-EOPNOTSUPP), ack_ctx->out);
    }
  }
  return true;
}

template <typename I>
void ImageWatcher<I>::process_payload(uint64_t notify_id, uint64_t handle,
                                      const Payload &payload) {
  apply_visitor(HandlePayloadVisitor<ImageWatcher<I>>(this, notify_id, handle),
                payload);
}

template <typename I>
void ImageWatcher<I>::handle_notify(uint64_t notify_id, uint64_t handle,
			            uint64_t notifier_id, bufferlist &bl) {
  NotifyMessage notify_message;
  if (bl.length() == 0) {
    // legacy notification for header updates
    notify_message = NotifyMessage(HeaderUpdatePayload());
  } else {
    try {
      auto iter = bl.cbegin();
      decode(notify_message, iter);
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
    process_payload(notify_id, handle, notify_message.payload);
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

  Watcher::handle_error(handle, err);
}

template <typename I>
void ImageWatcher<I>::handle_rewatch_complete(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  {
    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
    if (m_image_ctx.exclusive_lock != nullptr) {
      // update the lock cookie with the new watch handle
      m_image_ctx.exclusive_lock->reacquire_lock(nullptr);
    }
  }

  // image might have been updated while we didn't have active watch
  handle_payload(HeaderUpdatePayload(), nullptr);
}

template <typename I>
void ImageWatcher<I>::send_notify(const Payload &payload, Context *ctx) {
  bufferlist bl;

  encode(NotifyMessage(payload), bl);
  Watcher::send_notify(bl, nullptr, ctx);
}

template <typename I>
void ImageWatcher<I>::RemoteContext::finish(int r) {
  m_image_watcher.schedule_async_complete(m_async_request_id, r);
}

template <typename I>
void ImageWatcher<I>::C_ResponseMessage::finish(int r) {
  CephContext *cct = notify_ack->cct;
  ldout(cct, 10) << this << " C_ResponseMessage: r=" << r << dendl;

  encode(ResponseMessage(r), notify_ack->out);
  notify_ack->complete(0);
}

} // namespace librbd

template class librbd::ImageWatcher<librbd::ImageCtx>;
