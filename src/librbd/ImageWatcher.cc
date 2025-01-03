// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/ImageWatcher.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/internal.h"
#include "librbd/TaskFinisher.h"
#include "librbd/Types.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/exclusive_lock/Policy.h"
#include "librbd/image_watcher/NotifyLockOwner.h"
#include "librbd/io/AioCompletion.h"
#include "include/encoding.h"
#include "common/errno.h"
#include <boost/bind/bind.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ImageWatcher: "

namespace librbd {

using namespace image_watcher;
using namespace watch_notify;
using util::create_async_context_callback;
using util::create_context_callback;
using util::create_rados_callback;

using ceph::encode;
using ceph::decode;

using namespace boost::placeholders;

static const double	RETRY_DELAY_SECONDS = 1.0;

template <typename I>
struct ImageWatcher<I>::C_ProcessPayload : public Context {
  ImageWatcher *image_watcher;
  uint64_t notify_id;
  uint64_t handle;
  std::unique_ptr<watch_notify::Payload> payload;

  C_ProcessPayload(ImageWatcher *image_watcher, uint64_t notify_id,
                   uint64_t handle,
                   std::unique_ptr<watch_notify::Payload> &&payload)
    : image_watcher(image_watcher), notify_id(notify_id), handle(handle),
      payload(std::move(payload)) {
  }

  void finish(int r) override {
    image_watcher->m_async_op_tracker.start_op();
    if (image_watcher->notifications_blocked()) {
      // requests are blocked -- just ack the notification
      bufferlist bl;
      image_watcher->acknowledge_notify(notify_id, handle, bl);
    } else {
      image_watcher->process_payload(notify_id, handle, payload.get());
    }
    image_watcher->m_async_op_tracker.finish_op();
  }
};

template <typename I>
ImageWatcher<I>::ImageWatcher(I &image_ctx)
  : Watcher(image_ctx.md_ctx, image_ctx.op_work_queue, image_ctx.header_oid),
    m_image_ctx(image_ctx),
    m_task_finisher(new TaskFinisher<Task>(*m_image_ctx.cct)),
    m_async_request_lock(ceph::make_shared_mutex(
      util::unique_lock_name("librbd::ImageWatcher::m_async_request_lock", this))),
    m_owner_client_id_lock(ceph::make_mutex(
      util::unique_lock_name("librbd::ImageWatcher::m_owner_client_id_lock", this)))
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

  // flush the task finisher queue before completing
  on_finish = create_async_context_callback(m_task_finisher, on_finish);

  on_finish = new LambdaContext([this, on_finish](int r) {
    cancel_quiesce_requests();
    m_task_finisher->cancel_all();
    m_async_op_tracker.wait_for_ops(on_finish);
  });
  Watcher::unregister_watch(on_finish);
}

template <typename I>
void ImageWatcher<I>::block_notifies(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " "  << __func__ << dendl;

  on_finish = new LambdaContext([this, on_finish](int r) {
      cancel_async_requests();
      on_finish->complete(r);
    });
  Watcher::block_notifies(on_finish);
}

template <typename I>
void ImageWatcher<I>::schedule_async_progress(const AsyncRequestId &request,
					      uint64_t offset, uint64_t total) {
  auto ctx = new LambdaContext([this, request, offset, total](int r) {
    if (r != -ECANCELED) {
      notify_async_progress(request, offset, total);
    }
  });
  m_task_finisher->queue(Task(TASK_CODE_ASYNC_PROGRESS, request), ctx);
}

template <typename I>
int ImageWatcher<I>::notify_async_progress(const AsyncRequestId &request,
				           uint64_t offset, uint64_t total) {
  ldout(m_image_ctx.cct, 20) << this << " remote async request progress: "
			     << request << " @ " << offset
			     << "/" << total << dendl;

  send_notify(new AsyncProgressPayload(request, offset, total));
  return 0;
}

template <typename I>
void ImageWatcher<I>::schedule_async_complete(const AsyncRequestId &request,
                                              int r) {
  m_async_op_tracker.start_op();
  auto ctx = new LambdaContext([this, request, ret_val=r](int r) {
    if (r != -ECANCELED) {
      notify_async_complete(request, ret_val);
    }
  });
  m_task_finisher->queue(ctx);
}

template <typename I>
void ImageWatcher<I>::notify_async_complete(const AsyncRequestId &request,
                                            int r) {
  ldout(m_image_ctx.cct, 20) << this << " remote async request finished: "
			     << request << "=" << r << dendl;

  send_notify(new AsyncCompletePayload(request, r),
    new LambdaContext(boost::bind(&ImageWatcher<I>::handle_async_complete,
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
    if (ret_val == -ETIMEDOUT && !is_unregistered()) {
      schedule_async_complete(request, r);
      m_async_op_tracker.finish_op();
      return;
    }
  }

  std::unique_lock async_request_locker{m_async_request_lock};
  mark_async_request_complete(request, r);
  m_async_op_tracker.finish_op();
}

template <typename I>
void ImageWatcher<I>::notify_flatten(uint64_t request_id,
                                     ProgressContext &prog_ctx,
                                     Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  notify_async_request(async_request_id, new FlattenPayload(async_request_id),
                       prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_resize(uint64_t request_id, uint64_t size,
			            bool allow_shrink,
                                    ProgressContext &prog_ctx,
                                    Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  notify_async_request(async_request_id,
                       new ResizePayload(async_request_id, size, allow_shrink),
                       prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_snap_create(uint64_t request_id,
                                         const cls::rbd::SnapshotNamespace &snap_namespace,
					 const std::string &snap_name,
                                         uint64_t flags,
                                         ProgressContext &prog_ctx,
                                         Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  notify_async_request(async_request_id,
                       new SnapCreatePayload(async_request_id, snap_namespace,
                                             snap_name, flags),
                       prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_snap_rename(uint64_t request_id,
                                         const snapid_t &src_snap_id,
				         const std::string &dst_snap_name,
					 Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  notify_async_request(
      async_request_id,
      new SnapRenamePayload(async_request_id, src_snap_id, dst_snap_name),
      m_no_op_prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_snap_remove(
    uint64_t request_id, const cls::rbd::SnapshotNamespace &snap_namespace,
    const std::string &snap_name, Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  notify_async_request(
      async_request_id,
      new SnapRemovePayload(async_request_id, snap_namespace, snap_name),
      m_no_op_prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_snap_protect(
    uint64_t request_id, const cls::rbd::SnapshotNamespace &snap_namespace,
    const std::string &snap_name, Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  notify_async_request(
      async_request_id,
      new SnapProtectPayload(async_request_id, snap_namespace, snap_name),
      m_no_op_prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_snap_unprotect(
    uint64_t request_id, const cls::rbd::SnapshotNamespace &snap_namespace,
    const std::string &snap_name, Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  notify_async_request(
      async_request_id,
      new SnapUnprotectPayload(async_request_id, snap_namespace, snap_name),
      m_no_op_prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_rebuild_object_map(uint64_t request_id,
                                                ProgressContext &prog_ctx,
                                                Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  notify_async_request(async_request_id,
                       new RebuildObjectMapPayload(async_request_id),
                       prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_rename(uint64_t request_id,
                                    const std::string &image_name,
                                    Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  notify_async_request(async_request_id,
                       new RenamePayload(async_request_id, image_name),
                       m_no_op_prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_update_features(uint64_t request_id,
                                             uint64_t features, bool enabled,
                                             Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  notify_async_request(async_request_id,
      new UpdateFeaturesPayload(async_request_id, features, enabled),
      m_no_op_prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_migrate(uint64_t request_id,
                                     ProgressContext &prog_ctx,
                                     Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  notify_async_request(async_request_id, new MigratePayload(async_request_id),
                       prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_sparsify(uint64_t request_id, size_t sparse_size,
                                      ProgressContext &prog_ctx,
                                      Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  notify_async_request(async_request_id,
                       new SparsifyPayload(async_request_id, sparse_size),
                       prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_header_update(Context *on_finish) {
  ldout(m_image_ctx.cct, 10) << this << ": " << __func__ << dendl;

  // supports legacy (empty buffer) clients
  send_notify(new HeaderUpdatePayload(), on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_header_update(librados::IoCtx &io_ctx,
				           const std::string &oid) {
  // supports legacy (empty buffer) clients
  bufferlist bl;
  encode(NotifyMessage(new HeaderUpdatePayload()), bl);
  io_ctx.notify2(oid, bl, watcher::Notifier::NOTIFY_TIMEOUT, nullptr);
}

template <typename I>
void ImageWatcher<I>::notify_quiesce(uint64_t *request_id,
                                     ProgressContext &prog_ctx,
                                     Context *on_finish) {
  *request_id = util::reserve_async_request_id();

  ldout(m_image_ctx.cct, 10) << this << " " << __func__ << ": request_id="
                             << request_id << dendl;

  AsyncRequestId async_request_id(get_client_id(), *request_id);

  auto total_attempts = m_image_ctx.config.template get_val<uint64_t>(
    "rbd_quiesce_notification_attempts");

  notify_quiesce(async_request_id, 1, total_attempts, prog_ctx, on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_quiesce(const AsyncRequestId &async_request_id,
                                     size_t attempt, size_t total_attempts,
                                     ProgressContext &prog_ctx,
                                     Context *on_finish) {
  ceph_assert(attempt <= total_attempts);
  ldout(m_image_ctx.cct, 10) << this << " " << __func__ << ": async_request_id="
                             << async_request_id << " attempts @ "
                             << attempt << "/" << total_attempts << dendl;

  auto notify_response = new watcher::NotifyResponse();
  auto on_notify = new LambdaContext(
    [notify_response=std::unique_ptr<watcher::NotifyResponse>(notify_response),
     this, async_request_id, attempt, total_attempts, &prog_ctx,
     on_finish](int r) {
      prog_ctx.update_progress(attempt, total_attempts);
      if (r == -ETIMEDOUT) {
        ldout(m_image_ctx.cct, 10) << this << " " << __func__
                                   << ": async_request_id=" << async_request_id
                                   << " timed out" << dendl;
        if (attempt < total_attempts) {
          notify_quiesce(async_request_id, attempt + 1, total_attempts,
                         prog_ctx, on_finish);
          return;
        }
      } else if (r == 0) {
        for (auto &[client_id, bl] : notify_response->acks) {
          if (bl.length() == 0) {
            continue;
          }
          try {
            auto iter = bl.cbegin();

            ResponseMessage response_message;
            using ceph::decode;
            decode(response_message, iter);

            if (response_message.result != -EOPNOTSUPP) {
              r = response_message.result;
            }
          } catch (const buffer::error &err) {
            r = -EINVAL;
          }
          if (r < 0) {
            break;
          }
        }
      }
      if (r < 0) {
        lderr(m_image_ctx.cct) << this << " failed to notify quiesce: "
                               << cpp_strerror(r) << dendl;
      }
      on_finish->complete(r);
    });

  bufferlist bl;
  encode(NotifyMessage(new QuiescePayload(async_request_id)), bl);
  Watcher::send_notify(bl, notify_response, on_notify);
}

template <typename I>
void ImageWatcher<I>::notify_unquiesce(uint64_t request_id, Context *on_finish) {
  ldout(m_image_ctx.cct, 10) << this << " " << __func__ << ": request_id="
                             << request_id << dendl;

  AsyncRequestId async_request_id(get_client_id(), request_id);

  send_notify(new UnquiescePayload(async_request_id), on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_metadata_set(uint64_t request_id,
                                          const std::string &key,
                                          const std::string &value,
                                          Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  notify_async_request(
      async_request_id,
      new MetadataUpdatePayload(async_request_id, key,
                                std::optional<std::string>{value}),
      m_no_op_prog_ctx,  on_finish);
}

template <typename I>
void ImageWatcher<I>::notify_metadata_remove(uint64_t request_id,
                                             const std::string &key,
                                             Context *on_finish) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  ceph_assert(m_image_ctx.exclusive_lock &&
              !m_image_ctx.exclusive_lock->is_lock_owner());

  AsyncRequestId async_request_id(get_client_id(), request_id);

  notify_async_request(
      async_request_id,
      new MetadataUpdatePayload(async_request_id, key, std::nullopt),
      m_no_op_prog_ctx,  on_finish);
}

template <typename I>
void ImageWatcher<I>::schedule_cancel_async_requests() {
  auto ctx = new LambdaContext([this](int r) {
    if (r != -ECANCELED) {
      cancel_async_requests();
    }
  });
  m_task_finisher->queue(TASK_CODE_CANCEL_ASYNC_REQUESTS, ctx);
}

template <typename I>
void ImageWatcher<I>::cancel_async_requests() {
  std::unique_lock l{m_async_request_lock};
  for (auto iter = m_async_requests.begin(); iter != m_async_requests.end(); ) {
    if (iter->second.second == nullptr) {
      // Quiesce notify request. Skip.
      iter++;
    } else {
      iter->second.first->complete(-ERESTART);
      iter = m_async_requests.erase(iter);
    }
  }
}

template <typename I>
void ImageWatcher<I>::set_owner_client_id(const ClientId& client_id) {
  ceph_assert(ceph_mutex_is_locked(m_owner_client_id_lock));
  m_owner_client_id = client_id;
  ldout(m_image_ctx.cct, 10) << this << " current lock owner: "
                             << m_owner_client_id << dendl;
}

template <typename I>
ClientId ImageWatcher<I>::get_client_id() {
  std::shared_lock l{this->m_watch_lock};
  return ClientId(m_image_ctx.md_ctx.get_instance_id(), this->m_watch_handle);
}

template <typename I>
void ImageWatcher<I>::notify_acquired_lock() {
  ldout(m_image_ctx.cct, 10) << this << " notify acquired lock" << dendl;

  ClientId client_id = get_client_id();
  {
    std::lock_guard owner_client_id_locker{m_owner_client_id_lock};
    set_owner_client_id(client_id);
  }

  send_notify(new AcquiredLockPayload(client_id));
}

template <typename I>
void ImageWatcher<I>::notify_released_lock() {
  ldout(m_image_ctx.cct, 10) << this << " notify released lock" << dendl;

  {
    std::lock_guard owner_client_id_locker{m_owner_client_id_lock};
    set_owner_client_id(ClientId());
  }

  send_notify(new ReleasedLockPayload(get_client_id()));
}

template <typename I>
void ImageWatcher<I>::schedule_request_lock(bool use_timer, int timer_delay) {
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));

  // see notify_request_lock()
  if (m_image_ctx.exclusive_lock == nullptr ||
      m_image_ctx.exclusive_lock->is_lock_owner()) {
    return;
  }

  if (is_registered()) {
    ldout(m_image_ctx.cct, 15) << this << " requesting exclusive lock" << dendl;

    auto ctx = new LambdaContext([this](int r) {
      if (r != -ECANCELED) {
        notify_request_lock();
      }
    });

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
  std::shared_lock owner_locker{m_image_ctx.owner_lock};
  std::shared_lock image_locker{m_image_ctx.image_lock};

  // ExclusiveLock state machine can be dynamically disabled or
  // race with task cancel
  if (m_image_ctx.exclusive_lock == nullptr ||
      m_image_ctx.exclusive_lock->is_lock_owner()) {
    return;
  }

  ldout(m_image_ctx.cct, 10) << this << " notify request lock" << dendl;

  notify_lock_owner(new RequestLockPayload(get_client_id(), false),
      create_context_callback<
        ImageWatcher, &ImageWatcher<I>::handle_request_lock>(this));
}

template <typename I>
void ImageWatcher<I>::handle_request_lock(int r) {
  std::shared_lock owner_locker{m_image_ctx.owner_lock};
  std::shared_lock image_locker{m_image_ctx.image_lock};

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
void ImageWatcher<I>::notify_lock_owner(Payload *payload, Context *on_finish) {
  ceph_assert(on_finish != nullptr);
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));

  bufferlist bl;
  encode(NotifyMessage(payload), bl);

  NotifyLockOwner *notify_lock_owner = NotifyLockOwner::create(
    m_image_ctx, this->m_notifier, std::move(bl), on_finish);
  notify_lock_owner->send();
}

template <typename I>
bool ImageWatcher<I>::is_new_request(const AsyncRequestId &id) const {
  ceph_assert(ceph_mutex_is_locked(m_async_request_lock));

  return m_async_pending.count(id) == 0 && m_async_complete.count(id) == 0;
}

template <typename I>
bool ImageWatcher<I>::mark_async_request_complete(const AsyncRequestId &id,
                                                  int r) {
  ceph_assert(ceph_mutex_is_locked(m_async_request_lock));

  bool found = m_async_pending.erase(id);

  auto now = ceph_clock_now();

  auto it = m_async_complete_expiration.begin();
  while (it != m_async_complete_expiration.end() && it->first < now) {
    m_async_complete.erase(it->second);
    it = m_async_complete_expiration.erase(it);
  }

  if (!m_async_complete.insert({id, r}).second) {
    for (it = m_async_complete_expiration.begin();
         it != m_async_complete_expiration.end(); it++) {
      if (it->second == id) {
        m_async_complete_expiration.erase(it);
        break;
      }
    }
  }
  auto expiration_time = now;
  expiration_time += 600;
  m_async_complete_expiration.insert({expiration_time, id});

  return found;
}

template <typename I>
Context *ImageWatcher<I>::remove_async_request(const AsyncRequestId &id) {
  std::unique_lock async_request_locker{m_async_request_lock};

  return remove_async_request(id, m_async_request_lock);
}

template <typename I>
Context *ImageWatcher<I>::remove_async_request(const AsyncRequestId &id,
                                               ceph::shared_mutex &lock) {
  ceph_assert(ceph_mutex_is_locked(lock));

  ldout(m_image_ctx.cct, 20) << __func__ << ": " << id << dendl;

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

  auto ctx = new LambdaContext([this, id](int r) {
    if (r != -ECANCELED) {
      async_request_timed_out(id);
    }
  });

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
void ImageWatcher<I>::notify_async_request(
    const AsyncRequestId &async_request_id, Payload *payload,
    ProgressContext& prog_ctx, Context *on_finish) {
  ceph_assert(on_finish != nullptr);
  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));

  ldout(m_image_ctx.cct, 10) << this << " async request: " << async_request_id
                             << dendl;

  Context *on_notify = new LambdaContext([this, async_request_id](int r) {
    if (r < 0) {
      // notification failed -- don't expect updates
      Context *on_complete = remove_async_request(async_request_id);
      if (on_complete != nullptr) {
        on_complete->complete(r);
      }
    }
  });

  Context *on_complete = new LambdaContext(
    [this, async_request_id, on_finish](int r) {
      m_task_finisher->cancel(Task(TASK_CODE_ASYNC_REQUEST, async_request_id));
      on_finish->complete(r);
    });

  {
    std::unique_lock async_request_locker{m_async_request_lock};
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
    std::unique_lock l{m_async_request_lock};
    if (is_new_request(async_request_id)) {
      m_async_pending.insert(async_request_id);
      *new_request = true;
      *prog_ctx = new RemoteProgressContext(*this, async_request_id);
      *ctx = new RemoteContext(*this, async_request_id, *prog_ctx);
    } else {
      *new_request = false;
      auto it = m_async_complete.find(async_request_id);
      if (it != m_async_complete.end()) {
        int r = it->second;
        // reset complete request expiration time
        mark_async_request_complete(async_request_id, r);
        return r;
      }
    }
  }
  return 0;
}

template <typename I>
Context *ImageWatcher<I>::prepare_quiesce_request(
    const AsyncRequestId &request, C_NotifyAck *ack_ctx) {
  std::unique_lock locker{m_async_request_lock};

  auto timeout = 2 * watcher::Notifier::NOTIFY_TIMEOUT / 1000;

  if (!is_new_request(request)) {
    auto it = m_async_requests.find(request);
    if (it != m_async_requests.end()) {
      delete it->second.first;
      it->second.first = ack_ctx;
    } else {
      auto it = m_async_complete.find(request);
      ceph_assert(it != m_async_complete.end());
      m_task_finisher->queue(new C_ResponseMessage(ack_ctx), it->second);
      // reset complete request expiration time
      mark_async_request_complete(request, it->second);
    }
    locker.unlock();

    m_task_finisher->reschedule_event_after(Task(TASK_CODE_QUIESCE, request),
                                            timeout);
    return nullptr;
  }

  m_async_pending.insert(request);
  m_async_requests[request] = AsyncRequest(ack_ctx, nullptr);
  m_async_op_tracker.start_op();

  return new LambdaContext(
    [this, request, timeout](int r) {
      auto unquiesce_ctx = new LambdaContext(
        [this, request](int r) {
          if (r == 0) {
            ldout(m_image_ctx.cct, 10) << this << " quiesce request "
                                       << request << " timed out" << dendl;
          }

          auto on_finish = new LambdaContext(
            [this](int r) {
              m_async_op_tracker.finish_op();
            });

          m_image_ctx.state->notify_unquiesce(on_finish);
        });

      m_task_finisher->add_event_after(Task(TASK_CODE_QUIESCE, request),
                                       timeout, unquiesce_ctx);

      std::unique_lock async_request_locker{m_async_request_lock};
      mark_async_request_complete(request, r);
      auto ctx = remove_async_request(request, m_async_request_lock);
      async_request_locker.unlock();
      if (ctx != nullptr) {
        ctx = new C_ResponseMessage(static_cast<C_NotifyAck *>(ctx));
        ctx->complete(r);
      } else {
        m_task_finisher->cancel(Task(TASK_CODE_QUIESCE, request));
      }
    });
}

template <typename I>
void ImageWatcher<I>::prepare_unquiesce_request(const AsyncRequestId &request) {
  {
    std::unique_lock async_request_locker{m_async_request_lock};
    auto it = m_async_complete.find(request);
    if (it == m_async_complete.end()) {
      ldout(m_image_ctx.cct, 20) << this << " " << request
                                 << ": not found in complete" << dendl;
      return;
    }
    // reset complete request expiration time
    mark_async_request_complete(request, it->second);
  }

  bool canceled = m_task_finisher->cancel(Task(TASK_CODE_QUIESCE, request));
  if (!canceled) {
    ldout(m_image_ctx.cct, 20) << this << " " << request
                               << ": timer task not found" << dendl;
  }
}

template <typename I>
void ImageWatcher<I>::cancel_quiesce_requests() {
  std::unique_lock l{m_async_request_lock};
  for (auto it = m_async_requests.begin(); it != m_async_requests.end(); ) {
    if (it->second.second == nullptr) {
      // Quiesce notify request.
      mark_async_request_complete(it->first, 0);
      delete it->second.first;
      it = m_async_requests.erase(it);
    } else {
      it++;
    }
  }
}

template <typename I>
bool ImageWatcher<I>::handle_operation_request(
    const AsyncRequestId& async_request_id,
    exclusive_lock::OperationRequestType request_type, Operation operation,
    std::function<void(ProgressContext &prog_ctx, Context*)> execute,
    C_NotifyAck *ack_ctx) {
  std::shared_lock owner_locker{m_image_ctx.owner_lock};

  if (m_image_ctx.exclusive_lock != nullptr) {
    int r = 0;
    if (m_image_ctx.exclusive_lock->accept_request(request_type, &r)) {
      bool new_request;
      Context *ctx;
      ProgressContext *prog_ctx;
      bool complete;
      if (async_request_id) {
        r = prepare_async_request(async_request_id, &new_request, &ctx,
                                  &prog_ctx);
        encode(ResponseMessage(r), ack_ctx->out);
        complete = true;
      } else {
        new_request = true;
        ctx = new C_ResponseMessage(ack_ctx);
        prog_ctx = &m_no_op_prog_ctx;
        complete = false;
      }
      if (r == 0 && new_request) {
        ctx = new LambdaContext(
          [this, operation, ctx](int r) {
            m_image_ctx.operations->finish_op(operation, r);
            ctx->complete(r);
          });
        ctx = new LambdaContext(
          [this, execute, prog_ctx, ctx](int r) {
            if (r < 0) {
              ctx->complete(r);
              return;
            }
            std::shared_lock l{m_image_ctx.owner_lock};
            execute(*prog_ctx, ctx);
          });
        m_image_ctx.operations->start_op(operation, ctx);
      }
      return complete;
    } else if (r < 0) {
      encode(ResponseMessage(r), ack_ctx->out);
    }
  }
  return true;
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
    std::lock_guard owner_client_id_locker{m_owner_client_id_lock};
    if (payload.client_id == m_owner_client_id) {
      cancel_async_requests = false;
    }
    set_owner_client_id(payload.client_id);
  }

  std::shared_lock owner_locker{m_image_ctx.owner_lock};
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
    std::lock_guard l{m_owner_client_id_lock};
    if (payload.client_id != m_owner_client_id) {
      ldout(m_image_ctx.cct, 10) << this << " unexpected owner: "
                                 << payload.client_id << " != "
                                 << m_owner_client_id << dendl;
      cancel_async_requests = false;
    } else {
      set_owner_client_id(ClientId());
    }
  }

  std::shared_lock owner_locker{m_image_ctx.owner_lock};
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

  std::shared_lock l{m_image_ctx.owner_lock};
  if (m_image_ctx.exclusive_lock != nullptr &&
      m_image_ctx.exclusive_lock->is_lock_owner()) {
    int r = 0;
    bool accept_request = m_image_ctx.exclusive_lock->accept_request(
      exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL, &r);

    if (accept_request) {
      ceph_assert(r == 0);
      std::lock_guard owner_client_id_locker{m_owner_client_id_lock};
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
  std::shared_lock l{m_async_request_lock};
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
  ldout(m_image_ctx.cct, 10) << this << " remote flatten request: "
                             << payload.async_request_id << dendl;

  return handle_operation_request(
      payload.async_request_id, exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
      OPERATION_FLATTEN, std::bind(&Operations<I>::execute_flatten,
                                   m_image_ctx.operations,
                                   std::placeholders::_1,
                                   std::placeholders::_2),
      ack_ctx);
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const ResizePayload &payload,
				     C_NotifyAck *ack_ctx) {
  ldout(m_image_ctx.cct, 10) << this << " remote resize request: "
                             << payload.async_request_id << " "
                             << payload.size << " "
                             << payload.allow_shrink << dendl;

  return handle_operation_request(
      payload.async_request_id, exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
      OPERATION_RESIZE, std::bind(&Operations<I>::execute_resize,
                                  m_image_ctx.operations, payload.size,
                                  payload.allow_shrink, std::placeholders::_1,
                                  std::placeholders::_2, 0), ack_ctx);
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const SnapCreatePayload &payload,
			             C_NotifyAck *ack_ctx) {
  ldout(m_image_ctx.cct, 10) << this << " remote snap_create request: "
                             << payload.async_request_id << " "
                             << payload.snap_namespace << " "
                             << payload.snap_name << " "
                             << payload.flags << dendl;

  auto request_type = exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL;

  // rbd-mirror needs to accept forced promotion orphan snap create requests
  auto mirror_ns = std::get_if<cls::rbd::MirrorSnapshotNamespace>(
    &payload.snap_namespace);
  if (mirror_ns != nullptr && mirror_ns->is_orphan()) {
    request_type = exclusive_lock::OPERATION_REQUEST_TYPE_FORCE_PROMOTION;
  }

  return handle_operation_request(
      payload.async_request_id, request_type,
      OPERATION_SNAP_CREATE, std::bind(&Operations<I>::execute_snap_create,
                                       m_image_ctx.operations,
                                       payload.snap_namespace,
                                       payload.snap_name, std::placeholders::_2,
                                       0, payload.flags, std::placeholders::_1),
      ack_ctx);
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const SnapRenamePayload &payload,
			             C_NotifyAck *ack_ctx) {
  ldout(m_image_ctx.cct, 10) << this << " remote snap_rename request: "
                             << payload.async_request_id << " "
                             << payload.snap_id << " to "
                             << payload.snap_name << dendl;

  return handle_operation_request(
      payload.async_request_id, exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
      OPERATION_SNAP_RENAME, std::bind(&Operations<I>::execute_snap_rename,
                                       m_image_ctx.operations, payload.snap_id,
                                       payload.snap_name,
                                       std::placeholders::_2), ack_ctx);
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const SnapRemovePayload &payload,
			             C_NotifyAck *ack_ctx) {
  ldout(m_image_ctx.cct, 10) << this << " remote snap_remove request: "
                             << payload.snap_name << dendl;

  auto request_type = exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL;
  if (cls::rbd::get_snap_namespace_type(payload.snap_namespace) ==
      cls::rbd::SNAPSHOT_NAMESPACE_TYPE_TRASH) {
    request_type = exclusive_lock::OPERATION_REQUEST_TYPE_TRASH_SNAP_REMOVE;
  }

  return handle_operation_request(
      payload.async_request_id, request_type, OPERATION_SNAP_REMOVE,
      std::bind(&Operations<I>::execute_snap_remove, m_image_ctx.operations,
                payload.snap_namespace, payload.snap_name,
                std::placeholders::_2), ack_ctx);
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const SnapProtectPayload& payload,
                                     C_NotifyAck *ack_ctx) {
    ldout(m_image_ctx.cct, 10) << this << " remote snap_protect request: "
                               << payload.async_request_id << " "
                               << payload.snap_name << dendl;

    return handle_operation_request(
      payload.async_request_id, exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
      OPERATION_SNAP_PROTECT, std::bind(&Operations<I>::execute_snap_protect,
                                        m_image_ctx.operations,
                                        payload.snap_namespace,
                                        payload.snap_name,
                                        std::placeholders::_2), ack_ctx);
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const SnapUnprotectPayload& payload,
                                     C_NotifyAck *ack_ctx) {
    ldout(m_image_ctx.cct, 10) << this << " remote snap_unprotect request: "
                               << payload.async_request_id << " "
                               << payload.snap_name << dendl;

    return handle_operation_request(
      payload.async_request_id, exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
      OPERATION_SNAP_UNPROTECT, std::bind(&Operations<I>::execute_snap_unprotect,
                                          m_image_ctx.operations,
                                          payload.snap_namespace,
                                          payload.snap_name,
                                          std::placeholders::_2), ack_ctx);
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const RebuildObjectMapPayload& payload,
                                     C_NotifyAck *ack_ctx) {
  ldout(m_image_ctx.cct, 10) << this << " remote rebuild object map request: "
                             << payload.async_request_id << dendl;

  return handle_operation_request(
      payload.async_request_id, exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
      OPERATION_REBUILD_OBJECT_MAP,
      std::bind(&Operations<I>::execute_rebuild_object_map,
                m_image_ctx.operations, std::placeholders::_1,
                std::placeholders::_2), ack_ctx);
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const RenamePayload& payload,
                                     C_NotifyAck *ack_ctx) {
  ldout(m_image_ctx.cct, 10) << this << " remote rename request: "
                             << payload.async_request_id << " "
                             << payload.image_name << dendl;

  return handle_operation_request(
      payload.async_request_id, exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
      OPERATION_RENAME, std::bind(&Operations<I>::execute_rename,
                                  m_image_ctx.operations, payload.image_name,
                                  std::placeholders::_2), ack_ctx);
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const UpdateFeaturesPayload& payload,
                                     C_NotifyAck *ack_ctx) {
  ldout(m_image_ctx.cct, 10) << this << " remote update_features request: "
                             << payload.async_request_id << " "
                             << payload.features << " "
                             << (payload.enabled ? "enabled" : "disabled")
                             << dendl;

  return handle_operation_request(
      payload.async_request_id, exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
      OPERATION_UPDATE_FEATURES,
      std::bind(&Operations<I>::execute_update_features, m_image_ctx.operations,
                payload.features, payload.enabled, std::placeholders::_2, 0),
      ack_ctx);
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const MigratePayload &payload,
				     C_NotifyAck *ack_ctx) {
  ldout(m_image_ctx.cct, 10) << this << " remote migrate request: "
                             << payload.async_request_id << dendl;

  return handle_operation_request(
      payload.async_request_id, exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
      OPERATION_MIGRATE, std::bind(&Operations<I>::execute_migrate,
                                   m_image_ctx.operations,
                                   std::placeholders::_1,
                                   std::placeholders::_2), ack_ctx);
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const SparsifyPayload &payload,
				     C_NotifyAck *ack_ctx) {
  ldout(m_image_ctx.cct, 10) << this << " remote sparsify request: "
                             << payload.async_request_id << dendl;

  return handle_operation_request(
      payload.async_request_id, exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
      OPERATION_SPARSIFY, std::bind(&Operations<I>::execute_sparsify,
                                    m_image_ctx.operations,
                                    payload.sparse_size, std::placeholders::_1,
                                    std::placeholders::_2), ack_ctx);
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const MetadataUpdatePayload &payload,
                                     C_NotifyAck *ack_ctx) {
  if (payload.value) {
    ldout(m_image_ctx.cct, 10) << this << " remote metadata_set request: "
                               << payload.async_request_id << " "
                               << "key=" << payload.key << ", value="
                               << *payload.value << dendl;

    return handle_operation_request(
        payload.async_request_id,
        exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
        OPERATION_METADATA_UPDATE,
        std::bind(&Operations<I>::execute_metadata_set,
                  m_image_ctx.operations, payload.key, *payload.value,
                  std::placeholders::_2),
        ack_ctx);
  } else {
    ldout(m_image_ctx.cct, 10) << this << " remote metadata_remove request: "
                               << payload.async_request_id << " "
                               << "key=" << payload.key << dendl;

    return handle_operation_request(
        payload.async_request_id,
        exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL,
        OPERATION_METADATA_UPDATE,
        std::bind(&Operations<I>::execute_metadata_remove,
                  m_image_ctx.operations, payload.key, std::placeholders::_2),
        ack_ctx);
  }
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const QuiescePayload &payload,
				     C_NotifyAck *ack_ctx) {
  auto on_finish = prepare_quiesce_request(payload.async_request_id, ack_ctx);
  if (on_finish == nullptr) {
    ldout(m_image_ctx.cct, 10) << this << " duplicate quiesce request: "
                               << payload.async_request_id << dendl;
    return false;
  }

  ldout(m_image_ctx.cct, 10) << this << " quiesce request: "
                             << payload.async_request_id << dendl;
  m_image_ctx.state->notify_quiesce(on_finish);
  return false;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const UnquiescePayload &payload,
				     C_NotifyAck *ack_ctx) {
  ldout(m_image_ctx.cct, 10) << this << " unquiesce request: "
                             << payload.async_request_id << dendl;

  prepare_unquiesce_request(payload.async_request_id);
  return true;
}

template <typename I>
bool ImageWatcher<I>::handle_payload(const UnknownPayload &payload,
			             C_NotifyAck *ack_ctx) {
  std::shared_lock l{m_image_ctx.owner_lock};
  if (m_image_ctx.exclusive_lock != nullptr) {
    int r;
    if (m_image_ctx.exclusive_lock->accept_request(
          exclusive_lock::OPERATION_REQUEST_TYPE_GENERAL, &r) || r < 0) {
      encode(ResponseMessage(-EOPNOTSUPP), ack_ctx->out);
    }
  }
  return true;
}

template <typename I>
void ImageWatcher<I>::process_payload(uint64_t notify_id, uint64_t handle,
                                      Payload *payload) {
  auto ctx = new Watcher::C_NotifyAck(this, notify_id, handle);
  bool complete;

  switch (payload->get_notify_op()) {
  case NOTIFY_OP_ACQUIRED_LOCK:
    complete = handle_payload(*(static_cast<AcquiredLockPayload *>(payload)),
                              ctx);
    break;
  case NOTIFY_OP_RELEASED_LOCK:
    complete = handle_payload(*(static_cast<ReleasedLockPayload *>(payload)),
                              ctx);
    break;
  case NOTIFY_OP_REQUEST_LOCK:
    complete = handle_payload(*(static_cast<RequestLockPayload *>(payload)),
                              ctx);
    break;
  case NOTIFY_OP_HEADER_UPDATE:
    complete = handle_payload(*(static_cast<HeaderUpdatePayload *>(payload)),
                              ctx);
    break;
  case NOTIFY_OP_ASYNC_PROGRESS:
    complete = handle_payload(*(static_cast<AsyncProgressPayload *>(payload)),
                              ctx);
    break;
  case NOTIFY_OP_ASYNC_COMPLETE:
    complete = handle_payload(*(static_cast<AsyncCompletePayload *>(payload)),
                              ctx);
    break;
  case NOTIFY_OP_FLATTEN:
    complete = handle_payload(*(static_cast<FlattenPayload *>(payload)), ctx);
    break;
  case NOTIFY_OP_RESIZE:
    complete = handle_payload(*(static_cast<ResizePayload *>(payload)), ctx);
    break;
  case NOTIFY_OP_SNAP_CREATE:
    complete = handle_payload(*(static_cast<SnapCreatePayload *>(payload)),
                              ctx);
    break;
  case NOTIFY_OP_SNAP_REMOVE:
    complete = handle_payload(*(static_cast<SnapRemovePayload *>(payload)),
                              ctx);
    break;
  case NOTIFY_OP_SNAP_RENAME:
    complete = handle_payload(*(static_cast<SnapRenamePayload *>(payload)),
                              ctx);
    break;
  case NOTIFY_OP_SNAP_PROTECT:
    complete = handle_payload(*(static_cast<SnapProtectPayload *>(payload)),
                              ctx);
    break;
  case NOTIFY_OP_SNAP_UNPROTECT:
    complete = handle_payload(*(static_cast<SnapUnprotectPayload *>(payload)),
                              ctx);
    break;
  case NOTIFY_OP_REBUILD_OBJECT_MAP:
    complete = handle_payload(*(static_cast<RebuildObjectMapPayload *>(payload)),
                              ctx);
    break;
  case NOTIFY_OP_RENAME:
    complete = handle_payload(*(static_cast<RenamePayload *>(payload)), ctx);
    break;
  case NOTIFY_OP_UPDATE_FEATURES:
    complete = handle_payload(*(static_cast<UpdateFeaturesPayload *>(payload)),
                              ctx);
    break;
  case NOTIFY_OP_MIGRATE:
    complete = handle_payload(*(static_cast<MigratePayload *>(payload)), ctx);
    break;
  case NOTIFY_OP_SPARSIFY:
    complete = handle_payload(*(static_cast<SparsifyPayload *>(payload)), ctx);
    break;
  case NOTIFY_OP_QUIESCE:
    complete = handle_payload(*(static_cast<QuiescePayload *>(payload)), ctx);
    break;
  case NOTIFY_OP_UNQUIESCE:
    complete = handle_payload(*(static_cast<UnquiescePayload *>(payload)), ctx);
    break;
  case NOTIFY_OP_METADATA_UPDATE:
    complete = handle_payload(*(static_cast<MetadataUpdatePayload *>(payload)), ctx);
    break;
  default:
    ceph_assert(payload->get_notify_op() == static_cast<NotifyOp>(-1));
    complete = handle_payload(*(static_cast<UnknownPayload *>(payload)), ctx);
  }

  if (complete) {
    ctx->complete(0);
  }
}

template <typename I>
void ImageWatcher<I>::handle_notify(uint64_t notify_id, uint64_t handle,
			            uint64_t notifier_id, bufferlist &bl) {
  NotifyMessage notify_message;
  if (bl.length() == 0) {
    // legacy notification for header updates
    notify_message = NotifyMessage(new HeaderUpdatePayload());
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

    m_image_ctx.state->refresh(
      new C_ProcessPayload(this, notify_id, handle,
                           std::move(notify_message.payload)));
  } else {
    process_payload(notify_id, handle, notify_message.payload.get());
  }
}

template <typename I>
void ImageWatcher<I>::handle_error(uint64_t handle, int err) {
  lderr(m_image_ctx.cct) << this << " image watch failed: " << handle << ", "
                         << cpp_strerror(err) << dendl;

  {
    std::lock_guard l{m_owner_client_id_lock};
    set_owner_client_id(ClientId());
  }

  Watcher::handle_error(handle, err);
}

template <typename I>
void ImageWatcher<I>::handle_rewatch_complete(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  {
    std::shared_lock owner_locker{m_image_ctx.owner_lock};
    if (m_image_ctx.exclusive_lock != nullptr) {
      // update the lock cookie with the new watch handle
      m_image_ctx.exclusive_lock->reacquire_lock(nullptr);
    }
  }

  // image might have been updated while we didn't have active watch
  handle_payload(HeaderUpdatePayload(), nullptr);
}

template <typename I>
void ImageWatcher<I>::send_notify(Payload *payload, Context *ctx) {
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
