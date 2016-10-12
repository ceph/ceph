// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/managed_lock/LockWatcher.h"
#include "librbd/managed_lock/Policy.h"
#include "librbd/Lock.h"
#include "librbd/managed_lock/NotifyLockOwner.h"
#include "librbd/TaskFinisher.h"
#include "common/Mutex.h"
#include "librbd/Utils.h"
#include "common/errno.h"
#include <boost/bind.hpp>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::LockWatcher: "

namespace librbd {

using util::create_context_callback;
using watcher::ResponseMessage;

namespace managed_lock {

static const double	RETRY_DELAY_SECONDS = 1.0;

const watcher::TaskCode LockWatcher::TASK_CODE_REQUEST_LOCK = watcher::TaskCode(1);

LockWatcher::LockWatcher(Lock *managed_lock)
  : watcher::Watcher<LockPayload>(managed_lock->io_ctx(),
                                  managed_lock->work_queue(),
                                  managed_lock->oid()),
    m_managed_lock(managed_lock),
    m_cct(reinterpret_cast<CephContext *>(managed_lock->io_ctx().cct())),
    m_owner_client_id_lock(util::unique_lock_name(
                          "librbd::LockWatcher::m_owner_client_id_lock", this))
{
}

LockWatcher::~LockWatcher()
{
}

void LockWatcher::notify_acquired_lock() {
  ldout(m_cct, 10) << this << " notify acquired lock" << dendl;

  ClientId client_id = get_client_id();
  {
    Mutex::Locker owner_client_id_locker(m_owner_client_id_lock);
    set_owner_client_id(client_id);
  }

  send_notify(AcquiredLockPayload(client_id));
}

void LockWatcher::notify_released_lock() {
  ldout(m_cct, 10) << this << " notify released lock" << dendl;

  {
    Mutex::Locker owner_client_id_locker(m_owner_client_id_lock);
    set_owner_client_id(ClientId());
  }

  send_notify(ReleasedLockPayload(get_client_id()));
}

void LockWatcher::schedule_request_lock(bool use_timer, int timer_delay) {
  assert(m_managed_lock && !m_managed_lock->is_lock_owner());

  RWLock::RLocker watch_locker(m_watch_lock);
  if (m_watch_state == WATCH_STATE_REGISTERED) {
    ldout(m_cct, 15) << this << " requesting lock" << dendl;

    FunctionContext *ctx = new FunctionContext(
      boost::bind(&LockWatcher::notify_request_lock, this));
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

void LockWatcher::notify_request_lock() {
  assert(!m_managed_lock->is_lock_owner());

  ldout(m_cct, 10) << this << " notify request lock" << dendl;

  bufferlist bl;
  encode(NotifyMessage(RequestLockPayload(get_client_id(), false)), bl);
  notify_lock_owner(std::move(bl), create_context_callback<
    LockWatcher, &LockWatcher::handle_request_lock>(this));
}

void LockWatcher::handle_request_lock(int r) {
  if (r == -ETIMEDOUT) {
    ldout(m_cct, 5) << this << " timed out requesting lock: retrying"
                    << dendl;

    // treat this is a dead client -- so retest acquiring the lock
    m_managed_lock->handle_peer_notification();
  } else if (r < 0) {
    lderr(m_cct) << this << " error requesting lock: " << cpp_strerror(r)
                 << dendl;
    schedule_request_lock(true);
  } else {
    // lock owner acked -- but resend if we don't see them release the lock
    int retry_timeout = m_cct->_conf->client_notify_timeout;
    ldout(m_cct, 15) << this << " will retry in " << retry_timeout
                     << " seconds" << dendl;
    schedule_request_lock(true, retry_timeout);
  }
}

void LockWatcher::notify_lock_owner(bufferlist &&bl, Context *on_finish) {
  assert(on_finish != nullptr);
  NotifyLockOwner *notify_lock_owner = NotifyLockOwner::create(
      m_cct, m_notifier, std::move(bl), on_finish);
  notify_lock_owner->send();
}

bool LockWatcher::handle_payload(const AcquiredLockPayload &payload,
                                 C_NotifyAck *ack_ctx) {
  ldout(m_cct, 10) << this << " locked announcement" << dendl;

  if (payload.client_id.is_valid()) {
    Mutex::Locker owner_client_id_locker(m_owner_client_id_lock);
    set_owner_client_id(payload.client_id);
  }

  // potentially wake up the exclusive lock state machine now that
  // a lock owner has advertised itself
  m_managed_lock->handle_peer_notification();
  return true;
}

bool LockWatcher::handle_payload(const ReleasedLockPayload &payload,
                                 C_NotifyAck *ack_ctx) {
  ldout(m_cct, 10) << this << " exclusive lock released" << dendl;

  if (payload.client_id.is_valid()) {
    Mutex::Locker l(m_owner_client_id_lock);
    if (payload.client_id != m_owner_client_id) {
      ldout(m_cct, 10) << this << " unexpected owner: " << payload.client_id
                       << " != " << m_owner_client_id << dendl;
    } else {
      set_owner_client_id(ClientId());
    }
  }

  // alert the exclusive lock state machine that the lock is available
  if (!m_managed_lock->is_lock_owner()) {
    m_task_finisher->cancel(TASK_CODE_REQUEST_LOCK);
    m_managed_lock->handle_peer_notification();
  }
  return true;
}

bool LockWatcher::handle_payload(const RequestLockPayload &payload,
                                 C_NotifyAck *ack_ctx) {
  ldout(m_cct, 10) << this << " lock requested" << dendl;
  if (payload.client_id == get_client_id()) {
    return true;
  }

  if (m_managed_lock->is_lock_owner()) {
    int r = 0;
    Mutex::Locker owner_client_id_locker(m_owner_client_id_lock);
    if (!m_owner_client_id.is_valid()) {
      return true;
    }

    Policy *policy = m_managed_lock->policy();
    if (!policy) {
      return true;
    }

    ldout(m_cct, 10) << this << " queuing release of exclusive lock" << dendl;

    r = m_managed_lock->policy()->lock_requested(payload.force);
    ::encode(ResponseMessage(r), ack_ctx->out);
  }
  return true;
}

void LockWatcher::set_owner_client_id(const ClientId& client_id) {
  assert(m_owner_client_id_lock.is_locked());
  m_owner_client_id = client_id;
  ldout(m_cct, 10) << this << " current lock owner: " << m_owner_client_id
                   << dendl;
}

ClientId LockWatcher::get_client_id() {
  RWLock::RLocker l(m_watch_lock);
  return ClientId(m_ioctx.get_instance_id(), m_watch_handle);
}

void LockWatcher::handle_error(uint64_t handle, int err) {
  lderr(m_cct) << this << " watch failed: " << handle << ", "
               << cpp_strerror(err) << dendl;

  {
    Mutex::Locker l(m_owner_client_id_lock);
    set_owner_client_id(ClientId());
  }
  watcher::Watcher<LockPayload>::handle_error(handle, err);
}

} // namespace managed_lock
} // namespace librbd
