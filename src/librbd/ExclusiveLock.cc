// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/ExclusiveLock.h"
#include "cls/lock/cls_lock_client.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/AioImageRequestWQ.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/ImageWatcher.h"
#include "librbd/Lock.h"
#include "librbd/managed_lock/LockWatcher.h"
#include "librbd/Utils.h"
#include "librbd/exclusive_lock/AcquireRequest.h"
#include "librbd/exclusive_lock/ReleaseRequest.h"
#include <sstream>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ExclusiveLock: "

namespace librbd {

using namespace exclusive_lock;

namespace {

template <typename R>
struct C_SendRequest : public Context {
  R* request;
  explicit C_SendRequest(R* request) : request(request) {
  }
  virtual void finish(int r) override {
    request->send();
  }
};

} // anonymous namespace

template <typename I>
ExclusiveLock<I>::ExclusiveLock(I &image_ctx)
  : m_image_ctx(image_ctx),
    m_managed_lock(new Lock<managed_lock::LockWatcher>(image_ctx.md_ctx,
                                                       image_ctx.header_oid)),
    m_lock(util::unique_lock_name("librbd::ExclusiveLock::m_lock", this)),
    m_state(STATE_UNINITIALIZED) {
}

template <typename I>
ExclusiveLock<I>::~ExclusiveLock() {
  assert(m_state == STATE_UNINITIALIZED || m_state == STATE_SHUTDOWN);
  delete m_managed_lock;
}

template <typename I>
bool ExclusiveLock<I>::is_lock_owner() const {
  /*Mutex::Locker locker(m_lock);

  bool lock_owner;
  switch (m_state) {
  case STATE_LOCKED:
  case STATE_REACQUIRING:
    lock_owner = true;
    break;
  default:
    lock_owner = false;
    break;
  }

  ldout(m_image_ctx.cct, 20) << this << " " << __func__ << "=" << lock_owner
                             << dendl;
  return lock_owner;*/
  return m_managed_lock->is_lock_owner();
}

template <typename I>
bool ExclusiveLock<I>::accept_requests(int *ret_val) const {
  Mutex::Locker locker(m_lock);

  bool accept_requests = (!is_shutdown() && m_state == STATE_LOCKED &&
                          m_request_blocked_count == 0);
  *ret_val = m_request_blocked_ret_val;

  ldout(m_image_ctx.cct, 20) << this << " " << __func__ << "="
                             << accept_requests << dendl;
  return accept_requests;
}

template <typename I>
void ExclusiveLock<I>::block_requests(int r) {
  Mutex::Locker locker(m_lock);
  m_request_blocked_count++;
  if (m_request_blocked_ret_val == 0) {
    m_request_blocked_ret_val = r;
  }

  ldout(m_image_ctx.cct, 20) << this << " " << __func__ << dendl;
}

template <typename I>
void ExclusiveLock<I>::unblock_requests() {
  Mutex::Locker locker(m_lock);
  assert(m_request_blocked_count > 0);
  m_request_blocked_count--;
  if (m_request_blocked_count == 0) {
    m_request_blocked_ret_val = 0;
  }

  ldout(m_image_ctx.cct, 20) << this << " " << __func__ << dendl;
}

template <typename I>
void ExclusiveLock<I>::init(uint64_t features, Context *on_init) {
  assert(m_image_ctx.owner_lock.is_locked());
  ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;

  {
    Mutex::Locker locker(m_lock);
    assert(m_state == STATE_UNINITIALIZED);
    m_state = STATE_INITIALIZING;
  }

  m_image_ctx.aio_work_queue->block_writes(new C_InitComplete(this, on_init));
  if ((features & RBD_FEATURE_JOURNALING) != 0) {
    m_image_ctx.aio_work_queue->set_require_lock_on_read();
  }
}

template <typename I>
void ExclusiveLock<I>::shut_down(Context *on_shut_down) {
  ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;

  {
    Mutex::Locker locker(m_lock);
    assert(!is_shutdown());
    execute_action(ACTION_SHUT_DOWN, on_shut_down);
  }

  // if stalled in request state machine -- abort
  handle_peer_notification();
}

template <typename I>
void ExclusiveLock<I>::try_lock(Context *on_tried_lock) {
  int r = 0;
  {
    Mutex::Locker locker(m_lock);
    assert(m_image_ctx.owner_lock.is_locked());
    if (is_shutdown()) {
      r = -ESHUTDOWN;
    } else if (m_state != STATE_LOCKED || !m_actions_contexts.empty()) {
      ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;
      execute_action(ACTION_TRY_LOCK, on_tried_lock);
      return;
    }
  }

  on_tried_lock->complete(r);
}

template <typename I>
void ExclusiveLock<I>::request_lock(Context *on_locked) {
  int r = 0;
  {
    Mutex::Locker locker(m_lock);
    assert(m_image_ctx.owner_lock.is_locked());
    if (is_shutdown()) {
      r = -ESHUTDOWN;
    } else if (m_state != STATE_LOCKED || !m_actions_contexts.empty()) {
      ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;
      execute_action(ACTION_REQUEST_LOCK, on_locked);
      return;
    }
  }

  if (on_locked != nullptr) {
    on_locked->complete(r);
  }
}

template <typename I>
void ExclusiveLock<I>::release_lock(Context *on_released) {
  int r = 0;
  {
    Mutex::Locker locker(m_lock);
    assert(m_image_ctx.owner_lock.is_locked());
    if (is_shutdown()) {
      r = -ESHUTDOWN;
    } else if (m_state != STATE_UNLOCKED || !m_actions_contexts.empty()) {
      ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;
      execute_action(ACTION_RELEASE_LOCK, on_released);
      return;
    }
  }

  on_released->complete(r);
}

template <typename I>
void ExclusiveLock<I>::reacquire_lock(Context *on_reacquired) {
  {
    Mutex::Locker locker(m_lock);
    assert(m_image_ctx.owner_lock.is_locked());

    if (!is_shutdown() &&
        (m_state == STATE_LOCKED ||
        m_state == STATE_ACQUIRING)) {
      // interlock the lock operation with other image state ops
      ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;
      execute_action(ACTION_REACQUIRE_LOCK, on_reacquired);
      return;
    }
  }

  // ignore request if shutdown or not in a locked-related state
  if (on_reacquired != nullptr) {
    on_reacquired->complete(0);
  }
}

template <typename I>
void ExclusiveLock<I>::handle_peer_notification() {
  m_managed_lock->handle_peer_notification();
}

template <typename I>
void ExclusiveLock<I>::assert_header_locked(librados::ObjectWriteOperation *op) {
  m_managed_lock->assert_locked(op, LOCK_EXCLUSIVE);
}

template <typename I>
bool ExclusiveLock<I>::is_transition_state() const {
  switch (m_state) {
  case STATE_INITIALIZING:
  case STATE_ACQUIRING:
  case STATE_REACQUIRING:
  case STATE_RELEASING:
  case STATE_SHUTTING_DOWN:
    return true;
  case STATE_UNINITIALIZED:
  case STATE_UNLOCKED:
  case STATE_LOCKED:
  case STATE_SHUTDOWN:
    break;
  }
  return false;
}

template <typename I>
void ExclusiveLock<I>::append_context(Action action, Context *ctx) {
  assert(m_lock.is_locked());

  for (auto &action_ctxs : m_actions_contexts) {
    if (action == action_ctxs.first) {
      if (ctx != nullptr) {
        action_ctxs.second.push_back(ctx);
      }
      return;
    }
  }

  Contexts contexts;
  if (ctx != nullptr) {
    contexts.push_back(ctx);
  }
  m_actions_contexts.push_back({action, std::move(contexts)});
}

template <typename I>
void ExclusiveLock<I>::execute_action(Action action, Context *ctx) {
  assert(m_lock.is_locked());

  append_context(action, ctx);
  if (!is_transition_state()) {
    execute_next_action();
  }
}

template <typename I>
void ExclusiveLock<I>::execute_next_action() {
  assert(m_lock.is_locked());
  assert(!m_actions_contexts.empty());
  switch (get_active_action()) {
  case ACTION_TRY_LOCK:
  case ACTION_REQUEST_LOCK:
    send_acquire_lock();
    break;
  case ACTION_REACQUIRE_LOCK:
    send_reacquire_lock();
    break;
  case ACTION_RELEASE_LOCK:
    send_release_lock();
    break;
  case ACTION_SHUT_DOWN:
    send_shutdown();
    break;
  default:
    assert(false);
    break;
  }
}

template <typename I>
typename ExclusiveLock<I>::Action ExclusiveLock<I>::get_active_action() const {
  assert(m_lock.is_locked());
  assert(!m_actions_contexts.empty());
  return m_actions_contexts.front().first;
}

template <typename I>
void ExclusiveLock<I>::complete_active_action(State next_state, int r) {
  assert(m_lock.is_locked());
  assert(!m_actions_contexts.empty());

  ActionContexts action_contexts(std::move(m_actions_contexts.front()));
  m_actions_contexts.pop_front();
  m_state = next_state;

  m_lock.Unlock();
  for (auto ctx : action_contexts.second) {
    ctx->complete(r);
  }
  m_lock.Lock();

  if (!is_transition_state() && !m_actions_contexts.empty()) {
    execute_next_action();
  }
}

template <typename I>
bool ExclusiveLock<I>::is_shutdown() const {
  assert(m_lock.is_locked());

  return ((m_state == STATE_SHUTDOWN) ||
          (!m_actions_contexts.empty() &&
           m_actions_contexts.back().first == ACTION_SHUT_DOWN));
}

template <typename I>
void ExclusiveLock<I>::handle_init_complete() {
  ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;

  Mutex::Locker locker(m_lock);
  m_state = STATE_UNLOCKED;
}

template <typename I>
void ExclusiveLock<I>::send_acquire_lock() {
  assert(m_lock.is_locked());
  if (m_state == STATE_LOCKED) {
    complete_active_action(STATE_LOCKED, 0);
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;
  m_state = STATE_ACQUIRING;

  bool try_lock = get_active_action() == ACTION_TRY_LOCK;
  using el = ExclusiveLock<I>;
  AcquireRequest<I>* req = AcquireRequest<I>::create(
      m_image_ctx, m_managed_lock,
      util::create_context_callback<el, &el::handle_acquire_lock>(this),
      try_lock);
  m_image_ctx.op_work_queue->queue(new C_SendRequest<AcquireRequest<I> >(req),
                                   0);
}

template <typename I>
void ExclusiveLock<I>::handle_acquire_lock(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  {
    m_lock.Lock();
    assert(m_state == STATE_ACQUIRING);

    Action action = get_active_action();
    assert(action == ACTION_TRY_LOCK || action == ACTION_REQUEST_LOCK);
    m_lock.Unlock();
  }

  bool is_locked = m_managed_lock->is_locked();

  State next_state = (r < 0 || !is_locked ? STATE_UNLOCKED : STATE_LOCKED);

  if (next_state == STATE_LOCKED) {
    m_image_ctx.aio_work_queue->clear_require_lock_on_read();
    m_image_ctx.aio_work_queue->unblock_writes();
  }

  Mutex::Locker locker(m_lock);
  complete_active_action(next_state, r);
}

template <typename I>
void ExclusiveLock<I>::send_reacquire_lock() {
  assert(m_lock.is_locked());

  m_state = STATE_REACQUIRING;

  m_lock.Unlock();
  using el = ExclusiveLock<I>;
  m_managed_lock->reacquire_lock(
    util::create_context_callback<el, &el::handle_reacquire_lock>(this));
  m_lock.Lock();
}

template <typename I>
void ExclusiveLock<I>::handle_reacquire_lock(int r) {
  Mutex::Locker locker(m_lock);

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  assert(m_state == STATE_REACQUIRING);

  complete_active_action(STATE_LOCKED, 0);
}

template <typename I>
void ExclusiveLock<I>::send_release_lock() {
  assert(m_lock.is_locked());
  if (m_state == STATE_UNLOCKED) {
    complete_active_action(STATE_UNLOCKED, 0);
    return;
  }

  ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;
  m_state = STATE_RELEASING;

  using el = ExclusiveLock<I>;
  ReleaseRequest<I>* req = ReleaseRequest<I>::create(
      m_image_ctx, m_managed_lock,
      util::create_context_callback<el, &el::handle_release_lock>(this),
      false);
  m_image_ctx.op_work_queue->queue(new C_SendRequest<ReleaseRequest<I> >(req),
                                   0);
}

template <typename I>
void ExclusiveLock<I>::handle_release_lock(int r) {
  bool lock_request_needed = false;
  {
    Mutex::Locker locker(m_lock);
    ldout(m_image_ctx.cct, 10) << this << " " << __func__ << ": r=" << r
                               << dendl;

    assert(m_state == STATE_RELEASING);
    if (r >= 0) {
      m_lock.Unlock();
      lock_request_needed = m_image_ctx.aio_work_queue->is_lock_request_needed();
      m_lock.Lock();
    }
    complete_active_action(r < 0 ? STATE_LOCKED : STATE_UNLOCKED, r);
  }

  if (r >= 0 && lock_request_needed) {
    // if we have blocked IO -- re-request the lock
    RWLock::RLocker owner_locker(m_image_ctx.owner_lock);
    request_lock(nullptr);
  }
}

template <typename I>
void ExclusiveLock<I>::send_shutdown() {
  assert(m_lock.is_locked());
  if (m_state == STATE_UNLOCKED) {
    m_state = STATE_SHUTTING_DOWN;
    m_managed_lock->shut_down(util::create_context_callback<
            ExclusiveLock<I>, &ExclusiveLock<I>::handle_shutdown>(this));
    return;
  }

  ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;
  assert(m_state == STATE_LOCKED);
  m_state = STATE_SHUTTING_DOWN;

  m_lock.Unlock();
  m_image_ctx.op_work_queue->queue(new C_ShutDownRelease(this), 0);
  m_lock.Lock();
}

template <typename I>
void ExclusiveLock<I>::send_shutdown_release() {
  using el = ExclusiveLock<I>;
  ReleaseRequest<I>* req = ReleaseRequest<I>::create(
      m_image_ctx, m_managed_lock,
      util::create_context_callback<el, &el::handle_shutdown_released>(this),
      true);
  req->send();
}

template <typename I>
void ExclusiveLock<I>::handle_shutdown_released(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  {
    RWLock::WLocker owner_locker(m_image_ctx.owner_lock);
    m_image_ctx.aio_work_queue->clear_require_lock_on_read();
    m_image_ctx.exclusive_lock = nullptr;
  }

  if (r < 0) {
    lderr(cct) << "failed to shut down exclusive lock: " << cpp_strerror(r)
               << dendl;
  } else {
    m_image_ctx.aio_work_queue->unblock_writes();
  }

  m_managed_lock->shut_down(util::create_context_callback<
      ExclusiveLock<I>, &ExclusiveLock<I>::complete_shutdown>(this));
}

template <typename I>
void ExclusiveLock<I>::handle_shutdown(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  {
    RWLock::WLocker owner_locker(m_image_ctx.owner_lock);
    m_image_ctx.aio_work_queue->clear_require_lock_on_read();
    m_image_ctx.exclusive_lock = nullptr;
  }

  m_image_ctx.aio_work_queue->unblock_writes();
  m_image_ctx.image_watcher->flush(util::create_context_callback<
    ExclusiveLock<I>, &ExclusiveLock<I>::complete_shutdown>(this));
}

template <typename I>
void ExclusiveLock<I>::complete_shutdown(int r) {
  ActionContexts action_contexts;
  {
    Mutex::Locker locker(m_lock);
    assert(m_lock.is_locked());
    assert(m_actions_contexts.size() == 1);

    action_contexts = std::move(m_actions_contexts.front());
    m_actions_contexts.pop_front();
    m_state = STATE_SHUTDOWN;
  }

  // expect to be destroyed after firing callback
  for (auto ctx : action_contexts.second) {
    ctx->complete(r);
  }
}

} // namespace librbd

template class librbd::ExclusiveLock<librbd::ImageCtx>;
