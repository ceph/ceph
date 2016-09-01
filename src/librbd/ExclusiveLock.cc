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
#include "librbd/Utils.h"
#include "librbd/exclusive_lock/AcquireRequest.h"
#include "librbd/exclusive_lock/ReacquireRequest.h"
#include "librbd/exclusive_lock/ReleaseRequest.h"
#include <sstream>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ExclusiveLock: "

namespace librbd {

using namespace exclusive_lock;

namespace {

const std::string WATCHER_LOCK_COOKIE_PREFIX = "auto";

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
const std::string ExclusiveLock<I>::WATCHER_LOCK_TAG("internal");

template <typename I>
ExclusiveLock<I>::ExclusiveLock(I &image_ctx)
  : m_image_ctx(image_ctx),
    m_lock(util::unique_lock_name("librbd::ExclusiveLock::m_lock", this)),
    m_state(STATE_UNINITIALIZED), m_watch_handle(0) {
}

template <typename I>
ExclusiveLock<I>::~ExclusiveLock() {
  assert(m_state == STATE_UNINITIALIZED || m_state == STATE_SHUTDOWN);
}

template <typename I>
bool ExclusiveLock<I>::is_lock_owner() const {
  Mutex::Locker locker(m_lock);

  bool lock_owner;
  switch (m_state) {
  case STATE_LOCKED:
  case STATE_POST_ACQUIRING:
  case STATE_REACQUIRING:
  case STATE_PRE_RELEASING:
  case STATE_PRE_SHUTTING_DOWN:
    lock_owner = true;
    break;
  default:
    lock_owner = false;
    break;
  }

  ldout(m_image_ctx.cct, 20) << this << " " << __func__ << "=" << lock_owner
                             << dendl;
  return lock_owner;
}

template <typename I>
bool ExclusiveLock<I>::accept_requests(int *ret_val) const {
  Mutex::Locker locker(m_lock);

  bool accept_requests = (!is_shutdown() && m_state == STATE_LOCKED &&
                          !m_request_blocked);
  *ret_val = m_request_blocked_ret_val;

  ldout(m_image_ctx.cct, 20) << this << " " << __func__ << "="
                             << accept_requests << dendl;
  return accept_requests;
}

template <typename I>
void ExclusiveLock<I>::block_requests(int r) {
  Mutex::Locker locker(m_lock);
  assert(!m_request_blocked);
  m_request_blocked = true;
  m_request_blocked_ret_val = r;

  ldout(m_image_ctx.cct, 20) << this << " " << __func__ << dendl;
}

template <typename I>
void ExclusiveLock<I>::unblock_requests() {
  Mutex::Locker locker(m_lock);
  assert(m_request_blocked);
  m_request_blocked = false;
  m_request_blocked_ret_val = 0;

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

    if (m_state == STATE_WAITING_FOR_REGISTER) {
      // restart the acquire lock process now that watch is valid
      ldout(m_image_ctx.cct, 10) << this << " " << __func__ << ": "
                                 << "woke up waiting acquire" << dendl;
      Action active_action = get_active_action();
      assert(active_action == ACTION_TRY_LOCK ||
             active_action == ACTION_REQUEST_LOCK);
      execute_next_action();
    } else if (!is_shutdown() &&
               (m_state == STATE_LOCKED ||
                m_state == STATE_ACQUIRING ||
                m_state == STATE_POST_ACQUIRING ||
                m_state == STATE_WAITING_FOR_PEER)) {
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
  Mutex::Locker locker(m_lock);
  if (m_state != STATE_WAITING_FOR_PEER) {
    return;
  }

  ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;
  assert(get_active_action() == ACTION_REQUEST_LOCK);
  execute_next_action();
}

template <typename I>
void ExclusiveLock<I>::assert_header_locked(librados::ObjectWriteOperation *op) {
  Mutex::Locker locker(m_lock);
  rados::cls::lock::assert_locked(op, RBD_LOCK_NAME, LOCK_EXCLUSIVE,
                                  m_cookie, WATCHER_LOCK_TAG);
}

template <typename I>
std::string ExclusiveLock<I>::encode_lock_cookie() const {
  assert(m_lock.is_locked());

  assert(m_watch_handle != 0);
  std::ostringstream ss;
  ss << WATCHER_LOCK_COOKIE_PREFIX << " " << m_watch_handle;
  return ss.str();
}

template <typename I>
bool ExclusiveLock<I>::decode_lock_cookie(const std::string &tag,
                                          uint64_t *handle) {
  std::string prefix;
  std::istringstream ss(tag);
  if (!(ss >> prefix >> *handle) || prefix != WATCHER_LOCK_COOKIE_PREFIX) {
    return false;
  }
  return true;
}

template <typename I>
bool ExclusiveLock<I>::is_transition_state() const {
  switch (m_state) {
  case STATE_INITIALIZING:
  case STATE_ACQUIRING:
  case STATE_WAITING_FOR_PEER:
  case STATE_WAITING_FOR_REGISTER:
  case STATE_POST_ACQUIRING:
  case STATE_REACQUIRING:
  case STATE_PRE_RELEASING:
  case STATE_RELEASING:
  case STATE_PRE_SHUTTING_DOWN:
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

  m_watch_handle = m_image_ctx.image_watcher->get_watch_handle();
  if (m_watch_handle == 0) {
    lderr(cct) << "image watcher not registered - delaying request" << dendl;
    m_state = STATE_WAITING_FOR_REGISTER;
    return;
  }

  m_cookie = encode_lock_cookie();

  using el = ExclusiveLock<I>;
  AcquireRequest<I>* req = AcquireRequest<I>::create(
    m_image_ctx, m_cookie,
    util::create_context_callback<el, &el::handle_acquiring_lock>(this),
    util::create_context_callback<el, &el::handle_acquire_lock>(this));
  m_image_ctx.op_work_queue->queue(new C_SendRequest<AcquireRequest<I> >(req),
                                   0);
}

template <typename I>
void ExclusiveLock<I>::handle_acquiring_lock(int r) {
  Mutex::Locker locker(m_lock);
  ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;

  assert(r == 0);
  assert(m_state == STATE_ACQUIRING);

  // lock is owned at this point
  m_state = STATE_POST_ACQUIRING;
}

template <typename I>
void ExclusiveLock<I>::handle_acquire_lock(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  if (r == -EBUSY || r == -EAGAIN) {
    ldout(cct, 5) << "unable to acquire exclusive lock" << dendl;
  } else if (r < 0) {
    lderr(cct) << "failed to acquire exclusive lock:" << cpp_strerror(r)
               << dendl;
  } else {
    ldout(cct, 5) << "successfully acquired exclusive lock" << dendl;
  }

  {
    m_lock.Lock();
    assert(m_state == STATE_ACQUIRING ||
           m_state == STATE_POST_ACQUIRING);

    Action action = get_active_action();
    assert(action == ACTION_TRY_LOCK || action == ACTION_REQUEST_LOCK);
    if (action == ACTION_REQUEST_LOCK && r < 0 && r != -EBLACKLISTED &&
        r != -EPERM) {
      m_state = STATE_WAITING_FOR_PEER;
      m_lock.Unlock();

      // request the lock from a peer
      m_image_ctx.image_watcher->notify_request_lock();
      return;
    }
    m_lock.Unlock();
  }

  State next_state = (r < 0 ? STATE_UNLOCKED : STATE_LOCKED);
  if (r == -EAGAIN) {
    r = 0;
  }

  if (next_state == STATE_LOCKED) {
    m_image_ctx.image_watcher->notify_acquired_lock();
    m_image_ctx.aio_work_queue->clear_require_lock_on_read();
    m_image_ctx.aio_work_queue->unblock_writes();
  }

  Mutex::Locker locker(m_lock);
  complete_active_action(next_state, r);
}

template <typename I>
void ExclusiveLock<I>::send_reacquire_lock() {
  assert(m_lock.is_locked());

  CephContext *cct = m_image_ctx.cct;
  if (m_state != STATE_LOCKED) {
    complete_active_action(m_state, 0);
    return;
  }

  m_watch_handle = m_image_ctx.image_watcher->get_watch_handle();
  if (m_watch_handle == 0) {
    // watch (re)failed while recovering
    lderr(cct) << this << " " << __func__ << ": "
               << "aborting reacquire due to invalid watch handle" << dendl;
    complete_active_action(STATE_LOCKED, 0);
    return;
  }

  m_new_cookie = encode_lock_cookie();
  if (m_cookie == m_new_cookie) {
    ldout(cct, 10) << this << " " << __func__ << ": "
                   << "skipping reacquire since cookie still valid" << dendl;
    complete_active_action(STATE_LOCKED, 0);
    return;
  }

  ldout(cct, 10) << this << " " << __func__ << dendl;
  m_state = STATE_REACQUIRING;

  using el = ExclusiveLock<I>;
  ReacquireRequest<I>* req = ReacquireRequest<I>::create(
    m_image_ctx, m_cookie, m_new_cookie,
    util::create_context_callback<el, &el::handle_reacquire_lock>(this));
  req->send();
}

template <typename I>
void ExclusiveLock<I>::handle_reacquire_lock(int r) {
  Mutex::Locker locker(m_lock);

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  assert(m_state == STATE_REACQUIRING);
  if (r < 0) {
    if (r == -EOPNOTSUPP) {
      ldout(cct, 10) << this << " " << __func__ << ": "
                     << "updating lock is not supported" << dendl;
    } else {
      lderr(cct) << this << " " << __func__ << ": "
                 << "failed to update lock cookie: " << cpp_strerror(r)
                 << dendl;
    }

    if (!is_shutdown()) {
      // queue a release and re-acquire of the lock since cookie cannot
      // be updated on older OSDs
      execute_action(ACTION_RELEASE_LOCK, nullptr);

      assert(!m_actions_contexts.empty());
      ActionContexts &action_contexts(m_actions_contexts.front());

      // reacquire completes when the request lock completes
      Contexts contexts;
      std::swap(contexts, action_contexts.second);
      if (contexts.empty()) {
        execute_action(ACTION_REQUEST_LOCK, nullptr);
      } else {
        for (auto ctx : contexts) {
          ctx = new FunctionContext([ctx, r](int acquire_ret_val) {
              if (acquire_ret_val >= 0) {
                acquire_ret_val = r;
              }
              ctx->complete(acquire_ret_val);
            });
          execute_action(ACTION_REQUEST_LOCK, ctx);
        }
      }
    }
  } else {
    m_cookie = m_new_cookie;
  }

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
  m_state = STATE_PRE_RELEASING;

  using el = ExclusiveLock<I>;
  ReleaseRequest<I>* req = ReleaseRequest<I>::create(
    m_image_ctx, m_cookie,
    util::create_context_callback<el, &el::handle_releasing_lock>(this),
    util::create_context_callback<el, &el::handle_release_lock>(this),
    false);
  m_image_ctx.op_work_queue->queue(new C_SendRequest<ReleaseRequest<I> >(req),
                                   0);
}

template <typename I>
void ExclusiveLock<I>::handle_releasing_lock(int r) {
  Mutex::Locker locker(m_lock);
  ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;

  assert(r == 0);
  assert(m_state == STATE_PRE_RELEASING);

  // all IO and ops should be blocked/canceled by this point
  m_state = STATE_RELEASING;
}

template <typename I>
void ExclusiveLock<I>::handle_release_lock(int r) {
  bool lock_request_needed = false;
  {
    Mutex::Locker locker(m_lock);
    ldout(m_image_ctx.cct, 10) << this << " " << __func__ << ": r=" << r
                               << dendl;

    assert(m_state == STATE_PRE_RELEASING ||
           m_state == STATE_RELEASING);
    if (r >= 0) {
      m_lock.Unlock();
      m_image_ctx.image_watcher->notify_released_lock();
      lock_request_needed = m_image_ctx.aio_work_queue->is_lock_request_needed();
      m_lock.Lock();

      m_cookie = "";
      m_watch_handle = 0;
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
    m_image_ctx.op_work_queue->queue(util::create_context_callback<
      ExclusiveLock<I>, &ExclusiveLock<I>::handle_shutdown>(this), 0);
    return;
  }

  ldout(m_image_ctx.cct, 10) << this << " " << __func__ << dendl;
  assert(m_state == STATE_LOCKED);
  m_state = STATE_PRE_SHUTTING_DOWN;

  m_lock.Unlock();
  m_image_ctx.op_work_queue->queue(new C_ShutDownRelease(this), 0);
  m_lock.Lock();
}

template <typename I>
void ExclusiveLock<I>::send_shutdown_release() {
  std::string cookie;
  {
    Mutex::Locker locker(m_lock);
    cookie = m_cookie;
  }

  using el = ExclusiveLock<I>;
  ReleaseRequest<I>* req = ReleaseRequest<I>::create(
    m_image_ctx, cookie,
    util::create_context_callback<el, &el::handle_shutdown_releasing>(this),
    util::create_context_callback<el, &el::handle_shutdown_released>(this),
    true);
  req->send();
}

template <typename I>
void ExclusiveLock<I>::handle_shutdown_releasing(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  assert(r == 0);
  assert(m_state == STATE_PRE_SHUTTING_DOWN);

  // all IO and ops should be blocked/canceled by this point
  m_state = STATE_SHUTTING_DOWN;
}

template <typename I>
void ExclusiveLock<I>::handle_shutdown_released(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  {
    RWLock::WLocker owner_locker(m_image_ctx.owner_lock);
    m_image_ctx.exclusive_lock = nullptr;
  }

  if (r < 0) {
    lderr(cct) << "failed to shut down exclusive lock: " << cpp_strerror(r)
               << dendl;
  } else {
    m_image_ctx.aio_work_queue->clear_require_lock_on_read();
    m_image_ctx.aio_work_queue->unblock_writes();
  }

  m_image_ctx.image_watcher->notify_released_lock();
  complete_shutdown(r);
}

template <typename I>
void ExclusiveLock<I>::handle_shutdown(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  {
    RWLock::WLocker owner_locker(m_image_ctx.owner_lock);
    m_image_ctx.exclusive_lock = nullptr;
  }

  m_image_ctx.aio_work_queue->clear_require_lock_on_read();
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
