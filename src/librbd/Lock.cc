// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/Lock.h"
#include "librbd/lock/Policy.h"
#include "librbd/lock/LockWatcher.h"
#include "librbd/lock/AcquireRequest.h"
#include "librbd/lock/ReleaseRequest.h"
#include "librbd/lock/ReacquireRequest.h"
#include "cls/lock/cls_lock_client.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/Utils.h"
#include <sstream>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Lock: "

namespace librbd {

using namespace lock;
using std::string;

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

const std::string Lock::WATCHER_LOCK_TAG("internal");

Lock::Lock(librados::IoCtx &ioctx, ContextWQ *work_queue, const string& oid,
           Policy *policy)
  : m_ioctx(ioctx), m_cct(reinterpret_cast<CephContext *>(ioctx.cct())),
    m_work_queue(work_queue), m_oid(oid),
    m_watcher(new LockWatcher(this)),
    m_policy(policy),
    m_lock(util::unique_lock_name("librbd::Lock::m_lock", this)),
    m_state(STATE_UNLOCKED) {
}

Lock::~Lock() {
  assert(m_state == STATE_SHUTDOWN);
  // TODO: check when m_state == UNLOCKED

  if (m_policy != nullptr) {
    delete m_policy;
  }
}

bool Lock::is_lock_owner() const {
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

  ldout(m_cct, 20) << this << " " << __func__ << "=" << lock_owner
                   << dendl;
  return lock_owner;
}

void Lock::shut_down(Context *on_shut_down) {
  ldout(m_cct, 10) << this << " " << __func__ << dendl;

  {
    Mutex::Locker locker(m_lock);
    assert(!is_shutdown());
    execute_action(ACTION_SHUT_DOWN, on_shut_down);
  }

  // if stalled in request state machine -- abort
  handle_peer_notification();
}

void Lock::try_lock(Context *on_tried_lock) {
  int r = 0;
  {
    Mutex::Locker locker(m_lock);
    if (is_shutdown()) {
      r = -ESHUTDOWN;
    } else if (m_state != STATE_LOCKED || !m_actions_contexts.empty()) {
      ldout(m_cct, 10) << this << " " << __func__ << dendl;
      execute_action(ACTION_TRY_LOCK, on_tried_lock);
      return;
    }
  }

  on_tried_lock->complete(r);
}

void Lock::request_lock(Context *on_locked) {
  int r = 0;
  {
    Mutex::Locker locker(m_lock);
    if (is_shutdown()) {
      r = -ESHUTDOWN;
    } else if (m_state != STATE_LOCKED || !m_actions_contexts.empty()) {
      ldout(m_cct, 10) << this << " " << __func__ << dendl;
      execute_action(ACTION_REQUEST_LOCK, on_locked);
      return;
    }
  }

  if (on_locked != nullptr) {
    on_locked->complete(r);
  }
}

void Lock::release_lock(Context *on_released) {
  int r = 0;
  {
    Mutex::Locker locker(m_lock);
    if (is_shutdown()) {
      r = -ESHUTDOWN;
    } else if (m_state != STATE_UNLOCKED || !m_actions_contexts.empty()) {
      ldout(m_cct, 10) << this << " " << __func__ << dendl;
      execute_action(ACTION_RELEASE_LOCK, on_released);
      return;
    }
  }

  on_released->complete(r);
}

void Lock::reacquire_lock(Context *on_reacquired) {
  {
    Mutex::Locker locker(m_lock);

    if (!is_shutdown() &&
               (m_state == STATE_LOCKED ||
                m_state == STATE_ACQUIRING ||
                m_state == STATE_POST_ACQUIRING ||
                m_state == STATE_WAITING_FOR_PEER)) {
      // interlock the lock operation with other image state ops
      ldout(m_cct, 10) << this << " " << __func__ << dendl;
      execute_action(ACTION_REACQUIRE_LOCK, on_reacquired);
      return;
    }
  }

  // ignore request if shutdown or not in a locked-related state
  if (on_reacquired != nullptr) {
    on_reacquired->complete(0);
  }
}

void Lock::handle_peer_notification() {
  Mutex::Locker locker(m_lock);
  if (m_state != STATE_WAITING_FOR_PEER) {
    return;
  }

  ldout(m_cct, 10) << this << " " << __func__ << dendl;
  assert(get_active_action() == ACTION_REQUEST_LOCK);
  execute_next_action();
}

void Lock::assert_locked(librados::ObjectWriteOperation *op,
                          ClsLockType type) {
  Mutex::Locker locker(m_lock);
  rados::cls::lock::assert_locked(op, m_oid, type, m_cookie, WATCHER_LOCK_TAG);
}

void Lock::update_cookie(const string& cookie) {
  Mutex::Locker l(m_lock);

  std::ostringstream ss;
  ss << WATCHER_LOCK_COOKIE_PREFIX << " " << cookie;
  m_new_cookie = ss.str();
}

bool Lock::decode_lock_cookie(const std::string &tag, uint64_t *handle) {
  std::string prefix;
  std::istringstream ss(tag);
  if (!(ss >> prefix >> *handle) || prefix != WATCHER_LOCK_COOKIE_PREFIX) {
    return false;
  }
  return true;
}

bool Lock::is_transition_state() const {
  switch (m_state) {
  case STATE_ACQUIRING:
  case STATE_WAITING_FOR_PEER:
  case STATE_POST_ACQUIRING:
  case STATE_REACQUIRING:
  case STATE_PRE_RELEASING:
  case STATE_RELEASING:
  case STATE_PRE_SHUTTING_DOWN:
  case STATE_SHUTTING_DOWN:
    return true;
  case STATE_UNLOCKED:
  case STATE_LOCKED:
  case STATE_SHUTDOWN:
    break;
  }
  return false;
}

void Lock::append_context(Action action, Context *ctx) {
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

void Lock::execute_action(Action action, Context *ctx) {
  assert(m_lock.is_locked());

  append_context(action, ctx);
  if (!is_transition_state()) {
    execute_next_action();
  }
}

void Lock::execute_next_action() {
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

typename Lock::Action Lock::get_active_action() const {
  assert(m_lock.is_locked());
  assert(!m_actions_contexts.empty());
  return m_actions_contexts.front().first;
}

void Lock::complete_active_action(State next_state, int r) {
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

bool Lock::is_shutdown() const {
  assert(m_lock.is_locked());

  return ((m_state == STATE_SHUTDOWN) ||
          (!m_actions_contexts.empty() &&
           m_actions_contexts.back().first == ACTION_SHUT_DOWN));
}

void Lock::send_acquire_lock() {
  assert(m_lock.is_locked());
  if (m_state == STATE_LOCKED) {
    complete_active_action(STATE_LOCKED, 0);
    return;
  }

  ldout(m_cct, 10) << this << " " << __func__ << dendl;
  m_state = STATE_ACQUIRING;

  m_cookie = m_new_cookie;
  AcquireRequest* req = AcquireRequest::create(
    m_ioctx, m_work_queue, m_watcher, m_oid, m_cookie,
    util::create_context_callback<Lock, &Lock::handle_acquiring_lock>(this),
    util::create_context_callback<Lock, &Lock::handle_acquire_lock>(this));
  m_work_queue->queue(new C_SendRequest<AcquireRequest>(req), 0);
}

void Lock::handle_acquiring_lock(int r) {
  Mutex::Locker locker(m_lock);
  ldout(m_cct, 10) << this << " " << __func__ << dendl;

  assert(r == 0);
  assert(m_state == STATE_ACQUIRING);

  // lock is owned at this point
  m_state = STATE_POST_ACQUIRING;
}

void Lock::handle_acquire_lock(int r) {
  ldout(m_cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  if (r == -EBUSY || r == -EAGAIN) {
    ldout(m_cct, 5) << "unable to acquire exclusive lock" << dendl;
  } else if (r < 0) {
    lderr(m_cct) << "failed to acquire exclusive lock:" << cpp_strerror(r)
               << dendl;
  } else {
    ldout(m_cct, 5) << "successfully acquired exclusive lock" << dendl;
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
      m_watcher->notify_request_lock();
      return;
    }
    m_lock.Unlock();
  }

  State next_state = (r < 0 ? STATE_UNLOCKED : STATE_LOCKED);
  if (r == -EAGAIN) {
    r = 0;
  }

  if (next_state == STATE_LOCKED) {
    m_watcher->notify_acquired_lock();
  }

  Mutex::Locker locker(m_lock);
  complete_active_action(next_state, r);
}

void Lock::send_reacquire_lock() {
  assert(m_lock.is_locked());

  if (m_state != STATE_LOCKED) {
    complete_active_action(m_state, 0);
    return;
  }

  if (m_cookie == m_new_cookie) {
    ldout(m_cct, 10) << this << " " << __func__ << ": "
                   << "skipping reacquire since cookie still valid" << dendl;
    complete_active_action(STATE_LOCKED, 0);
    return;
  }

  ldout(m_cct, 10) << this << " " << __func__ << dendl;
  m_state = STATE_REACQUIRING;

  ReacquireRequest* req = ReacquireRequest::create(
    m_ioctx, m_oid, m_cookie, m_new_cookie,
    util::create_context_callback<Lock, &Lock::handle_reacquire_lock>(this));
  req->send();
}

void Lock::handle_reacquire_lock(int r) {
  Mutex::Locker locker(m_lock);

  ldout(m_cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  assert(m_state == STATE_REACQUIRING);
  if (r < 0) {
    if (r == -EOPNOTSUPP) {
      ldout(m_cct, 10) << this << " " << __func__ << ": "
                     << "updating lock is not supported" << dendl;
    } else {
      lderr(m_cct) << this << " " << __func__ << ": "
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

void Lock::send_release_lock() {
  assert(m_lock.is_locked());
  if (m_state == STATE_UNLOCKED) {
    complete_active_action(STATE_UNLOCKED, 0);
    return;
  }

  ldout(m_cct, 10) << this << " " << __func__ << dendl;
  m_state = STATE_PRE_RELEASING;

  ReleaseRequest* req = ReleaseRequest::create(
    m_ioctx, m_work_queue, m_watcher, m_oid, m_cookie,
    util::create_context_callback<Lock, &Lock::handle_releasing_lock>(this),
    util::create_context_callback<Lock, &Lock::handle_release_lock>(this),
    false);
  m_work_queue->queue(new C_SendRequest<ReleaseRequest>(req), 0);
}

void Lock::handle_releasing_lock(int r) {
  Mutex::Locker locker(m_lock);
  ldout(m_cct, 10) << this << " " << __func__ << dendl;

  assert(r == 0);
  assert(m_state == STATE_PRE_RELEASING);

  // all IO and ops should be blocked/canceled by this point
  m_state = STATE_RELEASING;
}

void Lock::handle_release_lock(int r) {
  Mutex::Locker locker(m_lock);
  ldout(m_cct, 10) << this << " " << __func__ << ": r=" << r
                             << dendl;

  assert(m_state == STATE_PRE_RELEASING ||
         m_state == STATE_RELEASING);
  if (r >= 0) {
    m_lock.Unlock();
    m_watcher->notify_released_lock();
    m_lock.Lock();

    m_cookie = "";
  }
  complete_active_action(r < 0 ? STATE_LOCKED : STATE_UNLOCKED, r);
}

void Lock::send_shutdown() {
  assert(m_lock.is_locked());
  if (m_state == STATE_UNLOCKED) {
    m_state = STATE_SHUTTING_DOWN;
    m_work_queue->queue(util::create_context_callback<
      Lock, &Lock::handle_shutdown>(this), 0);
    return;
  }

  ldout(m_cct, 10) << this << " " << __func__ << dendl;
  assert(m_state == STATE_LOCKED);
  m_state = STATE_PRE_SHUTTING_DOWN;

  m_lock.Unlock();
  m_work_queue->queue(new C_ShutDownRelease(this), 0);
  m_lock.Lock();
}

void Lock::send_shutdown_release() {
  std::string cookie;
  {
    Mutex::Locker locker(m_lock);
    cookie = m_cookie;
  }

  ReleaseRequest* req = ReleaseRequest::create(
    m_ioctx, m_work_queue, m_watcher, m_oid, cookie,
    util::create_context_callback<Lock, &Lock::handle_shutdown_releasing>(this),
    util::create_context_callback<Lock, &Lock::handle_shutdown_released>(this),
    true);
  req->send();
}

void Lock::handle_shutdown_releasing(int r) {
  ldout(m_cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  assert(r == 0);
  assert(m_state == STATE_PRE_SHUTTING_DOWN);

  // all IO and ops should be blocked/canceled by this point
  m_state = STATE_SHUTTING_DOWN;
}

void Lock::handle_shutdown_released(int r) {
  ldout(m_cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to shut down exclusive lock: " << cpp_strerror(r)
               << dendl;
  }

  m_watcher->notify_released_lock();
  complete_shutdown(r);
}

void Lock::handle_shutdown(int r) {
  ldout(m_cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  m_watcher->flush(util::create_context_callback<
    Lock, &Lock::complete_shutdown>(this));
}

void Lock::complete_shutdown(int r) {
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

