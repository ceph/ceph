// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/Lock.h"
#include "librbd/managed_lock/Policy.h"
#include "librbd/managed_lock/AcquireRequest.h"
#include "librbd/managed_lock/ReleaseRequest.h"
#include "librbd/managed_lock/ReacquireRequest.h"
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

using namespace managed_lock;
using std::string;
using util::detail::C_AsyncCallback;

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

class ThreadPoolSingleton : public ThreadPool {
public:
  explicit ThreadPoolSingleton(CephContext *cct)
    : ThreadPool(cct, "librbd::lock::thread_pool", "tp_librbd_lock", 1) {
    start();
  }
  virtual ~ThreadPoolSingleton() {
    stop();
  }
};

} // anonymous namespace

template <typename L>
Lock<L>::Lock(librados::IoCtx &ioctx, const string& oid,
           Policy *policy)
  : m_ioctx(ioctx), m_cct(reinterpret_cast<CephContext *>(ioctx.cct())),
    m_oid(oid),
    m_policy(policy),
    m_lock(util::unique_lock_name("librbd::Lock<L>::m_lock", this)),
    m_state(STATE_UNLOCKED) {

  ThreadPoolSingleton *thread_pool_singleton;
  m_cct->lookup_or_create_singleton_object<ThreadPoolSingleton>(
                        thread_pool_singleton, "librbd::lock::thread_pool");

  m_work_queue = new ContextWQ("librbd::lock::op_work_queue",
                               m_cct->_conf->rbd_op_thread_timeout,
                               thread_pool_singleton);

  m_watcher = new L(this);
}

template <typename L>
Lock<L>::~Lock() {
  Mutex::Locker locker(m_lock);
  assert(m_state == STATE_SHUTDOWN || (
         m_state == STATE_UNLOCKED && !m_watcher->is_registered()));

  delete m_watcher;

  if (m_policy != nullptr) {
    delete m_policy;
  }
}

template <typename L>
bool Lock<L>::is_lock_owner() const {
  Mutex::Locker locker(m_lock);

  bool lock_owner;

  switch (m_state) {
  case STATE_LOCKED:
  case STATE_REACQUIRING:
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

template <typename L>
void Lock<L>::shut_down(Context *on_shut_down) {
  ldout(m_cct, 10) << this << " " << __func__ << dendl;

  {
    Mutex::Locker locker(m_lock);
    assert(!is_shutdown_locked());
    execute_action(ACTION_SHUT_DOWN, on_shut_down);
  }

  // if stalled in request state machine -- abort
  handle_peer_notification();
}

template <typename L>
void Lock<L>::try_lock(Context *on_tried_lock) {
  int r = 0;
  {
    Mutex::Locker locker(m_lock);
    if (is_shutdown_locked()) {
      r = -ESHUTDOWN;
    } else if (m_state != STATE_LOCKED || !m_actions_contexts.empty()) {
      ldout(m_cct, 10) << this << " " << __func__ << dendl;
      execute_action(ACTION_TRY_LOCK, on_tried_lock);
      return;
    }
  }

  on_tried_lock->complete(r);
}

template <typename L>
void Lock<L>::request_lock(Context *on_locked) {
  int r = 0;
  {
    Mutex::Locker locker(m_lock);
    if (is_shutdown_locked()) {
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

template <typename L>
void Lock<L>::release_lock(Context *on_released) {
  int r = 0;
  {
    Mutex::Locker locker(m_lock);
    if (is_shutdown_locked()) {
      r = -ESHUTDOWN;
    } else if (m_state != STATE_UNLOCKED || !m_actions_contexts.empty()) {
      ldout(m_cct, 10) << this << " " << __func__ << dendl;
      execute_action(ACTION_RELEASE_LOCK, on_released);
      return;
    }
  }

  on_released->complete(r);
}

template <typename L>
void Lock<L>::reacquire_lock(Context *on_reacquired) {
  {
    Mutex::Locker locker(m_lock);

    if (!is_shutdown_locked() &&
               (m_state == STATE_LOCKED ||
                m_state == STATE_ACQUIRING ||
                m_state == STATE_WAITING_FOR_REGISTER ||
                m_state == STATE_WAITING_FOR_PEER)) {
      // interlock the lock operation with other state ops
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

template <typename L>
void Lock<L>::handle_peer_notification() {
  Mutex::Locker locker(m_lock);
  if (m_state != STATE_WAITING_FOR_PEER) {
    return;
  }

  ldout(m_cct, 10) << this << " " << __func__ << dendl;
  assert(get_active_action() == ACTION_REQUEST_LOCK);
  execute_next_action();
}

template <typename L>
void Lock<L>::assert_locked(librados::ObjectWriteOperation *op,
                          ClsLockType type) {
  Mutex::Locker locker(m_lock);
  rados::cls::lock::assert_locked(op, m_oid, type, m_cookie,
                                  L::WATCHER_LOCK_TAG);
}

template <typename L>
bool Lock<L>::is_transition_state() const {
  switch (m_state) {
  case STATE_ACQUIRING:
  case STATE_WAITING_FOR_PEER:
  case STATE_WAITING_FOR_REGISTER:
  case STATE_REACQUIRING:
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

template <typename L>
void Lock<L>::append_context(Action action, Context *ctx) {
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

template <typename L>
void Lock<L>::execute_action(Action action, Context *ctx) {
  assert(m_lock.is_locked());

  append_context(action, ctx);
  if (!is_transition_state()) {
    execute_next_action();
  }
}

template <typename L>
void Lock<L>::execute_next_action() {
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

template <typename L>
typename Lock<L>::Action Lock<L>::get_active_action() const {
  assert(m_lock.is_locked());
  assert(!m_actions_contexts.empty());
  return m_actions_contexts.front().first;
}

template <typename L>
void Lock<L>::complete_active_action(State next_state, int r) {
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

template <typename L>
bool Lock<L>::is_shutdown_locked() const {
  assert(m_lock.is_locked());

  return ((m_state == STATE_SHUTDOWN) ||
          (!m_actions_contexts.empty() &&
           m_actions_contexts.back().first == ACTION_SHUT_DOWN));
}

template <typename L>
void Lock<L>::send_acquire_lock() {
  assert(m_lock.is_locked());
  if (m_state == STATE_LOCKED) {
    complete_active_action(STATE_LOCKED, 0);
    return;
  }

  ldout(m_cct, 10) << this << " " << __func__ << dendl;
  m_state = STATE_ACQUIRING;

  if (!m_watcher->is_registered()) {
    ldout(m_cct, 10) << this << " watcher not registered - delaying request"
                     << dendl;
    m_state = STATE_WAITING_FOR_REGISTER;
    m_watcher->register_watch(new FunctionContext([this](int r) {
          if (r < 0) {
            lderr(m_cct) << "watcher registering error: " << cpp_strerror(r)
                         << dendl;

            Mutex::Locker locker(m_lock);
            complete_active_action(STATE_UNLOCKED, r);
            return;
          }
          m_work_queue->queue(new FunctionContext([this](int r) {
            Mutex::Locker locker(m_lock);
            send_acquire_lock();
          }));
    }));
    return;
  }

  m_cookie = m_watcher->encode_lock_cookie();
  AcquireRequest<L>* req = AcquireRequest<L>::create(
    m_ioctx, m_watcher, m_oid, m_cookie,
    util::create_context_callback<Lock<L>, &Lock<L>::handle_acquire_lock>(this));
  m_work_queue->queue(new C_SendRequest<AcquireRequest<L>>(req), 0);
}

template <typename L>
void Lock<L>::handle_acquire_lock(int r) {
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
    assert(m_state == STATE_ACQUIRING);

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

template <typename L>
void Lock<L>::send_reacquire_lock() {
  assert(m_lock.is_locked());

  if (m_state != STATE_LOCKED) {
    complete_active_action(m_state, 0);
    return;
  }

  if (!m_watcher->is_registered()) {
     // watch (re)failed while recovering
     lderr(m_cct) << this << " " << __func__ << ": "
                  << "aborting reacquire due to invalid watch handle" << dendl;
     complete_active_action(STATE_LOCKED, 0);
     return;
  }

  m_new_cookie = m_watcher->encode_lock_cookie();
  if (m_cookie == m_new_cookie) {
    ldout(m_cct, 10) << this << " " << __func__ << ": "
                   << "skipping reacquire since cookie still valid" << dendl;
    complete_active_action(STATE_LOCKED, 0);
    return;
  }

  ldout(m_cct, 10) << this << " " << __func__ << dendl;
  m_state = STATE_REACQUIRING;

  ReacquireRequest<L>* req = ReacquireRequest<L>::create(
    m_ioctx, m_oid, m_cookie, m_new_cookie,
    util::create_context_callback<Lock, &Lock<L>::handle_reacquire_lock>(this));
  req->send();
}

template <typename L>
void Lock<L>::handle_reacquire_lock(int r) {
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

    if (!is_shutdown_locked()) {
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

template <typename L>
void Lock<L>::send_release_lock() {
  assert(m_lock.is_locked());
  if (m_state == STATE_UNLOCKED) {
    complete_active_action(STATE_UNLOCKED, 0);
    return;
  }

  ldout(m_cct, 10) << this << " " << __func__ << dendl;
  m_state = STATE_RELEASING;

  ReleaseRequest<L>* req = ReleaseRequest<L>::create(
    m_ioctx, m_watcher, m_oid, m_cookie,
    util::create_context_callback<Lock<L>, &Lock<L>::handle_release_lock>(this));
  m_work_queue->queue(new C_SendRequest<ReleaseRequest<L>>(req), 0);
}

template <typename L>
void Lock<L>::handle_release_lock(int r) {
  Mutex::Locker locker(m_lock);
  ldout(m_cct, 10) << this << " " << __func__ << ": r=" << r
                             << dendl;

  assert(m_state == STATE_RELEASING);
  if (r >= 0) {
    m_lock.Unlock();
    m_watcher->notify_released_lock();
    m_lock.Lock();

    m_cookie = "";
  }
  complete_active_action(r < 0 ? STATE_LOCKED : STATE_UNLOCKED, r);
}

template <typename L>
void Lock<L>::send_shutdown() {
  assert(m_lock.is_locked());
  if (m_state == STATE_UNLOCKED) {
    m_state = STATE_SHUTTING_DOWN;
    m_work_queue->queue(util::create_context_callback<
      Lock, &Lock<L>::handle_shutdown>(this), 0);
    return;
  }

  ldout(m_cct, 10) << this << " " << __func__ << dendl;
  assert(m_state == STATE_LOCKED);
  m_state = STATE_PRE_SHUTTING_DOWN;

  m_lock.Unlock();
  m_work_queue->queue(new C_ShutDownRelease(this), 0);
  m_lock.Lock();
}

template <typename L>
void Lock<L>::send_shutdown_release() {
  std::string cookie;
  {
    Mutex::Locker locker(m_lock);
    cookie = m_cookie;
  }

  ReleaseRequest<L>* req = ReleaseRequest<L>::create(
    m_ioctx, m_watcher, m_oid, cookie,
    util::create_context_callback<Lock, &Lock<L>::handle_shutdown_released>(this));
  req->send();
}

template <typename L>
void Lock<L>::handle_shutdown_released(int r) {
  ldout(m_cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to shut down exclusive lock: " << cpp_strerror(r)
               << dendl;
  }

  m_watcher->notify_released_lock();

  handle_shutdown(r);
}

template <typename L>
void Lock<L>::handle_shutdown(int r) {
  ldout(m_cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  if (m_watcher->is_registered()) {
    m_watcher->unregister_watch(
      util::create_context_callback<Lock, &Lock<L>::complete_shutdown>(this)
    );
  } else {
    complete_shutdown(r);
  }
}

template <typename L>
void Lock<L>::complete_shutdown(int r) {
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

#include "librbd/managed_lock/LockWatcher.h"
template class librbd::Lock<librbd::managed_lock::LockWatcher>;

