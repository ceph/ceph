// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/ManagedLock.h"
#include "librbd/AsioEngine.h"
#include "librbd/ImageCtx.h"
#include "librbd/Watcher.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/managed_lock/AcquireRequest.h"
#include "librbd/managed_lock/BreakRequest.h"
#include "librbd/managed_lock/GetLockerRequest.h"
#include "librbd/managed_lock/ReleaseRequest.h"
#include "librbd/managed_lock/ReacquireRequest.h"
#include "librbd/managed_lock/Types.h"
#include "librbd/managed_lock/Utils.h"
#include "cls/lock/cls_lock_client.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Cond.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ManagedLock: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {

using std::string;
using namespace managed_lock;

namespace {

template <typename R>
struct C_SendLockRequest : public Context {
  R* request;
  explicit C_SendLockRequest(R* request) : request(request) {
  }
  void finish(int r) override {
    request->send();
  }
};

struct C_Tracked : public Context {
  AsyncOpTracker &tracker;
  Context *ctx;
  C_Tracked(AsyncOpTracker &tracker, Context *ctx)
    : tracker(tracker), ctx(ctx) {
    tracker.start_op();
  }
  ~C_Tracked() override {
    tracker.finish_op();
  }
  void finish(int r) override {
    ctx->complete(r);
  }
};

} // anonymous namespace

using librbd::util::create_context_callback;
using librbd::util::unique_lock_name;
using managed_lock::util::decode_lock_cookie;
using managed_lock::util::encode_lock_cookie;

template <typename I>
ManagedLock<I>::ManagedLock(librados::IoCtx &ioctx, AsioEngine& asio_engine,
                            const string& oid, Watcher *watcher, Mode mode,
                            bool blacklist_on_break_lock,
                            uint32_t blacklist_expire_seconds)
  : m_lock(ceph::make_mutex(unique_lock_name("librbd::ManagedLock<I>::m_lock", this))),
    m_ioctx(ioctx), m_cct(reinterpret_cast<CephContext *>(ioctx.cct())),
    m_asio_engine(asio_engine),
    m_work_queue(asio_engine.get_work_queue()),
    m_oid(oid),
    m_watcher(watcher),
    m_mode(mode),
    m_blacklist_on_break_lock(blacklist_on_break_lock),
    m_blacklist_expire_seconds(blacklist_expire_seconds),
    m_state(STATE_UNLOCKED) {
}

template <typename I>
ManagedLock<I>::~ManagedLock() {
  std::lock_guard locker{m_lock};
  ceph_assert(m_state == STATE_SHUTDOWN || m_state == STATE_UNLOCKED ||
              m_state == STATE_UNINITIALIZED);
  if (m_state == STATE_UNINITIALIZED) {
    // never initialized -- ensure any in-flight ops are complete
    // since we wouldn't expect shut_down to be invoked
    C_SaferCond ctx;
    m_async_op_tracker.wait_for_ops(&ctx);
    ctx.wait();
  }
  ceph_assert(m_async_op_tracker.empty());
}

template <typename I>
bool ManagedLock<I>::is_lock_owner() const {
  std::lock_guard locker{m_lock};

  return is_lock_owner(m_lock);
}

template <typename I>
bool ManagedLock<I>::is_lock_owner(ceph::mutex &lock) const {

  ceph_assert(ceph_mutex_is_locked(m_lock));

  bool lock_owner;

  switch (m_state) {
  case STATE_LOCKED:
  case STATE_REACQUIRING:
  case STATE_PRE_SHUTTING_DOWN:
  case STATE_POST_ACQUIRING:
  case STATE_PRE_RELEASING:
    lock_owner = true;
    break;
  default:
    lock_owner = false;
    break;
  }

  ldout(m_cct, 20) << "=" << lock_owner << dendl;
  return lock_owner;
}

template <typename I>
void ManagedLock<I>::shut_down(Context *on_shut_down) {
  ldout(m_cct, 10) << dendl;

  std::lock_guard locker{m_lock};
  ceph_assert(!is_state_shutdown());

  if (m_state == STATE_WAITING_FOR_REGISTER) {
    // abort stalled acquire lock state
    ldout(m_cct, 10) << "woke up waiting (re)acquire" << dendl;
    Action active_action = get_active_action();
    ceph_assert(active_action == ACTION_TRY_LOCK ||
                active_action == ACTION_ACQUIRE_LOCK);
    complete_active_action(STATE_UNLOCKED, -ESHUTDOWN);
  }

  execute_action(ACTION_SHUT_DOWN, on_shut_down);
}

template <typename I>
void ManagedLock<I>::acquire_lock(Context *on_acquired) {
  int r = 0;
  {
    std::lock_guard locker{m_lock};
    if (is_state_shutdown()) {
      r = -ESHUTDOWN;
    } else if (m_state != STATE_LOCKED || !m_actions_contexts.empty()) {
      ldout(m_cct, 10) << dendl;
      execute_action(ACTION_ACQUIRE_LOCK, on_acquired);
      return;
    }
  }

  if (on_acquired != nullptr) {
    on_acquired->complete(r);
  }
}

template <typename I>
void ManagedLock<I>::try_acquire_lock(Context *on_acquired) {
  int r = 0;
  {
    std::lock_guard locker{m_lock};
    if (is_state_shutdown()) {
      r = -ESHUTDOWN;
    } else if (m_state != STATE_LOCKED || !m_actions_contexts.empty()) {
      ldout(m_cct, 10) << dendl;
      execute_action(ACTION_TRY_LOCK, on_acquired);
      return;
    }
  }

  if (on_acquired != nullptr) {
    on_acquired->complete(r);
  }
}

template <typename I>
void ManagedLock<I>::release_lock(Context *on_released) {
  int r = 0;
  {
    std::lock_guard locker{m_lock};
    if (is_state_shutdown()) {
      r = -ESHUTDOWN;
    } else if (m_state != STATE_UNLOCKED || !m_actions_contexts.empty()) {
      ldout(m_cct, 10) << dendl;
      execute_action(ACTION_RELEASE_LOCK, on_released);
      return;
    }
  }

  if (on_released != nullptr) {
    on_released->complete(r);
  }
}

template <typename I>
void ManagedLock<I>::reacquire_lock(Context *on_reacquired) {
  {
    std::lock_guard locker{m_lock};

    if (m_state == STATE_WAITING_FOR_REGISTER) {
      // restart the acquire lock process now that watch is valid
      ldout(m_cct, 10) << "woke up waiting (re)acquire" << dendl;
      Action active_action = get_active_action();
      ceph_assert(active_action == ACTION_TRY_LOCK ||
                  active_action == ACTION_ACQUIRE_LOCK);
      execute_next_action();
    } else if (!is_state_shutdown() &&
               (m_state == STATE_LOCKED ||
                m_state == STATE_ACQUIRING ||
                m_state == STATE_POST_ACQUIRING ||
                m_state == STATE_WAITING_FOR_LOCK)) {
      // interlock the lock operation with other state ops
      ldout(m_cct, 10) << dendl;
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
void ManagedLock<I>::get_locker(managed_lock::Locker *locker,
                                Context *on_finish) {
  ldout(m_cct, 10) << dendl;

  int r;
  {
    std::lock_guard l{m_lock};
    if (is_state_shutdown()) {
      r = -ESHUTDOWN;
    } else {
      on_finish = new C_Tracked(m_async_op_tracker, on_finish);
      auto req = managed_lock::GetLockerRequest<I>::create(
        m_ioctx, m_oid, m_mode == EXCLUSIVE, locker, on_finish);
      req->send();
      return;
    }
  }

  on_finish->complete(r);
}

template <typename I>
void ManagedLock<I>::break_lock(const managed_lock::Locker &locker,
                                bool force_break_lock, Context *on_finish) {
  ldout(m_cct, 10) << dendl;

  int r;
  {
    std::lock_guard l{m_lock};
    if (is_state_shutdown()) {
      r = -ESHUTDOWN;
    } else if (is_lock_owner(m_lock)) {
      r = -EBUSY;
    } else {
      on_finish = new C_Tracked(m_async_op_tracker, on_finish);
      auto req = managed_lock::BreakRequest<I>::create(
        m_ioctx, m_asio_engine, m_oid, locker, m_mode == EXCLUSIVE,
        m_blacklist_on_break_lock, m_blacklist_expire_seconds, force_break_lock,
        on_finish);
      req->send();
      return;
    }
  }

  on_finish->complete(r);
}

template <typename I>
int ManagedLock<I>::assert_header_locked() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectReadOperation op;
  {
    std::lock_guard locker{m_lock};
    rados::cls::lock::assert_locked(&op, RBD_LOCK_NAME,
                                    (m_mode == EXCLUSIVE ? ClsLockType::EXCLUSIVE :
                                                           ClsLockType::SHARED),
                                    m_cookie,
                                    managed_lock::util::get_watcher_lock_tag());
  }

  int r = m_ioctx.operate(m_oid, &op, nullptr);
  if (r < 0) {
    if (r == -EBLACKLISTED) {
      ldout(m_cct, 5) << "client is not lock owner -- client blacklisted"
                      << dendl;
    } else if (r == -ENOENT) {
      ldout(m_cct, 5) << "client is not lock owner -- no lock detected"
                      << dendl;
    } else if (r == -EBUSY) {
      ldout(m_cct, 5) << "client is not lock owner -- owned by different client"
                      << dendl;
    } else {
      lderr(m_cct) << "failed to verify lock ownership: " << cpp_strerror(r)
                   << dendl;
    }

    return r;
  }

  return 0;
}

template <typename I>
void ManagedLock<I>::shutdown_handler(int r, Context *on_finish) {
  on_finish->complete(r);
}

template <typename I>
void ManagedLock<I>::pre_acquire_lock_handler(Context *on_finish) {
  on_finish->complete(0);
}

template <typename I>
void  ManagedLock<I>::post_acquire_lock_handler(int r, Context *on_finish) {
  on_finish->complete(r);
}

template <typename I>
void  ManagedLock<I>::pre_release_lock_handler(bool shutting_down,
                                               Context *on_finish) {
  on_finish->complete(0);
}

template <typename I>
void  ManagedLock<I>::post_release_lock_handler(bool shutting_down, int r,
                                                Context *on_finish) {
  on_finish->complete(r);
}

template <typename I>
void ManagedLock<I>::post_reacquire_lock_handler(int r, Context *on_finish) {
  on_finish->complete(r);
}

template <typename I>
bool ManagedLock<I>::is_transition_state() const {
  switch (m_state) {
  case STATE_ACQUIRING:
  case STATE_WAITING_FOR_REGISTER:
  case STATE_REACQUIRING:
  case STATE_RELEASING:
  case STATE_PRE_SHUTTING_DOWN:
  case STATE_SHUTTING_DOWN:
  case STATE_INITIALIZING:
  case STATE_WAITING_FOR_LOCK:
  case STATE_POST_ACQUIRING:
  case STATE_PRE_RELEASING:
    return true;
  case STATE_UNLOCKED:
  case STATE_LOCKED:
  case STATE_SHUTDOWN:
  case STATE_UNINITIALIZED:
    break;
  }
  return false;
}

template <typename I>
void ManagedLock<I>::append_context(Action action, Context *ctx) {
  ceph_assert(ceph_mutex_is_locked(m_lock));

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
void ManagedLock<I>::execute_action(Action action, Context *ctx) {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  append_context(action, ctx);
  if (!is_transition_state()) {
    execute_next_action();
  }
}

template <typename I>
void ManagedLock<I>::execute_next_action() {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  ceph_assert(!m_actions_contexts.empty());
  switch (get_active_action()) {
  case ACTION_ACQUIRE_LOCK:
  case ACTION_TRY_LOCK:
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
    ceph_abort();
    break;
  }
}

template <typename I>
typename ManagedLock<I>::Action ManagedLock<I>::get_active_action() const {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  ceph_assert(!m_actions_contexts.empty());
  return m_actions_contexts.front().first;
}

template <typename I>
void ManagedLock<I>::complete_active_action(State next_state, int r) {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  ceph_assert(!m_actions_contexts.empty());

  ActionContexts action_contexts(std::move(m_actions_contexts.front()));
  m_actions_contexts.pop_front();
  m_state = next_state;

  m_lock.unlock();
  for (auto ctx : action_contexts.second) {
    ctx->complete(r);
  }
  m_lock.lock();

  if (!is_transition_state() && !m_actions_contexts.empty()) {
    execute_next_action();
  }
}

template <typename I>
bool ManagedLock<I>::is_state_shutdown() const {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  switch (m_state) {
  case STATE_PRE_SHUTTING_DOWN:
  case STATE_SHUTTING_DOWN:
  case STATE_SHUTDOWN:
    return true;
  default:
    break;
  }

  return (!m_actions_contexts.empty() &&
          m_actions_contexts.back().first == ACTION_SHUT_DOWN);
}

template <typename I>
void ManagedLock<I>::send_acquire_lock() {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  if (m_state == STATE_LOCKED) {
    complete_active_action(STATE_LOCKED, 0);
    return;
  }

  ldout(m_cct, 10) << dendl;

  uint64_t watch_handle = m_watcher->get_watch_handle();
  if (watch_handle == 0) {
    lderr(m_cct) << "watcher not registered - delaying request" << dendl;
    m_state = STATE_WAITING_FOR_REGISTER;

    // shut down might race w/ release/re-acquire of the lock
    if (is_state_shutdown()) {
      complete_active_action(STATE_UNLOCKED, -ESHUTDOWN);
    }
    return;
  }

  m_state = STATE_ACQUIRING;
  m_cookie = encode_lock_cookie(watch_handle);

  m_work_queue->queue(new LambdaContext([this](int r) {
    pre_acquire_lock_handler(create_context_callback<
        ManagedLock<I>, &ManagedLock<I>::handle_pre_acquire_lock>(this));
  }));
}

template <typename I>
void ManagedLock<I>::handle_pre_acquire_lock(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    handle_acquire_lock(r);
    return;
  }

  using managed_lock::AcquireRequest;
  AcquireRequest<I>* req = AcquireRequest<I>::create(
    m_ioctx, m_watcher, m_asio_engine, m_oid, m_cookie, m_mode == EXCLUSIVE,
    m_blacklist_on_break_lock, m_blacklist_expire_seconds,
    create_context_callback<
        ManagedLock<I>, &ManagedLock<I>::handle_acquire_lock>(this));
  m_work_queue->queue(new C_SendLockRequest<AcquireRequest<I>>(req), 0);
}

template <typename I>
void ManagedLock<I>::handle_acquire_lock(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r == -EBUSY || r == -EAGAIN) {
    ldout(m_cct, 5) << "unable to acquire exclusive lock" << dendl;
  } else if (r < 0) {
    lderr(m_cct) << "failed to acquire exclusive lock:" << cpp_strerror(r)
               << dendl;
  } else {
    ldout(m_cct, 5) << "successfully acquired exclusive lock" << dendl;
  }

  m_post_next_state = (r < 0 ? STATE_UNLOCKED : STATE_LOCKED);

  m_work_queue->queue(new LambdaContext([this, r](int ret) {
    post_acquire_lock_handler(r, create_context_callback<
        ManagedLock<I>, &ManagedLock<I>::handle_post_acquire_lock>(this));
  }));
}

template <typename I>
void ManagedLock<I>::handle_post_acquire_lock(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::lock_guard locker{m_lock};

  if (r < 0 && m_post_next_state == STATE_LOCKED) {
    // release_lock without calling pre and post handlers
    revert_to_unlock_state(r);
  } else if (r != -ECANCELED) {
    // fail the lock request
    complete_active_action(m_post_next_state, r);
  }
}

template <typename I>
void ManagedLock<I>::revert_to_unlock_state(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  using managed_lock::ReleaseRequest;
  ReleaseRequest<I>* req = ReleaseRequest<I>::create(m_ioctx, m_watcher,
      m_work_queue, m_oid, m_cookie,
      new LambdaContext([this, r](int ret) {
        std::lock_guard locker{m_lock};
        ceph_assert(ret == 0);
        complete_active_action(STATE_UNLOCKED, r);
      }));
  m_work_queue->queue(new C_SendLockRequest<ReleaseRequest<I>>(req));
}

template <typename I>
void ManagedLock<I>::send_reacquire_lock() {
  ceph_assert(ceph_mutex_is_locked(m_lock));

  if (m_state != STATE_LOCKED) {
    complete_active_action(m_state, 0);
    return;
  }

  ldout(m_cct, 10) << dendl;
  m_state = STATE_REACQUIRING;

  uint64_t watch_handle = m_watcher->get_watch_handle();
  if (watch_handle == 0) {
    // watch (re)failed while recovering
    lderr(m_cct) << "aborting reacquire due to invalid watch handle"
                 << dendl;

    // treat double-watch failure as a lost lock and invoke the
    // release/acquire handlers
    release_acquire_lock();
    complete_active_action(STATE_LOCKED, 0);
    return;
  }

  m_new_cookie = encode_lock_cookie(watch_handle);
  if (m_cookie == m_new_cookie && m_blacklist_on_break_lock) {
    ldout(m_cct, 10) << "skipping reacquire since cookie still valid"
                     << dendl;
    auto ctx = create_context_callback<
      ManagedLock, &ManagedLock<I>::handle_no_op_reacquire_lock>(this);
    post_reacquire_lock_handler(0, ctx);
    return;
  }

  auto ctx = create_context_callback<
    ManagedLock, &ManagedLock<I>::handle_reacquire_lock>(this);
  ctx = new LambdaContext([this, ctx](int r) {
      post_reacquire_lock_handler(r, ctx);
    });

  using managed_lock::ReacquireRequest;
  ReacquireRequest<I>* req = ReacquireRequest<I>::create(m_ioctx, m_oid,
      m_cookie, m_new_cookie, m_mode == EXCLUSIVE, ctx);
  m_work_queue->queue(new C_SendLockRequest<ReacquireRequest<I>>(req));
}

template <typename I>
void ManagedLock<I>::handle_reacquire_lock(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::lock_guard locker{m_lock};
  ceph_assert(m_state == STATE_REACQUIRING);

  if (r < 0) {
    if (r == -EOPNOTSUPP) {
      ldout(m_cct, 10) << "updating lock is not supported" << dendl;
    } else {
      lderr(m_cct) << "failed to update lock cookie: " << cpp_strerror(r)
                   << dendl;
    }

    release_acquire_lock();
  } else {
    m_cookie = m_new_cookie;
  }

  complete_active_action(STATE_LOCKED, 0);
}

template <typename I>
void ManagedLock<I>::handle_no_op_reacquire_lock(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;
  ceph_assert(m_state == STATE_REACQUIRING);
  ceph_assert(r >= 0);
  complete_active_action(STATE_LOCKED, 0);
}

template <typename I>
void ManagedLock<I>::release_acquire_lock() {
  assert(ceph_mutex_is_locked(m_lock));

  if (!is_state_shutdown()) {
    // queue a release and re-acquire of the lock since cookie cannot
    // be updated on older OSDs
    execute_action(ACTION_RELEASE_LOCK, nullptr);

    ceph_assert(!m_actions_contexts.empty());
    ActionContexts &action_contexts(m_actions_contexts.front());

    // reacquire completes when the request lock completes
    Contexts contexts;
    std::swap(contexts, action_contexts.second);
    if (contexts.empty()) {
      execute_action(ACTION_ACQUIRE_LOCK, nullptr);
    } else {
      for (auto ctx : contexts) {
        execute_action(ACTION_ACQUIRE_LOCK, ctx);
      }
    }
  }
}

template <typename I>
void ManagedLock<I>::send_release_lock() {
  ceph_assert(ceph_mutex_is_locked(m_lock));
  if (m_state == STATE_UNLOCKED) {
    complete_active_action(STATE_UNLOCKED, 0);
    return;
  }

  ldout(m_cct, 10) << dendl;
  m_state = STATE_PRE_RELEASING;

  m_work_queue->queue(new LambdaContext([this](int r) {
    pre_release_lock_handler(false, create_context_callback<
        ManagedLock<I>, &ManagedLock<I>::handle_pre_release_lock>(this));
  }));
}

template <typename I>
void ManagedLock<I>::handle_pre_release_lock(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  {
    std::lock_guard locker{m_lock};
    ceph_assert(m_state == STATE_PRE_RELEASING);
    m_state = STATE_RELEASING;
  }

  if (r < 0) {
    handle_release_lock(r);
    return;
  }

  using managed_lock::ReleaseRequest;
  ReleaseRequest<I>* req = ReleaseRequest<I>::create(m_ioctx, m_watcher,
      m_work_queue, m_oid, m_cookie,
      create_context_callback<
        ManagedLock<I>, &ManagedLock<I>::handle_release_lock>(this));
  m_work_queue->queue(new C_SendLockRequest<ReleaseRequest<I>>(req), 0);
}

template <typename I>
void ManagedLock<I>::handle_release_lock(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::lock_guard locker{m_lock};
  ceph_assert(m_state == STATE_RELEASING);

  if (r >= 0 || r == -EBLACKLISTED || r == -ENOENT) {
    m_cookie = "";
    m_post_next_state = STATE_UNLOCKED;
  } else {
    m_post_next_state = STATE_LOCKED;
  }

  m_work_queue->queue(new LambdaContext([this, r](int ret) {
    post_release_lock_handler(false, r, create_context_callback<
        ManagedLock<I>, &ManagedLock<I>::handle_post_release_lock>(this));
  }));
}

template <typename I>
void ManagedLock<I>::handle_post_release_lock(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::lock_guard locker{m_lock};
  complete_active_action(m_post_next_state, r);
}

template <typename I>
void ManagedLock<I>::send_shutdown() {
  ldout(m_cct, 10) << dendl;
  ceph_assert(ceph_mutex_is_locked(m_lock));
  if (m_state == STATE_UNLOCKED) {
    m_state = STATE_SHUTTING_DOWN;
    m_work_queue->queue(new LambdaContext([this](int r) {
      shutdown_handler(r, create_context_callback<
          ManagedLock<I>, &ManagedLock<I>::handle_shutdown>(this));
    }));
    return;
  }

  ceph_assert(m_state == STATE_LOCKED);
  m_state = STATE_PRE_SHUTTING_DOWN;

  m_lock.unlock();
  m_work_queue->queue(new C_ShutDownRelease(this), 0);
  m_lock.lock();
}

template <typename I>
void ManagedLock<I>::handle_shutdown(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  wait_for_tracked_ops(r);
}

template <typename I>
void ManagedLock<I>::send_shutdown_release() {
  ldout(m_cct, 10) << dendl;

  std::lock_guard locker{m_lock};

  m_work_queue->queue(new LambdaContext([this](int r) {
    pre_release_lock_handler(true, create_context_callback<
        ManagedLock<I>, &ManagedLock<I>::handle_shutdown_pre_release>(this));
  }));
}

template <typename I>
void ManagedLock<I>::handle_shutdown_pre_release(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  std::string cookie;
  {
    std::lock_guard locker{m_lock};
    cookie = m_cookie;

    ceph_assert(m_state == STATE_PRE_SHUTTING_DOWN);
    m_state = STATE_SHUTTING_DOWN;
  }

  using managed_lock::ReleaseRequest;
  ReleaseRequest<I>* req = ReleaseRequest<I>::create(m_ioctx, m_watcher,
      m_work_queue, m_oid, cookie,
      new LambdaContext([this, r](int l) {
        int rst = r < 0 ? r : l;
        post_release_lock_handler(true, rst, create_context_callback<
            ManagedLock<I>, &ManagedLock<I>::handle_shutdown_post_release>(this));
      }));
  req->send();

}

template <typename I>
void ManagedLock<I>::handle_shutdown_post_release(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  wait_for_tracked_ops(r);
}

template <typename I>
void ManagedLock<I>::wait_for_tracked_ops(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  Context *ctx = new LambdaContext([this, r](int ret) {
      complete_shutdown(r);
    });

  m_async_op_tracker.wait_for_ops(ctx);
}

template <typename I>
void ManagedLock<I>::complete_shutdown(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to shut down lock: " << cpp_strerror(r)
               << dendl;
  }

  ActionContexts action_contexts;
  {
    std::lock_guard locker{m_lock};
    ceph_assert(ceph_mutex_is_locked(m_lock));
    ceph_assert(m_actions_contexts.size() == 1);

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

template class librbd::ManagedLock<librbd::ImageCtx>;
