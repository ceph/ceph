// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MANAGED_LOCK_H
#define CEPH_LIBRBD_MANAGED_LOCK_H

#include "include/int_types.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "common/AsyncOpTracker.h"
#include "cls/lock/cls_lock_types.h"
#include "librbd/watcher/Types.h"
#include "librbd/managed_lock/Types.h"
#include <list>
#include <string>
#include <utility>

namespace librbd {

struct AsioEngine;
struct ImageCtx;
namespace asio { struct ContextWQ; }
namespace managed_lock { struct Locker; }

template <typename ImageCtxT = librbd::ImageCtx>
class ManagedLock {
private:
  typedef watcher::Traits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::Watcher Watcher;

public:
  static ManagedLock *create(librados::IoCtx& ioctx,
                             AsioEngine& asio_engine,
                             const std::string& oid, Watcher *watcher,
                             managed_lock::Mode mode,
                             bool blacklist_on_break_lock,
                             uint32_t blacklist_expire_seconds) {
    return new ManagedLock(ioctx, asio_engine, oid, watcher, mode,
                           blacklist_on_break_lock, blacklist_expire_seconds);
  }
  void destroy() {
    delete this;
  }

  ManagedLock(librados::IoCtx& ioctx, AsioEngine& asio_engine,
              const std::string& oid, Watcher *watcher,
              managed_lock::Mode mode, bool blacklist_on_break_lock,
              uint32_t blacklist_expire_seconds);
  virtual ~ManagedLock();

  bool is_lock_owner() const;

  void shut_down(Context *on_shutdown);
  void acquire_lock(Context *on_acquired);
  void try_acquire_lock(Context *on_acquired);
  void release_lock(Context *on_released);
  void reacquire_lock(Context *on_reacquired);
  void get_locker(managed_lock::Locker *locker, Context *on_finish);
  void break_lock(const managed_lock::Locker &locker, bool force_break_lock,
                  Context *on_finish);

  int assert_header_locked();

  bool is_shutdown() const {
    std::lock_guard l{m_lock};
    return is_state_shutdown();
  }

protected:
  mutable ceph::mutex m_lock;

  inline void set_state_uninitialized() {
    ceph_assert(ceph_mutex_is_locked(m_lock));
    ceph_assert(m_state == STATE_UNLOCKED);
    m_state = STATE_UNINITIALIZED;
  }
  inline void set_state_initializing() {
    ceph_assert(ceph_mutex_is_locked(m_lock));
    ceph_assert(m_state == STATE_UNINITIALIZED);
    m_state = STATE_INITIALIZING;
  }
  inline void set_state_unlocked() {
    ceph_assert(ceph_mutex_is_locked(m_lock));
    ceph_assert(m_state == STATE_INITIALIZING || m_state == STATE_RELEASING);
    m_state = STATE_UNLOCKED;
  }
  inline void set_state_waiting_for_lock() {
    ceph_assert(ceph_mutex_is_locked(m_lock));
    ceph_assert(m_state == STATE_ACQUIRING);
    m_state = STATE_WAITING_FOR_LOCK;
  }
  inline void set_state_post_acquiring() {
    ceph_assert(ceph_mutex_is_locked(m_lock));
    ceph_assert(m_state == STATE_ACQUIRING);
    m_state = STATE_POST_ACQUIRING;
  }

  bool is_state_shutdown() const;
  inline bool is_state_acquiring() const {
    ceph_assert(ceph_mutex_is_locked(m_lock));
    return m_state == STATE_ACQUIRING;
  }
  inline bool is_state_post_acquiring() const {
    ceph_assert(ceph_mutex_is_locked(m_lock));
    return m_state == STATE_POST_ACQUIRING;
  }
  inline bool is_state_releasing() const {
    ceph_assert(ceph_mutex_is_locked(m_lock));
    return m_state == STATE_RELEASING;
  }
  inline bool is_state_pre_releasing() const {
    ceph_assert(ceph_mutex_is_locked(m_lock));
    return m_state == STATE_PRE_RELEASING;
  }
  inline bool is_state_locked() const {
    ceph_assert(ceph_mutex_is_locked(m_lock));
    return m_state == STATE_LOCKED;
  }
  inline bool is_state_waiting_for_lock() const {
    ceph_assert(ceph_mutex_is_locked(m_lock));
    return m_state == STATE_WAITING_FOR_LOCK;
  }

  inline bool is_action_acquire_lock() const {
    ceph_assert(ceph_mutex_is_locked(m_lock));
    return get_active_action() == ACTION_ACQUIRE_LOCK;
  }

  virtual void shutdown_handler(int r, Context *on_finish);
  virtual void pre_acquire_lock_handler(Context *on_finish);
  virtual void post_acquire_lock_handler(int r, Context *on_finish);
  virtual void pre_release_lock_handler(bool shutting_down,
                                        Context *on_finish);
  virtual void post_release_lock_handler(bool shutting_down, int r,
                                          Context *on_finish);
  virtual void post_reacquire_lock_handler(int r, Context *on_finish);

  void execute_next_action();

private:
  /**
   * @verbatim
   *
   *       <start>
   *          |
   *          |
   *          v           (acquire_lock)
   *       UNLOCKED -----------------------------------------> ACQUIRING
   *          ^                                                    |
   *          |                                                    |
   *      RELEASING                                                |
   *          |                                                    |
   *          |                                                    |
   *          |                    (release_lock)                  v
   *    PRE_RELEASING <----------------------------------------- LOCKED
   *
   * <LOCKED state>
   *    |
   *    v
   * REACQUIRING -------------------------------------> <finish>
   *    .                                                 ^
   *    .                                                 |
   *    . . . > <RELEASE action> ---> <ACQUIRE action> ---/
   *
   * <UNLOCKED/LOCKED states>
   *    |
   *    |
   *    v
   * PRE_SHUTTING_DOWN ---> SHUTTING_DOWN ---> SHUTDOWN ---> <finish>
   *
   * @endverbatim
   */
  enum State {
    STATE_UNINITIALIZED,
    STATE_INITIALIZING,
    STATE_UNLOCKED,
    STATE_LOCKED,
    STATE_ACQUIRING,
    STATE_POST_ACQUIRING,
    STATE_WAITING_FOR_REGISTER,
    STATE_WAITING_FOR_LOCK,
    STATE_REACQUIRING,
    STATE_PRE_RELEASING,
    STATE_RELEASING,
    STATE_PRE_SHUTTING_DOWN,
    STATE_SHUTTING_DOWN,
    STATE_SHUTDOWN,
  };

  enum Action {
    ACTION_TRY_LOCK,
    ACTION_ACQUIRE_LOCK,
    ACTION_REACQUIRE_LOCK,
    ACTION_RELEASE_LOCK,
    ACTION_SHUT_DOWN
  };

  typedef std::list<Context *> Contexts;
  typedef std::pair<Action, Contexts> ActionContexts;
  typedef std::list<ActionContexts> ActionsContexts;

  struct C_ShutDownRelease : public Context {
    ManagedLock *lock;
    C_ShutDownRelease(ManagedLock *lock)
      : lock(lock) {
    }
    void finish(int r) override {
      lock->send_shutdown_release();
    }
  };

  librados::IoCtx& m_ioctx;
  CephContext *m_cct;
  AsioEngine& m_asio_engine;
  asio::ContextWQ* m_work_queue;
  std::string m_oid;
  Watcher *m_watcher;
  managed_lock::Mode m_mode;
  bool m_blacklist_on_break_lock;
  uint32_t m_blacklist_expire_seconds;

  std::string m_cookie;
  std::string m_new_cookie;

  State m_state;
  State m_post_next_state;

  ActionsContexts m_actions_contexts;
  AsyncOpTracker m_async_op_tracker;

  bool is_lock_owner(ceph::mutex &lock) const;
  bool is_transition_state() const;

  void append_context(Action action, Context *ctx);
  void execute_action(Action action, Context *ctx);

  Action get_active_action() const;
  void complete_active_action(State next_state, int r);

  void send_acquire_lock();
  void handle_pre_acquire_lock(int r);
  void handle_acquire_lock(int r);
  void handle_no_op_reacquire_lock(int r);

  void handle_post_acquire_lock(int r);
  void revert_to_unlock_state(int r);

  void send_reacquire_lock();
  void handle_reacquire_lock(int r);
  void release_acquire_lock();

  void send_release_lock();
  void handle_pre_release_lock(int r);
  void handle_release_lock(int r);
  void handle_post_release_lock(int r);

  void send_shutdown();
  void handle_shutdown(int r);
  void send_shutdown_release();
  void handle_shutdown_pre_release(int r);
  void handle_shutdown_post_release(int r);
  void wait_for_tracked_ops(int r);
  void complete_shutdown(int r);
};

} // namespace librbd

extern template class librbd::ManagedLock<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MANAGED_LOCK_H
