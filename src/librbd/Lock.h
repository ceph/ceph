// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_LOCK_H
#define CEPH_LIBRBD_LOCK_H

#include "include/int_types.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "cls/lock/cls_lock_types.h"
#include "common/Mutex.h"
#include <list>
#include <string>
#include <utility>

class ContextWQ;

namespace librbd {

namespace managed_lock {
  class LockWatcher;
  class Policy;
}

using managed_lock::Policy;

class Lock {
public:
  static const std::string WATCHER_LOCK_TAG;

  static Lock *create(librados::IoCtx& ioctx,
                      const std::string& oid,
                      Policy *policy = nullptr) {
    return new Lock(ioctx, oid, policy);
  }

  Lock(librados::IoCtx& ioctx, const std::string& oid,
       Policy *policy = nullptr);
  ~Lock();

  bool is_lock_owner() const;

  void shut_down(Context *on_shutdown);

  void try_lock(Context *on_tried_lock);
  void request_lock(Context *on_locked);
  void release_lock(Context *on_released);

  void reacquire_lock(Context *on_reacquired = nullptr);

  void handle_peer_notification();

  void assert_locked(librados::ObjectWriteOperation *op, ClsLockType type);

  inline Policy *policy() {
    return m_policy;
  }

  inline librados::IoCtx& io_ctx() const {
    return m_ioctx;
  }

  inline const std::string& oid() const {
    return m_oid;
  }

  inline ContextWQ *work_queue() const {
    return m_work_queue;
  }

  bool is_shutdown() const {
    Mutex::Locker l(m_lock);
    return is_shutdown_locked();
  }

  bool is_locked() const {
    return m_state == STATE_LOCKED;
  }

  static bool decode_lock_cookie(const std::string &cookie, uint64_t *handle);

private:

  /**
   * @verbatim
   *
   *       <start>
   *          |
   *          |
   *          |                          * * > WAITING_FOR_PEER ------------\
   *          |                          * (request_lock busy)              |
   *          |                          *                                  |
   *          |                          * * * * * * * * * * * * * *        |
   *          |                                                    *        |
   *          v           (try_lock/request_lock)                  *        |
   *       UNLOCKED -----------------------------------------> ACQUIRING <--/
   *          ^                                                    |
   *          |                                                    v
   *      RELEASING                                          POST_ACQUIRING
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
    STATE_UNLOCKED,
    STATE_LOCKED,
    STATE_ACQUIRING,
    STATE_POST_ACQUIRING,
    STATE_WAITING_FOR_PEER,
    STATE_WAITING_FOR_REGISTER,
    STATE_REACQUIRING,
    STATE_PRE_RELEASING,
    STATE_RELEASING,
    STATE_PRE_SHUTTING_DOWN,
    STATE_SHUTTING_DOWN,
    STATE_SHUTDOWN
  };

  enum Action {
    ACTION_TRY_LOCK,
    ACTION_REQUEST_LOCK,
    ACTION_REACQUIRE_LOCK,
    ACTION_RELEASE_LOCK,
    ACTION_SHUT_DOWN
  };

  typedef std::list<Context *> Contexts;
  typedef std::pair<Action, Contexts> ActionContexts;
  typedef std::list<ActionContexts> ActionsContexts;

  struct C_ShutDownRelease : public Context {
    Lock *lock;
    C_ShutDownRelease(Lock *lock)
      : lock(lock) {
    }
    virtual void finish(int r) override {
      lock->send_shutdown_release();
    }
  };

  librados::IoCtx& m_ioctx;
  CephContext *m_cct;
  ContextWQ *m_work_queue;
  std::string m_oid;
  managed_lock::LockWatcher *m_watcher;
  Policy *m_policy;

  mutable Mutex m_lock;
  State m_state;
  std::string m_cookie;
  std::string m_new_cookie;

  ActionsContexts m_actions_contexts;

  std::string encode_lock_cookie() const;

  bool is_transition_state() const;

  void append_context(Action action, Context *ctx);
  void execute_action(Action action, Context *ctx);
  void execute_next_action();

  Action get_active_action() const;
  void complete_active_action(State next_state, int r);

  bool is_shutdown_locked() const;

  void send_acquire_lock();
  void handle_acquiring_lock(int r);
  void handle_acquire_lock(int r);

  void send_reacquire_lock();
  void handle_reacquire_lock(int r);

  void send_release_lock();
  void handle_releasing_lock(int r);
  void handle_release_lock(int r);

  void send_shutdown();
  void send_shutdown_release();
  void handle_shutdown_releasing(int r);
  void handle_shutdown_released(int r);
  void handle_shutdown(int r);
  void complete_shutdown(int r);
};

} // namespace librbd

#endif // CEPH_LIBRBD_LOCK_H
