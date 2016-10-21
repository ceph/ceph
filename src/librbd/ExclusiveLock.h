// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_EXCLUSIVE_LOCK_H
#define CEPH_LIBRBD_EXCLUSIVE_LOCK_H

#include "include/int_types.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "common/Mutex.h"
#include <list>
#include <string>
#include <utility>

namespace librbd {

namespace managed_lock {
class LockWatcher;
}

class ImageCtx;
template <typename> class Lock;

template <typename ImageCtxT = ImageCtx>
class ExclusiveLock {
public:
  static ExclusiveLock *create(ImageCtxT &image_ctx) {
    return new ExclusiveLock<ImageCtxT>(image_ctx);
  }

  ExclusiveLock(ImageCtxT &image_ctx);
  ~ExclusiveLock();

  bool is_lock_owner() const;
  bool accept_requests(int *ret_val) const;

  void block_requests(int r);
  void unblock_requests();

  void init(uint64_t features, Context *on_init);
  void shut_down(Context *on_shutdown);

  void try_lock(Context *on_tried_lock);
  void request_lock(Context *on_locked);
  void release_lock(Context *on_released);

  void reacquire_lock(Context *on_reacquired = nullptr);

  void handle_peer_notification();

  void assert_header_locked(librados::ObjectWriteOperation *op);

private:

  /**
   * @verbatim
   *
   * <start>
   *    |
   *    |
   *    |
   *    |
   *    |
   *    |
   *    |
   *    v            (init)
   * UNINITIALIZED  -------> UNLOCKED ------------------------> ACQUIRING
   *                            ^                                   |
   *                            |                                   |
   *                            |                                   |
   *                            |                                   |
   *                            |                                   |
   *                            |          (release_lock)           v
   *                          RELEASING <------------------------ LOCKED
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
   * SHUTTING_DOWN ---> SHUTDOWN ---> <finish>
   *
   * @endverbatim
   */
  enum State {
    STATE_UNINITIALIZED,
    STATE_UNLOCKED,
    STATE_LOCKED,
    STATE_INITIALIZING,
    STATE_ACQUIRING,
    STATE_REACQUIRING,
    STATE_RELEASING,
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

  struct C_InitComplete : public Context {
    ExclusiveLock *exclusive_lock;
    Context *on_init;
    C_InitComplete(ExclusiveLock *exclusive_lock, Context *on_init)
      : exclusive_lock(exclusive_lock), on_init(on_init) {
    }
    virtual void finish(int r) override {
      if (r == 0) {
        exclusive_lock->handle_init_complete();
      }
      on_init->complete(r);
    }
  };

  struct C_ShutDownRelease : public Context {
    ExclusiveLock *exclusive_lock;
    C_ShutDownRelease(ExclusiveLock *exclusive_lock)
      : exclusive_lock(exclusive_lock) {
    }
    virtual void finish(int r) override {
      exclusive_lock->send_shutdown_release();
    }
  };

  ImageCtxT &m_image_ctx;
  Lock<managed_lock::LockWatcher> *m_managed_lock;

  mutable Mutex m_lock;
  State m_state;

  ActionsContexts m_actions_contexts;

  uint32_t m_request_blocked_count = 0;
  int m_request_blocked_ret_val = 0;

  bool is_transition_state() const;

  void append_context(Action action, Context *ctx);
  void execute_action(Action action, Context *ctx);
  void execute_next_action();

  Action get_active_action() const;
  void complete_active_action(State next_state, int r);

  bool is_shutdown() const;

  void handle_init_complete();

  void send_acquire_lock();
  void handle_acquire_lock(int r);

  void send_reacquire_lock();
  void handle_reacquire_lock(int r);

  void send_release_lock();
  void handle_release_lock(int r);

  void send_shutdown();
  void send_shutdown_release();
  void handle_shutdown_released(int r);
  void handle_shutdown(int r);
  void complete_shutdown(int r);
};

} // namespace librbd

extern template class librbd::ExclusiveLock<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_EXCLUSIVE_LOCK_H
