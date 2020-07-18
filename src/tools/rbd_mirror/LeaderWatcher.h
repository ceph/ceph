// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_LEADER_WATCHER_H
#define CEPH_RBD_MIRROR_LEADER_WATCHER_H

#include <list>
#include <memory>
#include <string>

#include "common/AsyncOpTracker.h"
#include "librbd/ManagedLock.h"
#include "librbd/Watcher.h"
#include "librbd/managed_lock/Types.h"
#include "librbd/watcher/Types.h"
#include "Instances.h"
#include "tools/rbd_mirror/instances/Types.h"
#include "tools/rbd_mirror/leader_watcher/Types.h"

namespace librbd {
class ImageCtx;
namespace asio { struct ContextWQ; }
} // namespace librbd

namespace rbd {
namespace mirror {

template <typename> struct Threads;

template <typename ImageCtxT = librbd::ImageCtx>
class LeaderWatcher : protected librbd::Watcher {
  using librbd::Watcher::unregister_watch; // Silence overloaded virtual warning
public:
  static LeaderWatcher* create(Threads<ImageCtxT> *threads,
                               librados::IoCtx &io_ctx,
                               leader_watcher::Listener *listener) {
    return new LeaderWatcher(threads, io_ctx, listener);
  }

  LeaderWatcher(Threads<ImageCtxT> *threads, librados::IoCtx &io_ctx,
                leader_watcher::Listener *listener);
  ~LeaderWatcher() override;

  int init();
  void shut_down();

  void init(Context *on_finish);
  void shut_down(Context *on_finish);

  bool is_blacklisted() const;
  bool is_leader() const;
  bool is_releasing_leader() const;
  bool get_leader_instance_id(std::string *instance_id) const;
  void release_leader();
  void list_instances(std::vector<std::string> *instance_ids);

  std::string get_instance_id();

private:
  /**
   * @verbatim
   *
   *  <uninitialized> <------------------------------ WAIT_FOR_TASKS
   *     | (init)            ^                                ^
   *     v                   *                                |
   *  CREATE_OBJECT  * * * * *  (error)               UNREGISTER_WATCH
   *     |                   *                                ^
   *     v                   *                                |
   *  REGISTER_WATCH * * * * *                        SHUT_DOWN_LEADER_LOCK
   *     |                                                    ^
   *     |           (no leader heartbeat and acquire failed) |
   *     | BREAK_LOCK <-------------------------------------\ |
   *     |    |                 (no leader heartbeat)       | | (shut down)
   *     |    |  /----------------------------------------\ | |
   *     |    |  |              (lock_released received)    | |
   *     |    |  |  /-------------------------------------\ | |
   *     |    |  |  |                   (lock_acquired or | | |
   *     |    |  |  |                 heartbeat received) | | |
   *     |    |  |  |       (ENOENT)        /-----------\ | | |
   *     |    |  |  |  * * * * * * * * * *  |           | | | |
   *     v    v  v  v  v  (error)        *  v           | | | |
   *  ACQUIRE_LEADER_LOCK  * * * * *> GET_LOCKER ---> <secondary>
   *     |                   *                           ^
   * ....|...................*....................  .....|.....................
   * .   v                   *                   .  .    |       post_release .
   * .INIT_INSTANCES * * * * *                   .  .NOTIFY_LOCK_RELEASED     .
   * .   |                                       .  .....^.....................
   * .   v                                       .       |
   * .NOTIFY_LISTENER                            .   RELEASE_LEADER_LOCK
   * .   |                                       .       ^
   * .   v                                       .  .....|.....................
   * .NOTIFY_LOCK_ACQUIRED                       .  .    |                    .
   * .   |                          post_acquire .  .SHUT_DOWN_INSTANCES      .
   * ....|........................................  .    ^                    .
   *     v                                          .    |                    .
   *  <leader> -----------------------------------> .NOTIFY_LISTENER          .
   *            (shut_down, release_leader,         .             pre_release .
   *             notify error)                      ...........................
   * @endverbatim
   */

  struct InstancesListener : public instances::Listener {
    LeaderWatcher* leader_watcher;

    InstancesListener(LeaderWatcher* leader_watcher)
      : leader_watcher(leader_watcher) {
    }

    void handle_added(const InstanceIds& instance_ids) override {
      leader_watcher->m_listener->handle_instances_added(instance_ids);
    }

    void handle_removed(const InstanceIds& instance_ids) override {
      leader_watcher->m_listener->handle_instances_removed(instance_ids);
    }
  };

  class LeaderLock : public librbd::ManagedLock<ImageCtxT> {
  public:
    typedef librbd::ManagedLock<ImageCtxT> Parent;

    LeaderLock(librados::IoCtx& ioctx, librbd::AsioEngine& asio_engine,
               const std::string& oid, LeaderWatcher *watcher,
               bool blacklist_on_break_lock,
               uint32_t blacklist_expire_seconds)
      : Parent(ioctx, asio_engine, oid, watcher,
               librbd::managed_lock::EXCLUSIVE, blacklist_on_break_lock,
               blacklist_expire_seconds),
        watcher(watcher) {
    }

    bool is_leader() const {
      std::lock_guard locker{Parent::m_lock};
      return Parent::is_state_post_acquiring() || Parent::is_state_locked();
    }

    bool is_releasing_leader() const {
      std::lock_guard locker{Parent::m_lock};
      return Parent::is_state_pre_releasing();
    }

  protected:
    void post_acquire_lock_handler(int r, Context *on_finish) {
      if (r == 0) {
        // lock is owned at this point
	std::lock_guard locker{Parent::m_lock};
        Parent::set_state_post_acquiring();
      }
      watcher->handle_post_acquire_leader_lock(r, on_finish);
    }
    void pre_release_lock_handler(bool shutting_down,
                                  Context *on_finish) {
      watcher->handle_pre_release_leader_lock(on_finish);
    }
    void post_release_lock_handler(bool shutting_down, int r,
                                   Context *on_finish) {
      watcher->handle_post_release_leader_lock(r, on_finish);
    }
  private:
    LeaderWatcher *watcher;
  };

  struct HandlePayloadVisitor : public boost::static_visitor<void> {
    LeaderWatcher *leader_watcher;
    Context *on_notify_ack;

    HandlePayloadVisitor(LeaderWatcher *leader_watcher, Context *on_notify_ack)
      : leader_watcher(leader_watcher), on_notify_ack(on_notify_ack) {
    }

    template <typename Payload>
    inline void operator()(const Payload &payload) const {
      leader_watcher->handle_payload(payload, on_notify_ack);
    }
  };

  struct C_GetLocker : public Context {
    LeaderWatcher *leader_watcher;
    librbd::managed_lock::Locker locker;

    C_GetLocker(LeaderWatcher *leader_watcher)
      : leader_watcher(leader_watcher) {
    }

    void finish(int r) override {
      leader_watcher->handle_get_locker(r, locker);
    }
  };

  typedef void (LeaderWatcher<ImageCtxT>::*TimerCallback)();

  struct C_TimerGate : public Context {
    LeaderWatcher *leader_watcher;

    bool leader = false;
    TimerCallback timer_callback = nullptr;

    C_TimerGate(LeaderWatcher *leader_watcher)
      : leader_watcher(leader_watcher) {
    }

    void finish(int r) override {
      leader_watcher->m_timer_gate = nullptr;
      leader_watcher->execute_timer_task(leader, timer_callback);
    }
  };

  Threads<ImageCtxT> *m_threads;
  leader_watcher::Listener *m_listener;

  InstancesListener m_instances_listener;
  mutable ceph::mutex m_lock;
  uint64_t m_notifier_id;
  std::string m_instance_id;
  LeaderLock *m_leader_lock;
  Context *m_on_finish = nullptr;
  Context *m_on_shut_down_finish = nullptr;
  uint64_t m_acquire_attempts = 0;
  int m_ret_val = 0;
  Instances<ImageCtxT> *m_instances = nullptr;
  librbd::managed_lock::Locker m_locker;

  bool m_blacklisted = false;

  AsyncOpTracker m_timer_op_tracker;
  Context *m_timer_task = nullptr;
  C_TimerGate *m_timer_gate = nullptr;

  librbd::watcher::NotifyResponse m_heartbeat_response;

  bool is_leader(ceph::mutex &m_lock) const;
  bool is_releasing_leader(ceph::mutex &m_lock) const;

  void cancel_timer_task();
  void schedule_timer_task(const std::string &name,
                           int delay_factor, bool leader,
                           TimerCallback callback, bool shutting_down);
  void execute_timer_task(bool leader, TimerCallback timer_callback);

  void create_leader_object();
  void handle_create_leader_object(int r);

  void register_watch();
  void handle_register_watch(int r);

  void shut_down_leader_lock();
  void handle_shut_down_leader_lock(int r);

  void unregister_watch();
  void handle_unregister_watch(int r);

  void wait_for_tasks();
  void handle_wait_for_tasks();

  void break_leader_lock();
  void handle_break_leader_lock(int r);

  void schedule_get_locker(bool reset_leader, uint32_t delay_factor);
  void get_locker();
  void handle_get_locker(int r, librbd::managed_lock::Locker& locker);

  void schedule_acquire_leader_lock(uint32_t delay_factor);
  void acquire_leader_lock();
  void handle_acquire_leader_lock(int r);

  void release_leader_lock();
  void handle_release_leader_lock(int r);

  void init_instances();
  void handle_init_instances(int r);

  void shut_down_instances();
  void handle_shut_down_instances(int r);

  void notify_listener();
  void handle_notify_listener(int r);

  void notify_lock_acquired();
  void handle_notify_lock_acquired(int r);

  void notify_lock_released();
  void handle_notify_lock_released(int r);

  void notify_heartbeat();
  void handle_notify_heartbeat(int r);

  void handle_post_acquire_leader_lock(int r, Context *on_finish);
  void handle_pre_release_leader_lock(Context *on_finish);
  void handle_post_release_leader_lock(int r, Context *on_finish);

  void handle_notify(uint64_t notify_id, uint64_t handle,
                     uint64_t notifier_id, bufferlist &bl) override;

  void handle_rewatch_complete(int r) override;

  void handle_heartbeat(Context *on_ack);
  void handle_lock_acquired(Context *on_ack);
  void handle_lock_released(Context *on_ack);

  void handle_payload(const leader_watcher::HeartbeatPayload &payload,
                      Context *on_notify_ack);
  void handle_payload(const leader_watcher::LockAcquiredPayload &payload,
                      Context *on_notify_ack);
  void handle_payload(const leader_watcher::LockReleasedPayload &payload,
                      Context *on_notify_ack);
  void handle_payload(const leader_watcher::UnknownPayload &payload,
                      Context *on_notify_ack);
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_LEADER_WATCHER_H
