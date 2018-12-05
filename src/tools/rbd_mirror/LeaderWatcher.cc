// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "LeaderWatcher.h"
#include "common/Timer.h"
#include "common/debug.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "include/stringify.h"
#include "librbd/Utils.h"
#include "librbd/watcher/Types.h"
#include "Threads.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::LeaderWatcher: " \
                           << this << " " << __func__ << ": "
namespace rbd {
namespace mirror {

using namespace leader_watcher;

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
LeaderWatcher<I>::LeaderWatcher(Threads<I> *threads, librados::IoCtx &io_ctx,
                                leader_watcher::Listener *listener)
  : Watcher(io_ctx, threads->work_queue, RBD_MIRROR_LEADER),
    m_threads(threads), m_listener(listener), m_instances_listener(this),
    m_lock("rbd::mirror::LeaderWatcher " + io_ctx.get_pool_name()),
    m_notifier_id(librados::Rados(io_ctx).get_instance_id()),
    m_instance_id(stringify(m_notifier_id)),
    m_leader_lock(new LeaderLock(m_ioctx, m_work_queue, m_oid, this, true,
                                 m_cct->_conf.get_val<uint64_t>(
                                   "rbd_blacklist_expire_seconds"))) {
}

template <typename I>
LeaderWatcher<I>::~LeaderWatcher() {
  ceph_assert(m_status_watcher == nullptr);
  ceph_assert(m_instances == nullptr);
  ceph_assert(m_timer_task == nullptr);

  delete m_leader_lock;
}

template <typename I>
std::string LeaderWatcher<I>::get_instance_id() {
  return m_instance_id;
}

template <typename I>
int LeaderWatcher<I>::init() {
  C_SaferCond init_ctx;
  init(&init_ctx);
  return init_ctx.wait();
}

template <typename I>
void LeaderWatcher<I>::init(Context *on_finish) {
  dout(10) << "notifier_id=" << m_notifier_id << dendl;

  Mutex::Locker locker(m_lock);

  ceph_assert(m_on_finish == nullptr);
  m_on_finish = on_finish;

  create_leader_object();
}

template <typename I>
void LeaderWatcher<I>::create_leader_object() {
  dout(10) << dendl;

  ceph_assert(m_lock.is_locked());

  librados::ObjectWriteOperation op;
  op.create(false);

  librados::AioCompletion *aio_comp = create_rados_callback<
    LeaderWatcher<I>, &LeaderWatcher<I>::handle_create_leader_object>(this);
  int r = m_ioctx.aio_operate(m_oid, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void LeaderWatcher<I>::handle_create_leader_object(int r) {
  dout(10) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);

    if (r == 0) {
      register_watch();
      return;
    }

    derr << "error creating " << m_oid << " object: " << cpp_strerror(r)
         << dendl;

    std::swap(on_finish, m_on_finish);
  }
  on_finish->complete(r);
}

template <typename I>
void LeaderWatcher<I>::register_watch() {
  dout(10) << dendl;

  ceph_assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
      LeaderWatcher<I>, &LeaderWatcher<I>::handle_register_watch>(this));

  librbd::Watcher::register_watch(ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_register_watch(int r) {
  dout(10) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  if (r < 0) {
    Mutex::Locker locker(m_lock);
    derr << "error registering leader watcher for " << m_oid << " object: "
         << cpp_strerror(r) << dendl;
    ceph_assert(m_on_finish != nullptr);
    std::swap(on_finish, m_on_finish);
  } else {
    Mutex::Locker locker(m_lock);
    init_status_watcher();
    return;
  }

  on_finish->complete(r);
}

template <typename I>
void LeaderWatcher<I>::shut_down() {
  C_SaferCond shut_down_ctx;
  shut_down(&shut_down_ctx);
  int r = shut_down_ctx.wait();
  ceph_assert(r == 0);
}

template <typename I>
void LeaderWatcher<I>::shut_down(Context *on_finish) {
  dout(10) << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);

  ceph_assert(m_on_shut_down_finish == nullptr);
  m_on_shut_down_finish = on_finish;
  cancel_timer_task();
  shut_down_leader_lock();
}

template <typename I>
void LeaderWatcher<I>::shut_down_leader_lock() {
  dout(10) << dendl;

  ceph_assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
      LeaderWatcher<I>, &LeaderWatcher<I>::handle_shut_down_leader_lock>(this));

  m_leader_lock->shut_down(ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_shut_down_leader_lock(int r) {
  dout(10) << "r=" << r << dendl;

  Mutex::Locker locker(m_lock);

  if (r < 0) {
    derr << "error shutting down leader lock: " << cpp_strerror(r) << dendl;
  }

  shut_down_status_watcher();
}

template <typename I>
void LeaderWatcher<I>::unregister_watch() {
  dout(10) << dendl;

  ceph_assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
      LeaderWatcher<I>, &LeaderWatcher<I>::handle_unregister_watch>(this));

  librbd::Watcher::unregister_watch(ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_unregister_watch(int r) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    derr << "error unregistering leader watcher for " << m_oid << " object: "
         << cpp_strerror(r) << dendl;
  }
  wait_for_tasks();
}

template <typename I>
void LeaderWatcher<I>::wait_for_tasks() {
  dout(10) << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);
  schedule_timer_task("wait for tasks", 0, false,
                      &LeaderWatcher<I>::handle_wait_for_tasks, true);
}

template <typename I>
void LeaderWatcher<I>::handle_wait_for_tasks() {
  dout(10) << dendl;

  ceph_assert(m_threads->timer_lock.is_locked());
  ceph_assert(m_lock.is_locked());
  ceph_assert(m_on_shut_down_finish != nullptr);

  ceph_assert(!m_timer_op_tracker.empty());
  m_timer_op_tracker.finish_op();

  auto ctx = new FunctionContext([this](int r) {
      Context *on_finish;
      {
        // ensure lock isn't held when completing shut down
        Mutex::Locker locker(m_lock);
        ceph_assert(m_on_shut_down_finish != nullptr);
        on_finish = m_on_shut_down_finish;
      }
      on_finish->complete(0);
    });
  m_work_queue->queue(ctx, 0);
}

template <typename I>
bool LeaderWatcher<I>::is_leader() const {
  Mutex::Locker locker(m_lock);

  return is_leader(m_lock);
}

template <typename I>
bool LeaderWatcher<I>::is_leader(Mutex &lock) const {
  ceph_assert(m_lock.is_locked());

  bool leader = m_leader_lock->is_leader();
  dout(10) << leader << dendl;
  return leader;
}

template <typename I>
bool LeaderWatcher<I>::is_releasing_leader() const {
  Mutex::Locker locker(m_lock);

  return is_releasing_leader(m_lock);
}

template <typename I>
bool LeaderWatcher<I>::is_releasing_leader(Mutex &lock) const {
  ceph_assert(m_lock.is_locked());

  bool releasing = m_leader_lock->is_releasing_leader();
  dout(10) << releasing << dendl;
  return releasing;
}

template <typename I>
bool LeaderWatcher<I>::get_leader_instance_id(std::string *instance_id) const {
  dout(10) << dendl;

  Mutex::Locker locker(m_lock);

  if (is_leader(m_lock) || is_releasing_leader(m_lock)) {
    *instance_id = m_instance_id;
    return true;
  }

  if (!m_locker.cookie.empty()) {
    *instance_id = stringify(m_locker.entity.num());
    return true;
  }

  return false;
}

template <typename I>
void LeaderWatcher<I>::release_leader() {
  dout(10) << dendl;

  Mutex::Locker locker(m_lock);
  if (!is_leader(m_lock)) {
    return;
  }

  release_leader_lock();
}

template <typename I>
void LeaderWatcher<I>::list_instances(std::vector<std::string> *instance_ids) {
  dout(10) << dendl;

  Mutex::Locker locker(m_lock);

  instance_ids->clear();
  if (m_instances != nullptr) {
    m_instances->list(instance_ids);
  }
}

template <typename I>
void LeaderWatcher<I>::cancel_timer_task() {
  ceph_assert(m_threads->timer_lock.is_locked());
  ceph_assert(m_lock.is_locked());

  if (m_timer_task == nullptr) {
    return;
  }

  dout(10) << m_timer_task << dendl;
  bool canceled = m_threads->timer->cancel_event(m_timer_task);
  ceph_assert(canceled);
  m_timer_task = nullptr;
}

template <typename I>
void LeaderWatcher<I>::schedule_timer_task(const std::string &name,
                                           int delay_factor, bool leader,
                                           TimerCallback timer_callback,
                                           bool shutting_down) {
  ceph_assert(m_threads->timer_lock.is_locked());
  ceph_assert(m_lock.is_locked());

  if (!shutting_down && m_on_shut_down_finish != nullptr) {
    return;
  }

  cancel_timer_task();

  m_timer_task = new FunctionContext(
    [this, leader, timer_callback](int r) {
      ceph_assert(m_threads->timer_lock.is_locked());
      m_timer_task = nullptr;

      if (m_timer_op_tracker.empty()) {
        Mutex::Locker locker(m_lock);
        execute_timer_task(leader, timer_callback);
        return;
      }

      // old timer task is still running -- do not start next
      // task until the previous task completes
      if (m_timer_gate == nullptr) {
        m_timer_gate = new C_TimerGate(this);
        m_timer_op_tracker.wait_for_ops(m_timer_gate);
      }
      m_timer_gate->leader = leader;
      m_timer_gate->timer_callback = timer_callback;
    });

  int after = delay_factor * m_cct->_conf.get_val<uint64_t>(
    "rbd_mirror_leader_heartbeat_interval");

  dout(10) << "scheduling " << name << " after " << after << " sec (task "
           << m_timer_task << ")" << dendl;
  m_threads->timer->add_event_after(after, m_timer_task);
}

template <typename I>
void LeaderWatcher<I>::execute_timer_task(bool leader,
                                          TimerCallback timer_callback) {
  dout(10) << dendl;

  ceph_assert(m_threads->timer_lock.is_locked());
  ceph_assert(m_lock.is_locked());
  ceph_assert(m_timer_op_tracker.empty());

  if (is_leader(m_lock) != leader) {
    return;
  }

  m_timer_op_tracker.start_op();
  (this->*timer_callback)();
}

template <typename I>
void LeaderWatcher<I>::handle_post_acquire_leader_lock(int r,
                                                       Context *on_finish) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    if (r == -EAGAIN) {
      dout(10) << "already locked" << dendl;
    } else {
      derr << "error acquiring leader lock: " << cpp_strerror(r) << dendl;
    }
    on_finish->complete(r);
    return;
  }

  Mutex::Locker locker(m_lock);
  ceph_assert(m_on_finish == nullptr);
  m_on_finish = on_finish;
  m_ret_val = 0;

  init_instances();
}

template <typename I>
void LeaderWatcher<I>::handle_pre_release_leader_lock(Context *on_finish) {
  dout(10) << dendl;

  Mutex::Locker locker(m_lock);
  ceph_assert(m_on_finish == nullptr);
  m_on_finish = on_finish;
  m_ret_val = 0;

  notify_listener();
}

template <typename I>
void LeaderWatcher<I>::handle_post_release_leader_lock(int r,
                                                       Context *on_finish) {
  dout(10) << "r=" << r << dendl;

  if (r < 0) {
    on_finish->complete(r);
    return;
  }

  Mutex::Locker locker(m_lock);
  ceph_assert(m_on_finish == nullptr);
  m_on_finish = on_finish;

  notify_lock_released();
}

template <typename I>
void LeaderWatcher<I>::break_leader_lock() {
  dout(10) << dendl;

  ceph_assert(m_threads->timer_lock.is_locked());
  ceph_assert(m_lock.is_locked());
  ceph_assert(!m_timer_op_tracker.empty());

  if (m_locker.cookie.empty()) {
    get_locker();
    return;
  }

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
      LeaderWatcher<I>, &LeaderWatcher<I>::handle_break_leader_lock>(this));

  m_leader_lock->break_lock(m_locker, true, ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_break_leader_lock(int r) {
  dout(10) << "r=" << r << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);
  ceph_assert(!m_timer_op_tracker.empty());

  if (m_leader_lock->is_shutdown()) {
    dout(10) << "canceling due to shutdown" << dendl;
    m_timer_op_tracker.finish_op();
    return;
  }

  if (r < 0 && r != -ENOENT) {
    derr << "error breaking leader lock: " << cpp_strerror(r)  << dendl;
    schedule_acquire_leader_lock(1);
    m_timer_op_tracker.finish_op();
    return;
  }

  m_locker = {};
  m_acquire_attempts = 0;
  acquire_leader_lock();
}

template <typename I>
void LeaderWatcher<I>::schedule_get_locker(bool reset_leader,
                                           uint32_t delay_factor) {
  dout(10) << dendl;

  ceph_assert(m_threads->timer_lock.is_locked());
  ceph_assert(m_lock.is_locked());

  if (reset_leader) {
    m_locker = {};
    m_acquire_attempts = 0;
  }

  schedule_timer_task("get locker", delay_factor, false,
                      &LeaderWatcher<I>::get_locker, false);
}

template <typename I>
void LeaderWatcher<I>::get_locker() {
  dout(10) << dendl;

  ceph_assert(m_threads->timer_lock.is_locked());
  ceph_assert(m_lock.is_locked());
  ceph_assert(!m_timer_op_tracker.empty());

  C_GetLocker *get_locker_ctx = new C_GetLocker(this);
  Context *ctx = create_async_context_callback(m_work_queue, get_locker_ctx);

  m_leader_lock->get_locker(&get_locker_ctx->locker, ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_get_locker(int r,
                                         librbd::managed_lock::Locker& locker) {
  dout(10) << "r=" << r << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker mutex_locker(m_lock);
  ceph_assert(!m_timer_op_tracker.empty());

  if (m_leader_lock->is_shutdown()) {
    dout(10) << "canceling due to shutdown" << dendl;
    m_timer_op_tracker.finish_op();
    return;
  }

  if (is_leader(m_lock)) {
    m_locker = {};
    m_timer_op_tracker.finish_op();
    return;
  }

  if (r == -ENOENT) {
    m_locker = {};
    m_acquire_attempts = 0;
    acquire_leader_lock();
    return;
  } else if (r < 0) {
    derr << "error retrieving leader locker: " << cpp_strerror(r) << dendl;
    schedule_get_locker(true, 1);
    m_timer_op_tracker.finish_op();
    return;
  }

  bool notify_listener = false;
  if (m_locker != locker) {
    m_locker = locker;
    notify_listener = true;
    if (m_acquire_attempts > 1) {
      dout(10) << "new lock owner detected -- resetting heartbeat counter"
               << dendl;
      m_acquire_attempts = 0;
    }
  }

  if (m_acquire_attempts >= m_cct->_conf.get_val<uint64_t>(
        "rbd_mirror_leader_max_acquire_attempts_before_break")) {
    dout(0) << "breaking leader lock after " << m_acquire_attempts << " "
            << "failed attempts to acquire" << dendl;
    break_leader_lock();
    return;
  }

  schedule_acquire_leader_lock(1);

  if (!notify_listener) {
    m_timer_op_tracker.finish_op();
    return;
  }

  auto ctx = new FunctionContext(
    [this](int r) {
      std::string instance_id;
      if (get_leader_instance_id(&instance_id)) {
        m_listener->update_leader_handler(instance_id);
      }
      Mutex::Locker timer_locker(m_threads->timer_lock);
      Mutex::Locker locker(m_lock);
      m_timer_op_tracker.finish_op();
    });
  m_work_queue->queue(ctx, 0);
}

template <typename I>
void LeaderWatcher<I>::schedule_acquire_leader_lock(uint32_t delay_factor) {
  dout(10) << dendl;

  ceph_assert(m_threads->timer_lock.is_locked());
  ceph_assert(m_lock.is_locked());

  schedule_timer_task("acquire leader lock",
                      delay_factor *
                        m_cct->_conf.get_val<uint64_t>("rbd_mirror_leader_max_missed_heartbeats"),
                      false, &LeaderWatcher<I>::acquire_leader_lock, false);
}

template <typename I>
void LeaderWatcher<I>::acquire_leader_lock() {
  ceph_assert(m_threads->timer_lock.is_locked());
  ceph_assert(m_lock.is_locked());
  ceph_assert(!m_timer_op_tracker.empty());

  ++m_acquire_attempts;
  dout(10) << "acquire_attempts=" << m_acquire_attempts << dendl;

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
      LeaderWatcher<I>, &LeaderWatcher<I>::handle_acquire_leader_lock>(this));
  m_leader_lock->try_acquire_lock(ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_acquire_leader_lock(int r) {
  dout(10) << "r=" << r << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);
  ceph_assert(!m_timer_op_tracker.empty());

  if (m_leader_lock->is_shutdown()) {
    dout(10) << "canceling due to shutdown" << dendl;
    m_timer_op_tracker.finish_op();
    return;
  }

  if (r < 0) {
    if (r == -EAGAIN) {
      dout(10) << "already locked" << dendl;
    } else {
      derr << "error acquiring lock: " << cpp_strerror(r) << dendl;
    }

    get_locker();
    return;
  }

  m_locker = {};
  m_acquire_attempts = 0;

  if (m_ret_val) {
    dout(5) << "releasing due to error on notify" << dendl;
    release_leader_lock();
    m_timer_op_tracker.finish_op();
    return;
  }

  notify_heartbeat();
}

template <typename I>
void LeaderWatcher<I>::release_leader_lock() {
  dout(10) << dendl;

  ceph_assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
      LeaderWatcher<I>, &LeaderWatcher<I>::handle_release_leader_lock>(this));

  m_leader_lock->release_lock(ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_release_leader_lock(int r) {
  dout(10) << "r=" << r << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);

  if (r < 0) {
    derr << "error releasing lock: " << cpp_strerror(r) << dendl;
    return;
  }

  schedule_acquire_leader_lock(1);
}

template <typename I>
void LeaderWatcher<I>::init_status_watcher() {
  dout(10) << dendl;

  ceph_assert(m_lock.is_locked());
  ceph_assert(m_status_watcher == nullptr);

  m_status_watcher = MirrorStatusWatcher<I>::create(m_ioctx, m_work_queue);

  Context *ctx = create_context_callback<
    LeaderWatcher<I>, &LeaderWatcher<I>::handle_init_status_watcher>(this);

  m_status_watcher->init(ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_init_status_watcher(int r) {
  dout(10) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    Mutex::Locker timer_locker(m_threads->timer_lock);
    Mutex::Locker locker(m_lock);

    if (r < 0) {
      derr << "error initializing mirror status watcher: " << cpp_strerror(r)
           << cpp_strerror(r) << dendl;
    } else {
      schedule_acquire_leader_lock(0);
    }

    ceph_assert(m_on_finish != nullptr);
    std::swap(on_finish, m_on_finish);
  }

  on_finish->complete(r);
}

template <typename I>
void LeaderWatcher<I>::shut_down_status_watcher() {
  dout(10) << dendl;

  ceph_assert(m_lock.is_locked());
  ceph_assert(m_status_watcher != nullptr);

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<LeaderWatcher<I>,
      &LeaderWatcher<I>::handle_shut_down_status_watcher>(this));

  m_status_watcher->shut_down(ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_shut_down_status_watcher(int r) {
  dout(10) << "r=" << r << dendl;

  Mutex::Locker locker(m_lock);
  m_status_watcher->destroy();
  m_status_watcher = nullptr;

  if (r < 0) {
    derr << "error shutting mirror status watcher down: " << cpp_strerror(r)
         << dendl;
  }

  unregister_watch();
}

template <typename I>
void LeaderWatcher<I>::init_instances() {
  dout(10) << dendl;

  ceph_assert(m_lock.is_locked());
  ceph_assert(m_instances == nullptr);

  m_instances = Instances<I>::create(m_threads, m_ioctx, m_instance_id,
                                     m_instances_listener);

  Context *ctx = create_context_callback<
    LeaderWatcher<I>, &LeaderWatcher<I>::handle_init_instances>(this);

  m_instances->init(ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_init_instances(int r) {
  dout(10) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  if (r < 0) {
    Mutex::Locker locker(m_lock);
    derr << "error initializing instances: " << cpp_strerror(r) << dendl;
    m_instances->destroy();
    m_instances = nullptr;

    ceph_assert(m_on_finish != nullptr);
    std::swap(m_on_finish, on_finish);
  } else {
    Mutex::Locker locker(m_lock);
    notify_listener();
    return;
  }

  on_finish->complete(r);
}

template <typename I>
void LeaderWatcher<I>::shut_down_instances() {
  dout(10) << dendl;

  ceph_assert(m_lock.is_locked());
  ceph_assert(m_instances != nullptr);

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<LeaderWatcher<I>,
      &LeaderWatcher<I>::handle_shut_down_instances>(this));

  m_instances->shut_down(ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_shut_down_instances(int r) {
  dout(10) << "r=" << r << dendl;
  ceph_assert(r == 0);

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);

    m_instances->destroy();
    m_instances = nullptr;

    ceph_assert(m_on_finish != nullptr);
    std::swap(m_on_finish, on_finish);
  }
  on_finish->complete(r);
}

template <typename I>
void LeaderWatcher<I>::notify_listener() {
  dout(10) << dendl;

  ceph_assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_work_queue, create_context_callback<
      LeaderWatcher<I>, &LeaderWatcher<I>::handle_notify_listener>(this));

  if (is_leader(m_lock)) {
    ctx = new FunctionContext(
      [this, ctx](int r) {
        m_listener->post_acquire_handler(ctx);
      });
  } else {
    ctx = new FunctionContext(
      [this, ctx](int r) {
        m_listener->pre_release_handler(ctx);
      });
  }
  m_work_queue->queue(ctx, 0);
}

template <typename I>
void LeaderWatcher<I>::handle_notify_listener(int r) {
  dout(10) << "r=" << r << dendl;

  Mutex::Locker locker(m_lock);

  if (r < 0) {
    derr << "error notifying listener: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
  }

  if (is_leader(m_lock)) {
    notify_lock_acquired();
  } else {
    shut_down_instances();
  }
}

template <typename I>
void LeaderWatcher<I>::notify_lock_acquired() {
  dout(10) << dendl;

  ceph_assert(m_lock.is_locked());

  Context *ctx = create_context_callback<
    LeaderWatcher<I>, &LeaderWatcher<I>::handle_notify_lock_acquired>(this);

  bufferlist bl;
  encode(NotifyMessage{LockAcquiredPayload{}}, bl);

  send_notify(bl, nullptr, ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_notify_lock_acquired(int r) {
  dout(10) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);
    if (r < 0 && r != -ETIMEDOUT) {
      derr << "error notifying leader lock acquired: " << cpp_strerror(r)
           << dendl;
      m_ret_val = r;
    }

    ceph_assert(m_on_finish != nullptr);
    std::swap(m_on_finish, on_finish);

    // listener should be ready for instance add/remove events now
    m_instances->unblock_listener();
  }
  on_finish->complete(0);
}

template <typename I>
void LeaderWatcher<I>::notify_lock_released() {
  dout(10) << dendl;

  ceph_assert(m_lock.is_locked());

  Context *ctx = create_context_callback<
    LeaderWatcher<I>, &LeaderWatcher<I>::handle_notify_lock_released>(this);

  bufferlist bl;
  encode(NotifyMessage{LockReleasedPayload{}}, bl);

  send_notify(bl, nullptr, ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_notify_lock_released(int r) {
  dout(10) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);
    if (r < 0 && r != -ETIMEDOUT) {
      derr << "error notifying leader lock released: " << cpp_strerror(r)
           << dendl;
    }

    ceph_assert(m_on_finish != nullptr);
    std::swap(m_on_finish, on_finish);
  }
  on_finish->complete(r);
}

template <typename I>
void LeaderWatcher<I>::notify_heartbeat() {
  dout(10) << dendl;

  ceph_assert(m_threads->timer_lock.is_locked());
  ceph_assert(m_lock.is_locked());
  ceph_assert(!m_timer_op_tracker.empty());

  if (!is_leader(m_lock)) {
    dout(5) << "not leader, canceling" << dendl;
    m_timer_op_tracker.finish_op();
    return;
  }

  Context *ctx = create_context_callback<
    LeaderWatcher<I>, &LeaderWatcher<I>::handle_notify_heartbeat>(this);

  bufferlist bl;
  encode(NotifyMessage{HeartbeatPayload{}}, bl);

  m_heartbeat_response.acks.clear();
  send_notify(bl, &m_heartbeat_response, ctx);
}

template <typename I>
void LeaderWatcher<I>::handle_notify_heartbeat(int r) {
  dout(10) << "r=" << r << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);
  ceph_assert(!m_timer_op_tracker.empty());

  m_timer_op_tracker.finish_op();
  if (m_leader_lock->is_shutdown()) {
    dout(10) << "canceling due to shutdown" << dendl;
    return;
  } else if (!is_leader(m_lock)) {
    return;
  }

  if (r < 0 && r != -ETIMEDOUT) {
    derr << "error notifying heartbeat: " << cpp_strerror(r)
         <<  ", releasing leader" << dendl;
    release_leader_lock();
    return;
  }

  dout(10) << m_heartbeat_response.acks.size() << " acks received, "
           << m_heartbeat_response.timeouts.size() << " timed out" << dendl;

  std::vector<std::string> instance_ids;
  for (auto &it: m_heartbeat_response.acks) {
    uint64_t notifier_id = it.first.gid;
    instance_ids.push_back(stringify(notifier_id));
  }
  if (!instance_ids.empty()) {
    m_instances->acked(instance_ids);
  }

  schedule_timer_task("heartbeat", 1, true,
                      &LeaderWatcher<I>::notify_heartbeat, false);
}

template <typename I>
void LeaderWatcher<I>::handle_heartbeat(Context *on_notify_ack) {
  dout(10) << dendl;

  {
    Mutex::Locker timer_locker(m_threads->timer_lock);
    Mutex::Locker locker(m_lock);
    if (is_leader(m_lock)) {
      dout(5) << "got another leader heartbeat, ignoring" << dendl;
    } else {
      cancel_timer_task();
      m_acquire_attempts = 0;
      schedule_acquire_leader_lock(1);
    }
  }

  on_notify_ack->complete(0);
}

template <typename I>
void LeaderWatcher<I>::handle_lock_acquired(Context *on_notify_ack) {
  dout(10) << dendl;

  {
    Mutex::Locker timer_locker(m_threads->timer_lock);
    Mutex::Locker locker(m_lock);
    if (is_leader(m_lock)) {
      dout(5) << "got another leader lock_acquired, ignoring" << dendl;
    } else {
      cancel_timer_task();
      schedule_get_locker(true, 0);
    }
  }

  on_notify_ack->complete(0);
}

template <typename I>
void LeaderWatcher<I>::handle_lock_released(Context *on_notify_ack) {
  dout(10) << dendl;

  {
    Mutex::Locker timer_locker(m_threads->timer_lock);
    Mutex::Locker locker(m_lock);
    if (is_leader(m_lock)) {
      dout(5) << "got another leader lock_released, ignoring" << dendl;
    } else {
      cancel_timer_task();
      schedule_get_locker(true, 0);
    }
  }

  on_notify_ack->complete(0);
}

template <typename I>
void LeaderWatcher<I>::handle_notify(uint64_t notify_id, uint64_t handle,
                                     uint64_t notifier_id, bufferlist &bl) {
  dout(10) << "notify_id=" << notify_id << ", handle=" << handle << ", "
           << "notifier_id=" << notifier_id << dendl;

  Context *ctx = new C_NotifyAck(this, notify_id, handle);

  if (notifier_id == m_notifier_id) {
    dout(10) << "our own notification, ignoring" << dendl;
    ctx->complete(0);
    return;
  }

  NotifyMessage notify_message;
  try {
    auto iter = bl.cbegin();
    decode(notify_message, iter);
  } catch (const buffer::error &err) {
    derr << ": error decoding image notification: " << err.what() << dendl;
    ctx->complete(0);
    return;
  }

  apply_visitor(HandlePayloadVisitor(this, ctx), notify_message.payload);
}

template <typename I>
void LeaderWatcher<I>::handle_rewatch_complete(int r) {
  dout(5) << "r=" << r << dendl;

  m_leader_lock->reacquire_lock(nullptr);
}

template <typename I>
void LeaderWatcher<I>::handle_payload(const HeartbeatPayload &payload,
                                      Context *on_notify_ack) {
  dout(10) << "heartbeat" << dendl;

  handle_heartbeat(on_notify_ack);
}

template <typename I>
void LeaderWatcher<I>::handle_payload(const LockAcquiredPayload &payload,
                                      Context *on_notify_ack) {
  dout(10) << "lock_acquired" << dendl;

  handle_lock_acquired(on_notify_ack);
}

template <typename I>
void LeaderWatcher<I>::handle_payload(const LockReleasedPayload &payload,
                                      Context *on_notify_ack) {
  dout(10) << "lock_released" << dendl;

  handle_lock_released(on_notify_ack);
}

template <typename I>
void LeaderWatcher<I>::handle_payload(const UnknownPayload &payload,
                                      Context *on_notify_ack) {
  dout(10) << "unknown" << dendl;

  on_notify_ack->complete(0);
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::LeaderWatcher<librbd::ImageCtx>;
