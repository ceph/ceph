// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "include/stringify.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"
#include "common/debug.h"
#include "common/errno.h"
#include "librbd/Utils.h"
#include "InstanceWatcher.h"
#include "Instances.h"
#include "Threads.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::Instances: " \
                           << this << " " << __func__ << ": "

namespace rbd {
namespace mirror {

using librbd::util::create_async_context_callback;
using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
Instances<I>::Instances(Threads<I> *threads, librados::IoCtx &ioctx,
                        const std::string& instance_id,
                        instances::Listener& listener) :
  m_threads(threads), m_ioctx(ioctx), m_instance_id(instance_id),
  m_listener(listener), m_cct(reinterpret_cast<CephContext *>(ioctx.cct())),
  m_lock("rbd::mirror::Instances " + ioctx.get_pool_name()) {
}

template <typename I>
Instances<I>::~Instances() {
}

template <typename I>
void Instances<I>::init(Context *on_finish) {
  dout(10) << dendl;

  Mutex::Locker locker(m_lock);
  ceph_assert(m_on_finish == nullptr);
  m_on_finish = on_finish;
  get_instances();
}

template <typename I>
void Instances<I>::shut_down(Context *on_finish) {
  dout(10) << dendl;

  Mutex::Locker locker(m_lock);
  ceph_assert(m_on_finish == nullptr);
  m_on_finish = on_finish;

  Context *ctx = new FunctionContext(
    [this](int r) {
      Mutex::Locker timer_locker(m_threads->timer_lock);
      Mutex::Locker locker(m_lock);
      cancel_remove_task();
      wait_for_ops();
    });

  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void Instances<I>::unblock_listener() {
  dout(5) << dendl;

  Mutex::Locker locker(m_lock);
  ceph_assert(m_listener_blocked);
  m_listener_blocked = false;

  InstanceIds added_instance_ids;
  for (auto& pair : m_instances) {
    if (pair.second.state == INSTANCE_STATE_ADDING) {
      added_instance_ids.push_back(pair.first);
    }
  }

  if (!added_instance_ids.empty()) {
    m_threads->work_queue->queue(
      new C_NotifyInstancesAdded(this, added_instance_ids), 0);
  }
}

template <typename I>
void Instances<I>::acked(const InstanceIds& instance_ids) {
  dout(10) << "instance_ids=" << instance_ids << dendl;

  Mutex::Locker locker(m_lock);
  if (m_on_finish != nullptr) {
    dout(5) << "received on shut down, ignoring" << dendl;
    return;
  }

  Context *ctx = new C_HandleAcked(this, instance_ids);
  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void Instances<I>::handle_acked(const InstanceIds& instance_ids) {
  dout(5) << "instance_ids=" << instance_ids << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);
  if (m_on_finish != nullptr) {
    dout(5) << "handled on shut down, ignoring" << dendl;
    return;
  }

  InstanceIds added_instance_ids;
  auto time = ceph_clock_now();
  for (auto& instance_id : instance_ids) {
    auto &instance = m_instances.insert(
      std::make_pair(instance_id, Instance{})).first->second;
    instance.acked_time = time;
    if (instance.state == INSTANCE_STATE_ADDING) {
      added_instance_ids.push_back(instance_id);
    }
  }

  schedule_remove_task(time);
  if (!m_listener_blocked && !added_instance_ids.empty()) {
    m_threads->work_queue->queue(
      new C_NotifyInstancesAdded(this, added_instance_ids), 0);
  }
}

template <typename I>
void Instances<I>::notify_instances_added(const InstanceIds& instance_ids) {
  Mutex::Locker locker(m_lock);
  InstanceIds added_instance_ids;
  for (auto& instance_id : instance_ids) {
    auto it = m_instances.find(instance_id);
    if (it != m_instances.end() && it->second.state == INSTANCE_STATE_ADDING) {
      added_instance_ids.push_back(instance_id);
    }
  }

  if (added_instance_ids.empty()) {
    return;
  }

  dout(5) << "instance_ids=" << added_instance_ids << dendl;
  m_lock.Unlock();
  m_listener.handle_added(added_instance_ids);
  m_lock.Lock();

  for (auto& instance_id : added_instance_ids) {
    auto it = m_instances.find(instance_id);
    if (it != m_instances.end() && it->second.state == INSTANCE_STATE_ADDING) {
      it->second.state = INSTANCE_STATE_IDLE;
    }
  }
}

template <typename I>
void Instances<I>::notify_instances_removed(const InstanceIds& instance_ids) {
  dout(5) << "instance_ids=" << instance_ids << dendl;
  m_listener.handle_removed(instance_ids);

  Mutex::Locker locker(m_lock);
  for (auto& instance_id : instance_ids) {
    m_instances.erase(instance_id);
  }
}

template <typename I>
void Instances<I>::list(std::vector<std::string> *instance_ids) {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);

  for (auto it : m_instances) {
    instance_ids->push_back(it.first);
  }
}


template <typename I>
void Instances<I>::get_instances() {
  dout(10) << dendl;

  ceph_assert(m_lock.is_locked());

  Context *ctx = create_context_callback<
    Instances, &Instances<I>::handle_get_instances>(this);

  InstanceWatcher<I>::get_instances(m_ioctx, &m_instance_ids, ctx);
}

template <typename I>
void Instances<I>::handle_get_instances(int r) {
  dout(10) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);
    std::swap(on_finish, m_on_finish);
  }

  if (r < 0) {
    derr << "error retrieving instances: " << cpp_strerror(r) << dendl;
  } else {
    handle_acked(m_instance_ids);
  }
  on_finish->complete(r);
}

template <typename I>
void Instances<I>::wait_for_ops() {
  dout(10) << dendl;

  ceph_assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_threads->work_queue, create_context_callback<
    Instances, &Instances<I>::handle_wait_for_ops>(this));

  m_async_op_tracker.wait_for_ops(ctx);
}

template <typename I>
void Instances<I>::handle_wait_for_ops(int r) {
  dout(10) << "r=" << r << dendl;

  ceph_assert(r == 0);

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);
    std::swap(on_finish, m_on_finish);
  }
  on_finish->complete(r);
}

template <typename I>
void Instances<I>::remove_instances(const utime_t& time) {
  ceph_assert(m_lock.is_locked());

  InstanceIds instance_ids;
  for (auto& instance_pair : m_instances) {
    if (instance_pair.first == m_instance_id) {
      continue;
    }
    auto& instance = instance_pair.second;
    if (instance.state != INSTANCE_STATE_REMOVING &&
        instance.acked_time <= time) {
      instance.state = INSTANCE_STATE_REMOVING;
      instance_ids.push_back(instance_pair.first);
    }
  }
  ceph_assert(!instance_ids.empty());

  dout(10) << "instance_ids=" << instance_ids << dendl;
  Context* ctx = new FunctionContext([this, instance_ids](int r) {
      handle_remove_instances(r, instance_ids);
    });
  ctx = create_async_context_callback(m_threads->work_queue, ctx);

  auto gather_ctx = new C_Gather(m_cct, ctx);
  for (auto& instance_id : instance_ids) {
    InstanceWatcher<I>::remove_instance(m_ioctx, m_threads->work_queue,
                                        instance_id, gather_ctx->new_sub());
  }

  m_async_op_tracker.start_op();
  gather_ctx->activate();
}

template <typename I>
void Instances<I>::handle_remove_instances(
    int r, const InstanceIds& instance_ids) {
  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);

  dout(10) << "r=" << r << ", instance_ids=" << instance_ids << dendl;
  ceph_assert(r == 0);

  // fire removed notification now that instances have been blacklisted
  m_threads->work_queue->queue(
    new C_NotifyInstancesRemoved(this, instance_ids), 0);

  // reschedule the timer for the next batch
  schedule_remove_task(ceph_clock_now());
  m_async_op_tracker.finish_op();
}

template <typename I>
void Instances<I>::cancel_remove_task() {
  ceph_assert(m_threads->timer_lock.is_locked());
  ceph_assert(m_lock.is_locked());

  if (m_timer_task == nullptr) {
    return;
  }

  dout(10) << dendl;

  bool canceled = m_threads->timer->cancel_event(m_timer_task);
  ceph_assert(canceled);
  m_timer_task = nullptr;
}

template <typename I>
void Instances<I>::schedule_remove_task(const utime_t& time) {
  cancel_remove_task();
  if (m_on_finish != nullptr) {
    dout(10) << "received on shut down, ignoring" << dendl;
    return;
  }

  int after = m_cct->_conf.get_val<uint64_t>("rbd_mirror_leader_heartbeat_interval") *
    (1 + m_cct->_conf.get_val<uint64_t>("rbd_mirror_leader_max_missed_heartbeats") +
     m_cct->_conf.get_val<uint64_t>("rbd_mirror_leader_max_acquire_attempts_before_break"));

  bool schedule = false;
  utime_t oldest_time = time;
  for (auto& instance : m_instances) {
    if (instance.first == m_instance_id) {
      continue;
    }
    if (instance.second.state == INSTANCE_STATE_REMOVING) {
      // removal is already in-flight
      continue;
    }

    oldest_time = std::min(oldest_time, instance.second.acked_time);
    schedule = true;
  }

  if (!schedule) {
    return;
  }

  dout(10) << dendl;

  // schedule a time to fire when the oldest instance should be removed
  m_timer_task = new FunctionContext(
    [this, oldest_time](int r) {
      ceph_assert(m_threads->timer_lock.is_locked());
      Mutex::Locker locker(m_lock);
      m_timer_task = nullptr;

      remove_instances(oldest_time);
    });

  oldest_time += after;
  m_threads->timer->add_event_at(oldest_time, m_timer_task);
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::Instances<librbd::ImageCtx>;
