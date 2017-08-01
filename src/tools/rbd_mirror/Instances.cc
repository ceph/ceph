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
Instances<I>::Instances(Threads<I> *threads, librados::IoCtx &ioctx) :
  m_threads(threads), m_ioctx(ioctx),
  m_cct(reinterpret_cast<CephContext *>(ioctx.cct())),
  m_lock("rbd::mirror::Instances " + ioctx.get_pool_name()) {
}

template <typename I>
Instances<I>::~Instances() {
}

template <typename I>
void Instances<I>::init(Context *on_finish) {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);
  assert(m_on_finish == nullptr);
  m_on_finish = on_finish;
  get_instances();
}

template <typename I>
void Instances<I>::shut_down(Context *on_finish) {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);
  assert(m_on_finish == nullptr);
  m_on_finish = on_finish;

  Context *ctx = new FunctionContext(
    [this](int r) {
      Mutex::Locker timer_locker(m_threads->timer_lock);
      Mutex::Locker locker(m_lock);

      for (auto it : m_instances) {
        cancel_remove_task(it.second);
      }
      wait_for_ops();
    });

  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void Instances<I>::notify(const std::string &instance_id) {
  dout(20) << instance_id << dendl;

  Mutex::Locker locker(m_lock);

  if (m_on_finish != nullptr) {
    dout(20) << "received on shut down, ignoring" << dendl;
    return;
  }

  Context *ctx = new C_Notify(this, instance_id);

  m_threads->work_queue->queue(ctx, 0);
}

template <typename I>
void Instances<I>::handle_notify(const std::string &instance_id) {
  dout(20) << instance_id << dendl;

  Mutex::Locker timer_locker(m_threads->timer_lock);
  Mutex::Locker locker(m_lock);

  if (m_on_finish != nullptr) {
    dout(20) << "handled on shut down, ignoring" << dendl;
    return;
  }

  auto &instance = m_instances.insert(
    std::make_pair(instance_id, Instance(instance_id))).first->second;

  schedule_remove_task(instance);
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
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_context_callback<
    Instances, &Instances<I>::handle_get_instances>(this);

  InstanceWatcher<I>::get_instances(m_ioctx, &m_instance_ids, ctx);
}

template <typename I>
void Instances<I>::handle_get_instances(int r) {
  dout(20) << "r=" << r << dendl;

  Context *on_finish = nullptr;
  {
    Mutex::Locker timer_locker(m_threads->timer_lock);
    Mutex::Locker locker(m_lock);

    if (r < 0) {
      derr << "error retrieving instances: " << cpp_strerror(r) << dendl;
    } else {
      auto my_instance_id = stringify(m_ioctx.get_instance_id());
      for (auto &instance_id : m_instance_ids) {
        if (instance_id == my_instance_id) {
          continue;
        }
        auto &instance = m_instances.insert(
          std::make_pair(instance_id, Instance(instance_id))).first->second;
        schedule_remove_task(instance);
      }
    }
    std::swap(on_finish, m_on_finish);
  }
  on_finish->complete(r);
}

template <typename I>
void Instances<I>::wait_for_ops() {
  dout(20) << dendl;

  assert(m_lock.is_locked());

  Context *ctx = create_async_context_callback(
    m_threads->work_queue, create_context_callback<
    Instances, &Instances<I>::handle_wait_for_ops>(this));

  m_async_op_tracker.wait_for_ops(ctx);
}

template <typename I>
void Instances<I>::handle_wait_for_ops(int r) {
  dout(20) << "r=" << r << dendl;

  assert(r == 0);

  Context *on_finish = nullptr;
  {
    Mutex::Locker locker(m_lock);
    std::swap(on_finish, m_on_finish);
  }
  on_finish->complete(r);
}

template <typename I>
void Instances<I>::remove_instance(Instance &instance) {
  assert(m_lock.is_locked());

  dout(20) << instance.id << dendl;

  Context *ctx = create_async_context_callback(
    m_threads->work_queue, create_context_callback<
    Instances, &Instances<I>::handle_remove_instance>(this));

  m_async_op_tracker.start_op();
  InstanceWatcher<I>::remove_instance(m_ioctx, m_threads->work_queue,
                                      instance.id, ctx);
  m_instances.erase(instance.id);
}

template <typename I>
void Instances<I>::handle_remove_instance(int r) {
  Mutex::Locker locker(m_lock);

  dout(20) << " r=" << r << dendl;

  assert(r == 0);

  m_async_op_tracker.finish_op();
}

template <typename I>
void Instances<I>::cancel_remove_task(Instance &instance) {
  assert(m_threads->timer_lock.is_locked());
  assert(m_lock.is_locked());

  if (instance.timer_task == nullptr) {
    return;
  }

  dout(20) << instance.timer_task << dendl;

  bool canceled = m_threads->timer->cancel_event(instance.timer_task);
  assert(canceled);
  instance.timer_task = nullptr;
}

template <typename I>
void Instances<I>::schedule_remove_task(Instance &instance) {
  dout(20) << dendl;

  cancel_remove_task(instance);

  int after = m_cct->_conf->get_val<int64_t>("rbd_mirror_leader_heartbeat_interval") *
    (1 + m_cct->_conf->get_val<int64_t>("rbd_mirror_leader_max_missed_heartbeats") +
     m_cct->_conf->get_val<int64_t>("rbd_mirror_leader_max_acquire_attempts_before_break"));

  instance.timer_task = new FunctionContext(
    [this, &instance](int r) {
      assert(m_threads->timer_lock.is_locked());
      Mutex::Locker locker(m_lock);
      instance.timer_task = nullptr;
      remove_instance(instance);
    });

  dout(20) << "scheduling instance " << instance.id << " remove after " << after
           << " sec (task " << instance.timer_task << ")" << dendl;

  m_threads->timer->add_event_after(after, instance.timer_task);
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::Instances<librbd::ImageCtx>;
