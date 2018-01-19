// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/WorkQueue.h"

#include "librbd/Utils.h"
#include "tools/rbd_mirror/Threads.h"

#include "ImageMap.h"
#include "image_map/LoadRequest.h"
#include "image_map/SimplePolicy.h"
#include "image_map/UpdateRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::ImageMap: " << this << " " << __func__

namespace rbd {
namespace mirror {

using image_map::Policy;

using librbd::util::unique_lock_name;
using librbd::util::create_async_context_callback;

template <typename I>
ImageMap<I>::ImageMap(librados::IoCtx &ioctx, Threads<I> *threads, image_map::Listener &listener)
  : m_ioctx(ioctx),
    m_threads(threads),
    m_listener(listener),
    m_lock(unique_lock_name("rbd::mirror::ImageMap::m_lock", this)) {
}

template <typename I>
ImageMap<I>::~ImageMap() {
  assert(m_async_op_tracker.empty());
  assert(m_timer_task == nullptr);
}

template<typename I>
bool ImageMap<I>::add_peer(const std::string &global_image_id, const std::string &peer_uuid) {
    assert(m_lock.is_locked());

    dout(20) << ": global_image_id=" << global_image_id << ", peer_uuid="
             << peer_uuid << dendl;

    auto ins = m_peer_map[global_image_id].insert(peer_uuid);
    return ins.second && m_peer_map[global_image_id].size() == 1;
}

template<typename I>
bool ImageMap<I>::remove_peer(const std::string &global_image_id, const std::string &peer_uuid) {
  assert(m_lock.is_locked());

  dout(20) << ": global_image_id=" << global_image_id << ", peer_uuid="
           << peer_uuid << dendl;

  auto rm = m_peer_map[global_image_id].erase(peer_uuid);
  return rm && m_peer_map[global_image_id].empty();
}

template<typename I>
void ImageMap<I>::queue_update_map(const std::string &global_image_id) {
  assert(m_lock.is_locked());

  dout(20) << ": global_image_id=" << global_image_id << dendl;

  Policy::LookupInfo info = m_policy->lookup(global_image_id);
  assert(info.instance_id != Policy::UNMAPPED_INSTANCE_ID);

  m_updates.emplace_back(global_image_id, info.instance_id, info.mapped_time);
}

template<typename I>
void ImageMap<I>::queue_remove_map(const std::string &global_image_id) {
  assert(m_lock.is_locked());

  dout(20) << ": global_image_id=" << global_image_id << dendl;
  m_remove_global_image_ids.emplace(global_image_id);
}

template <typename I>
void ImageMap<I>::queue_acquire_image(const std::string &global_image_id) {
  assert(m_lock.is_locked());

  dout(20) << ": global_image_id=" << global_image_id << dendl;

  Policy::LookupInfo info = m_policy->lookup(global_image_id);
  assert(info.instance_id != Policy::UNMAPPED_INSTANCE_ID);

  m_acquire_updates.emplace_back(global_image_id, info.instance_id);
}

template <typename I>
void ImageMap<I>::queue_release_image(const std::string &global_image_id) {
  assert(m_lock.is_locked());

  dout(20) << ": global_image_id=" << global_image_id << dendl;

  Policy::LookupInfo info = m_policy->lookup(global_image_id);
  assert(info.instance_id != Policy::UNMAPPED_INSTANCE_ID);

  m_release_updates.emplace_back(global_image_id, info.instance_id);
}

template <typename I>
void ImageMap<I>::continue_action(const std::set<std::string> &global_image_ids, int r) {
  dout(20) << dendl;

  {
    Mutex::Locker locker(m_lock);
    if (m_shutting_down) {
      return;
    }

    for (auto const &global_image_id : global_image_ids) {
      bool schedule = m_policy->finish_action(global_image_id, r);
      if (schedule) {
        schedule_action(global_image_id);
      }
    }
  }

  schedule_update_task();
}

template <typename I>
void ImageMap<I>::handle_update_request(const Updates &updates,
                                        const std::set<std::string> &remove_global_image_ids, int r) {
  dout(20) << ": r=" << r << dendl;

  std::set<std::string> global_image_ids;

  global_image_ids.insert(remove_global_image_ids.begin(), remove_global_image_ids.end());
  for (auto const &update : updates) {
    global_image_ids.insert(update.global_image_id);
  }

  continue_action(global_image_ids, r);
}

template <typename I>
void ImageMap<I>::update_image_mapping() {
  dout(20) << ": update_count=" << m_updates.size() << ", remove_count="
           << m_remove_global_image_ids.size() << dendl;

  if (m_updates.empty() && m_remove_global_image_ids.empty()) {
    return;
  }

  Updates updates(m_updates);
  std::set<std::string> remove_global_image_ids(m_remove_global_image_ids);

  Context *on_finish = new FunctionContext(
    [this, updates, remove_global_image_ids](int r) {
      handle_update_request(updates, remove_global_image_ids, r);
      finish_async_op();
    });
  on_finish = create_async_context_callback(m_threads->work_queue, on_finish);

  // empty meta policy for now..
  image_map::PolicyMetaNone policy_meta;

  bufferlist bl;
  encode(image_map::PolicyData(policy_meta), bl);

  // prepare update map
  std::map<std::string, cls::rbd::MirrorImageMap> update_mapping;
  for (auto const &update : updates) {
    update_mapping.emplace(
      update.global_image_id, cls::rbd::MirrorImageMap(update.instance_id, update.mapped_time, bl));
  }

  start_async_op();
  image_map::UpdateRequest<I> *req = image_map::UpdateRequest<I>::create(
    m_ioctx, std::move(update_mapping), std::move(remove_global_image_ids), on_finish);
  req->send();
}

template <typename I>
void ImageMap<I>::process_updates() {
  dout(20) << dendl;

  assert(m_threads->timer_lock.is_locked());
  assert(m_timer_task == nullptr);

  {
    Mutex::Locker locker(m_lock);

    // gather updates by advancing the state machine
    for (auto const &global_image_id : m_global_image_ids) {
      m_policy->start_next_action(global_image_id);
    }

    m_global_image_ids.clear();
  }

  // notify listener (acquire, release) and update on-disk map. note
  // that its safe to process this outside m_lock as we still hold
  // timer lock.
  notify_listener_acquire_release_images(m_acquire_updates, m_release_updates);
  update_image_mapping();

  m_updates.clear();
  m_remove_global_image_ids.clear();
  m_acquire_updates.clear();
  m_release_updates.clear();
}

template <typename I>
void ImageMap<I>::schedule_update_task() {
  dout(20) << dendl;

  Mutex::Locker timer_lock(m_threads->timer_lock);
  if (m_timer_task != nullptr) {
    return;
  }

  {
    Mutex::Locker locker(m_lock);
    if (m_global_image_ids.empty()) {
      return;
    }
  }

  m_timer_task = new FunctionContext([this](int r) {
      assert(m_threads->timer_lock.is_locked());
      m_timer_task = nullptr;

      process_updates();
    });

  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  double after = cct->_conf->get_val<double>("rbd_mirror_image_policy_update_throttle_interval");

  dout(20) << ": scheduling image check update (" << m_timer_task << ")"
           << " after " << after << " second(s)" << dendl;
  m_threads->timer->add_event_after(after, m_timer_task);
}

template <typename I>
void ImageMap<I>::schedule_action(const std::string &global_image_id) {
  dout(20) << ": global_image_id=" << global_image_id << dendl;
  assert(m_lock.is_locked());

  m_global_image_ids.emplace(global_image_id);
}

template <typename I>
void ImageMap<I>::notify_listener_acquire_release_images(const Updates &acquire,
                                                         const Updates &release) {
  dout(20) << ": acquire_count: " << acquire.size() << ", release_count="
           << release.size() << dendl;

  for (auto const &update : acquire) {
    m_listener.acquire_image(update.global_image_id, update.instance_id);
  }

  for (auto const &update : release) {
    m_listener.release_image(update.global_image_id, update.instance_id);
  }
}

template <typename I>
void ImageMap<I>::notify_listener_remove_images(const std::string &peer_uuid,
                                                const Updates &remove) {
  dout(20) << ": peer_uuid=" << peer_uuid << ", remove_count=" << remove.size()
           << dendl;

  for (auto const &update : remove) {
    m_listener.remove_image(peer_uuid, update.global_image_id, update.instance_id);
  }
}

template <typename I>
void ImageMap<I>::handle_load(const std::map<std::string, cls::rbd::MirrorImageMap> &image_mapping) {
  dout(20) << dendl;

  Mutex::Locker locker(m_lock);
  m_policy->init(image_mapping);
}

template <typename I>
void ImageMap<I>::handle_add_action(const std::string &global_image_id, int r) {
  assert(m_lock.is_locked());
  dout(20) << ": global_image_id=" << global_image_id << dendl;

  if (r < 0) {
    derr << ": failed to add global_image_id=" << global_image_id << dendl;
  }
}

template <typename I>
void ImageMap<I>::handle_remove_action(const std::string &global_image_id, int r) {
  assert(m_lock.is_locked());
  dout(20) << ": global_image_id=" << global_image_id << dendl;

  if (r < 0) {
    derr << ": failed to remove global_image_id=" << global_image_id << dendl;
  }

  if (m_peer_map[global_image_id].empty()) {
    m_peer_map.erase(global_image_id);
  }
}

template <typename I>
void ImageMap<I>::handle_shuffle_action(const std::string &global_image_id, int r) {
  assert(m_lock.is_locked());
  dout(20) << ": global_image_id=" << global_image_id << dendl;

  if (r < 0) {
    derr << ": failed to shuffle global_image_id=" << global_image_id << dendl;
  }
}

template <typename I>
void ImageMap<I>::schedule_add_action(const std::string &global_image_id) {
  dout(20) << ": global_image_id=" << global_image_id << dendl;

  // in order of state-machine execution, so its easier to follow
  Context *on_update = new C_UpdateMap(this, global_image_id);
  Context *on_acquire = new C_AcquireImage(this, global_image_id);
  Context *on_finish = new FunctionContext([this, global_image_id](int r) {
      handle_add_action(global_image_id, r);
    });

  if (m_policy->add_image(global_image_id, on_update, on_acquire, on_finish)) {
    schedule_action(global_image_id);
  }
}

template <typename I>
void ImageMap<I>::schedule_remove_action(const std::string &global_image_id) {
  dout(20) << ": global_image_id=" << global_image_id << dendl;

  // in order of state-machine execution, so its easier to follow
  Context *on_release = new FunctionContext([this, global_image_id](int r) {
      queue_release_image(global_image_id);
    });
  Context *on_remove = new C_RemoveMap(this, global_image_id);
  Context *on_finish = new FunctionContext([this, global_image_id](int r) {
      handle_remove_action(global_image_id, r);
    });

  if (m_policy->remove_image(global_image_id, on_release, on_remove, on_finish)) {
    schedule_action(global_image_id);
  }
}

template <typename I>
void ImageMap<I>::schedule_shuffle_action(const std::string &global_image_id) {
  assert(m_lock.is_locked());

  dout(20) << ": global_image_id=" << global_image_id << dendl;

  // in order of state-machine execution, so its easier to follow
  Context *on_release = new FunctionContext([this, global_image_id](int r) {
      queue_release_image(global_image_id);
    });
  Context *on_update = new C_UpdateMap(this, global_image_id);
  Context *on_acquire = new C_AcquireImage(this, global_image_id);
  Context *on_finish = new FunctionContext([this, global_image_id](int r) {
      handle_shuffle_action(global_image_id, r);
    });

  if (m_policy->shuffle_image(global_image_id, on_release, on_update, on_acquire, on_finish)) {
    schedule_action(global_image_id);
  }
}

template <typename I>
void ImageMap<I>::update_images_added(const std::string &peer_uuid,
                                      const std::set<std::string> &global_image_ids) {
  dout(20) << dendl;
  assert(m_lock.is_locked());

  for (auto const &global_image_id : global_image_ids) {
    bool schedule_update = add_peer(global_image_id, peer_uuid);
    if (schedule_update) {
      schedule_add_action(global_image_id);
    }
  }
}

template <typename I>
void ImageMap<I>::update_images_removed(const std::string &peer_uuid,
                                        const std::set<std::string> &global_image_ids) {
  dout(20) << dendl;
  assert(m_lock.is_locked());

  Updates to_remove;
  for (auto const &global_image_id : global_image_ids) {
    bool schedule_update = remove_peer(global_image_id, peer_uuid);
    if (schedule_update) {
      schedule_remove_action(global_image_id);
    }

    Policy::LookupInfo info = m_policy->lookup(global_image_id);
    if (info.instance_id != Policy::UNMAPPED_INSTANCE_ID) {
      to_remove.emplace_back(global_image_id, info.instance_id);
    }
  }

  // removal notification will be notified instantly. this is safe
  // even after scheduling action for images as we still hold m_lock
  if (!peer_uuid.empty()) {
    notify_listener_remove_images(peer_uuid, to_remove);
  }
}

template <typename I>
void ImageMap<I>::update_instances_added(const std::vector<std::string> &instance_ids) {
  dout(20) << dendl;

  {
    Mutex::Locker locker(m_lock);
    if (m_shutting_down) {
      return;
    }

    std::set<std::string> remap_global_image_ids;
    m_policy->add_instances(instance_ids, &remap_global_image_ids);

    for (auto const &global_image_id : remap_global_image_ids) {
      schedule_shuffle_action(global_image_id);
    }
  }

  schedule_update_task();
}

template <typename I>
void ImageMap<I>::update_instances_removed(const std::vector<std::string> &instance_ids) {
  dout(20) << dendl;

  {
    Mutex::Locker locker(m_lock);
    if (m_shutting_down) {
      return;
    }

    std::set<std::string> remap_global_image_ids;
    m_policy->remove_instances(instance_ids, &remap_global_image_ids);

    for (auto const &global_image_id : remap_global_image_ids) {
      schedule_shuffle_action(global_image_id);
    }
  }

  schedule_update_task();
}

template <typename I>
void ImageMap<I>::update_images(const std::string &peer_uuid,
                                std::set<std::string> &&added_global_image_ids,
                                std::set<std::string> &&removed_global_image_ids) {
  dout(20) << ": peer_uuid=" << peer_uuid << ", " << "added_count="
           << added_global_image_ids.size() << ", " << "removed_count="
           << removed_global_image_ids.size() << dendl;

  {
    Mutex::Locker locker(m_lock);
    if (m_shutting_down) {
      return;
    }

    update_images_removed(peer_uuid, removed_global_image_ids);
    update_images_added(peer_uuid, added_global_image_ids);
  }

  schedule_update_task();
}

template <typename I>
void ImageMap<I>::handle_peer_ack(const std::string &global_image_id, int r) {
  dout (20) << ": global_image_id=" << global_image_id << ", r=" << r
            << dendl;

  continue_action({global_image_id}, r);
}

template <typename I>
void ImageMap<I>::init(Context *on_finish) {
  dout(20) << dendl;

  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  std::string policy_type = cct->_conf->get_val<string>("rbd_mirror_image_policy_type");

  if (policy_type == "simple") {
    m_policy.reset(image_map::SimplePolicy::create(m_ioctx));
  } else {
    assert(false); // not really needed as such, but catch it.
  }

  dout(20) << ": mapping policy=" << policy_type << dendl;

  start_async_op();
  C_LoadMap *ctx = new C_LoadMap(this, on_finish);
  image_map::LoadRequest<I> *req = image_map::LoadRequest<I>::create(
    m_ioctx, &ctx->image_mapping, ctx);
  req->send();
}

template <typename I>
void ImageMap<I>::shut_down(Context *on_finish) {
  dout(20) << dendl;

  {
    Mutex::Locker timer_lock(m_threads->timer_lock);

    {
      Mutex::Locker locker(m_lock);
      assert(!m_shutting_down);

      m_shutting_down = true;
      m_policy.reset();
    }

    if (m_timer_task != nullptr) {
      m_threads->timer->cancel_event(m_timer_task);
    }
  }

  wait_for_async_ops(on_finish);
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::ImageMap<librbd::ImageCtx>;
