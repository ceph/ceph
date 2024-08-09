// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"
#include "common/Timer.h"

#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "tools/rbd_mirror/Threads.h"

#include "ImageMap.h"
#include "image_map/LoadRequest.h"
#include "image_map/SimplePolicy.h"
#include "image_map/UpdateRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::ImageMap: " << this << " " \
                           << __func__ << ": "

using namespace std;

namespace rbd {
namespace mirror {

using ::operator<<;
using image_map::Policy;
using image_map::GlobalId;
using image_map::GlobalIds;

using librbd::util::unique_lock_name;
using librbd::util::create_async_context_callback;

template <typename I>
struct ImageMap<I>::C_NotifyInstance : public Context {
  ImageMap* image_map;
  GlobalId global_id;
  bool acquire_release;

  C_NotifyInstance(ImageMap* image_map, const GlobalId& global_id,
                   bool acquire_release)
    : image_map(image_map), global_id(global_id),
      acquire_release(acquire_release) {
    image_map->start_async_op();
  }

  void finish(int r) override {
    if (acquire_release) {
      image_map->handle_peer_ack(global_id, r);
    } else {
      image_map->handle_peer_ack_remove(global_id, r);
    }
    image_map->finish_async_op();
  }
};

template <typename I>
ImageMap<I>::ImageMap(librados::IoCtx &ioctx, Threads<I> *threads,
                      const std::string& instance_id,
                      image_map::Listener &listener)
  : m_ioctx(ioctx), m_threads(threads), m_instance_id(instance_id),
    m_listener(listener),
    m_lock(ceph::make_mutex(
      unique_lock_name("rbd::mirror::ImageMap::m_lock", this))) {
}

template <typename I>
ImageMap<I>::~ImageMap() {
  ceph_assert(m_async_op_tracker.empty());
  ceph_assert(m_timer_task == nullptr);
  ceph_assert(m_rebalance_task == nullptr);
}

template <typename I>
void ImageMap<I>::continue_action(const GlobalIds &global_ids, int r) {
  dout(20) << dendl;

  {
    std::lock_guard locker{m_lock};
    if (m_shutting_down) {
      return;
    }

    for (auto const &global_id : global_ids) {
      bool schedule = m_policy->finish_action(global_id, r);
      if (schedule) {
        schedule_action(global_id);
      }
    }
  }

  schedule_update_task();
}

template <typename I>
void ImageMap<I>::handle_update_request(
    const Updates &updates, const GlobalIds &remove_global_ids, int r) {
  dout(20) << "r=" << r << dendl;

  GlobalIds global_ids;

  global_ids.insert(remove_global_ids.begin(), remove_global_ids.end());
  for (auto const &update : updates) {
    global_ids.insert({update.entity.type, update.entity.global_id});
  }

  continue_action(global_ids, r);
}

template <typename I>
void ImageMap<I>::update_image_mapping(Updates &&map_updates,
                                       GlobalIds &&map_removals) {
  if (map_updates.empty() && map_removals.empty()) {
    return;
  }

  dout(5) << "updates=[" << map_updates << "], "
          << "removes=[" << map_removals << "]" << dendl;

  Context *on_finish = new LambdaContext(
    [this, map_updates, map_removals](int r) {
      handle_update_request(map_updates, map_removals, r);
      finish_async_op();
    });
  on_finish = create_async_context_callback(m_threads->work_queue, on_finish);

  // empty meta policy for now..
  image_map::PolicyMetaNone policy_meta;

  // prepare update map
  std::map<GlobalId, cls::rbd::MirrorImageMap> update_mapping;
  for (auto const &update : map_updates) {
    bufferlist bl;
    encode(image_map::PolicyData(update.entity.count, policy_meta), bl);

    update_mapping.emplace(
        GlobalId{update.entity.type, update.entity.global_id},
        cls::rbd::MirrorImageMap(update.instance_id, update.mapped_time, bl));
  }

  start_async_op();
  image_map::UpdateRequest<I> *req = image_map::UpdateRequest<I>::create(
    m_ioctx, std::move(update_mapping), std::move(map_removals), on_finish);
  req->send();
}

template <typename I>
void ImageMap<I>::process_updates() {
  dout(20) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_threads->timer_lock));
  ceph_assert(m_timer_task == nullptr);

  Updates map_updates;
  GlobalIds map_removals;
  Updates acquire_updates;
  Updates release_updates;

  // gather updates by advancing the state machine
  m_lock.lock();
  for (auto const &global_id : m_global_ids) {
    image_map::ActionType action_type = m_policy->start_action(global_id);
    image_map::LookupInfo info = m_policy->lookup(global_id);

    dout(15) << "global_id=" << global_id << ", "
             << "action=" << action_type << ", "
             << "instance=" << info.instance_id << dendl;
    switch (action_type) {
    case image_map::ACTION_TYPE_NONE:
      continue;
    case image_map::ACTION_TYPE_MAP_UPDATE:
      ceph_assert(info.instance_id != image_map::UNMAPPED_INSTANCE_ID);
      map_updates.emplace_back(
          MirrorEntity{global_id.type, global_id.id, info.weight},
          info.instance_id, info.mapped_time);
      break;
    case image_map::ACTION_TYPE_MAP_REMOVE:
      map_removals.emplace(global_id);
      break;
    case image_map::ACTION_TYPE_ACQUIRE:
      ceph_assert(info.instance_id != image_map::UNMAPPED_INSTANCE_ID);
      acquire_updates.emplace_back(
          MirrorEntity{global_id.type, global_id.id, info.weight},
          info.instance_id);
      break;
    case image_map::ACTION_TYPE_RELEASE:
      ceph_assert(info.instance_id != image_map::UNMAPPED_INSTANCE_ID);
      release_updates.emplace_back(
          MirrorEntity{global_id.type, global_id.id, info.weight},
          info.instance_id);
      break;
    }
  }
  m_global_ids.clear();
  m_lock.unlock();

  // notify listener (acquire, release) and update on-disk map. note
  // that its safe to process this outside m_lock as we still hold
  // timer lock.
  notify_listener_acquire_release_images(acquire_updates, release_updates);
  update_image_mapping(std::move(map_updates), std::move(map_removals));
}

template <typename I>
void ImageMap<I>::schedule_update_task() {
  std::lock_guard timer_lock{m_threads->timer_lock};
  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  double after = cct->_conf.get_val<double>("rbd_mirror_image_policy_update_throttle_interval");
  schedule_update_task(m_threads->timer_lock, after);
}

template <typename I>
void ImageMap<I>::schedule_update_task(const ceph::mutex &timer_lock,
                                       double after) {
  ceph_assert(ceph_mutex_is_locked(m_threads->timer_lock));

  schedule_rebalance_task();

  if (m_timer_task != nullptr) {
    return;
  }

  {
    std::lock_guard locker{m_lock};
    if (m_global_ids.empty()) {
      return;
    }
  }

  m_timer_task = new LambdaContext([this](int r) {
      ceph_assert(ceph_mutex_is_locked(m_threads->timer_lock));
      m_timer_task = nullptr;

      process_updates();
    });

  dout(20) << "scheduling image check update (" << m_timer_task << ")"
           << " after " << after << " second(s)" << dendl;
  m_threads->timer->add_event_after(after, m_timer_task);
}

template <typename I>
void ImageMap<I>::rebalance() {
  ceph_assert(m_rebalance_task == nullptr);

  {
    std::lock_guard locker{m_lock};
    if (m_async_op_tracker.empty() && m_global_ids.empty()){
      dout(20) << "starting rebalance" << dendl;

      GlobalIds remap_global_ids;
      m_policy->add_instances({}, &remap_global_ids);

      for (auto const &global_id : remap_global_ids) {
        schedule_action(global_id);
      }
    }
  }

  schedule_update_task();
}

template <typename I>
void ImageMap<I>::schedule_rebalance_task() {
  ceph_assert(ceph_mutex_is_locked(m_threads->timer_lock));

  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());

  // fetch the updated value of idle timeout for (re)scheduling
  double resched_after = cct->_conf.get_val<double>(
    "rbd_mirror_image_policy_rebalance_timeout");
  if (!resched_after) {
    return;
  }

  // cancel existing rebalance task if any before scheduling
  if (m_rebalance_task != nullptr) {
    m_threads->timer->cancel_event(m_rebalance_task);
  }

  m_rebalance_task = new LambdaContext([this](int _) {
      ceph_assert(ceph_mutex_is_locked(m_threads->timer_lock));
      m_rebalance_task = nullptr;

      rebalance();
    });

  dout(20) << "scheduling rebalance (" << m_rebalance_task << ")"
           << " after " << resched_after << " second(s)" << dendl;
  m_threads->timer->add_event_after(resched_after, m_rebalance_task);
}

template <typename I>
void ImageMap<I>::schedule_action(const GlobalId &global_id) {
  dout(20) << "global_id=" << global_id << dendl;
  ceph_assert(ceph_mutex_is_locked(m_lock));

  m_global_ids.emplace(global_id);
}

template <typename I>
void ImageMap<I>::notify_listener_acquire_release_images(
    const Updates &acquire, const Updates &release) {
  if (acquire.empty() && release.empty()) {
    return;
  }

  dout(5) << "acquire=[" << acquire << "], "
          << "release=[" << release << "]" << dendl;

  for (auto const &update : acquire) {
    auto global_id = GlobalId(update.entity.type, update.entity.global_id);
    if (update.entity.type == MIRROR_ENTITY_TYPE_IMAGE) {
      m_listener.acquire_image(
        update.entity.global_id, update.instance_id,
        create_async_context_callback(
          m_threads->work_queue,
          new C_NotifyInstance(this, global_id, true)));
    } else if (update.entity.type == MIRROR_ENTITY_TYPE_GROUP) {
      m_listener.acquire_group(
        update.entity.global_id, update.instance_id,
        create_async_context_callback(
          m_threads->work_queue,
          new C_NotifyInstance(this, global_id, true)));
    } else {
      ceph_abort_msgf("invalid mirror entity type: %d",
                      (int)update.entity.type);
    }
  }

  for (auto const &update : release) {
    auto global_id = GlobalId(update.entity.type, update.entity.global_id);
    if (update.entity.type == MIRROR_ENTITY_TYPE_IMAGE || update.entity.count != 0) {
      m_listener.release_image(
        update.entity.global_id, update.instance_id,
        create_async_context_callback(
          m_threads->work_queue,
          new C_NotifyInstance(this, global_id, true)));
    } else if (update.entity.type == MIRROR_ENTITY_TYPE_GROUP) {
      m_listener.release_group(
        update.entity.global_id, update.instance_id,
        create_async_context_callback(
          m_threads->work_queue,
          new C_NotifyInstance(this, global_id, true)));
    } else {
      ceph_abort_msgf("invalid mirror entity type: %d",
                      (int)update.entity.type);
    }
  }
}

template <typename I>
void ImageMap<I>::notify_listener_remove_images(const std::string &mirror_uuid,
                                                const Updates &remove) {
  dout(5) << "mirror_uuid=" << mirror_uuid << ", "
          << "remove=[" << remove << "]" << dendl;

  for (auto const &update : remove) {
    auto global_id = GlobalId(update.entity.type, update.entity.global_id);
    if (update.entity.type == MIRROR_ENTITY_TYPE_IMAGE) {
      m_listener.remove_image(
        mirror_uuid, update.entity.global_id, update.instance_id,
        create_async_context_callback(
          m_threads->work_queue,
          new C_NotifyInstance(this, global_id, false)));
    } else if (update.entity.type == MIRROR_ENTITY_TYPE_GROUP) {
      m_listener.remove_group(
        mirror_uuid, update.entity.global_id, update.instance_id,
        create_async_context_callback(
          m_threads->work_queue,
          new C_NotifyInstance(this, global_id, false)));
    } else {
      ceph_abort_msgf("invalid mirror entity type: %d",
                      (int)update.entity.type);
    }
  }
}

template <typename I>
void ImageMap<I>::handle_load(const std::map<GlobalId,
                              cls::rbd::MirrorImageMap> &image_mapping) {
  dout(20) << dendl;

  {
    std::lock_guard locker{m_lock};
    m_policy->init(image_mapping);

    for (auto& pair : image_mapping) {
      schedule_action(pair.first);
    }
  }
  schedule_update_task();
}

template <typename I>
void ImageMap<I>::handle_peer_ack_remove(const GlobalId &global_id, int r) {
  std::lock_guard locker{m_lock};
  dout(5) << "global_id=" << global_id << dendl;

  if (r < 0) {
    derr << "failed to remove global_id=" << global_id << dendl;
  }

  auto peer_it = m_peer_map.find(global_id);
  if (peer_it == m_peer_map.end()) {
    return;
  }

  m_peer_map.erase(peer_it);
}

template <typename I>
void ImageMap<I>::update_images_added(
    const std::string &mirror_uuid,
    const MirrorEntities &entities) {
  dout(5) << "mirror_uuid=" << mirror_uuid << ", "
          << "entities={ " << entities << " }" << dendl;
  ceph_assert(ceph_mutex_is_locked(m_lock));

  for (auto &entity : entities) {
    auto global_id = GlobalId(entity.type, entity.global_id);
    auto result = m_peer_map[global_id].insert(mirror_uuid);
    if ((result.second || entity.type == MIRROR_ENTITY_TYPE_GROUP)) {
      if (m_policy->add_entity(global_id, entity.count)) {
        schedule_action(global_id);
      }
    }
  }
}

template <typename I>
void ImageMap<I>::update_images_removed(
    const std::string &mirror_uuid,
    const MirrorEntities &entities) {
  dout(5) << "mirror_uuid=" << mirror_uuid << ", "
          << "entities={ " << entities << " }" << dendl;
  ceph_assert(ceph_mutex_is_locked(m_lock));

  Updates to_remove;
  for (auto &entity : entities) {
    auto global_id = GlobalId(entity.type, entity.global_id);
    image_map::LookupInfo info = m_policy->lookup(global_id);
    bool entity_mapped = (info.instance_id != image_map::UNMAPPED_INSTANCE_ID);

    bool entity_removed = entity_mapped;
    bool peer_removed = false;
    auto peer_it = m_peer_map.find(global_id);
    if (peer_it != m_peer_map.end()) {
      auto& peer_set = peer_it->second;
      peer_removed = peer_set.erase(mirror_uuid);
      entity_removed = peer_removed && peer_set.empty();
    }

    if (entity_mapped && peer_removed && !mirror_uuid.empty()) {
      // peer entity has been deleted or local non-image entity needs restart
      to_remove.emplace_back(entity, info.instance_id);
    }

    if (entity_removed) {
      // local and peer images have been deleted
      if (m_policy->remove_entity(global_id)) {
        schedule_action(global_id);
      }
    }
  }

  if (!to_remove.empty()) {
    // removal notification will be notified instantly. this is safe
    // even after scheduling action for images as we still hold m_lock
    notify_listener_remove_images(mirror_uuid, to_remove);
  }
}

template <typename I>
void ImageMap<I>::update_instances_added(
    const std::vector<std::string> &instance_ids) {
  {
    std::lock_guard locker{m_lock};
    if (m_shutting_down) {
      return;
    }

    std::vector<std::string> filtered_instance_ids;
    filter_instance_ids(instance_ids, &filtered_instance_ids, false);
    if (filtered_instance_ids.empty()) {
      return;
    }

    dout(20) << "instance_ids=" << filtered_instance_ids << dendl;

    GlobalIds remap_global_ids;
    m_policy->add_instances(filtered_instance_ids, &remap_global_ids);

    for (auto const &global_id : remap_global_ids) {
      schedule_action(global_id);
    }
  }

  schedule_update_task();
}

template <typename I>
void ImageMap<I>::update_instances_removed(
    const std::vector<std::string> &instance_ids) {
  {
    std::lock_guard locker{m_lock};
    if (m_shutting_down) {
      return;
    }

    std::vector<std::string> filtered_instance_ids;
    filter_instance_ids(instance_ids, &filtered_instance_ids, true);
    if (filtered_instance_ids.empty()) {
      return;
    }

    dout(20) << "instance_ids=" << filtered_instance_ids << dendl;

    GlobalIds remap_global_ids;
    m_policy->remove_instances(filtered_instance_ids, &remap_global_ids);

    for (auto const &global_id : remap_global_ids) {
      schedule_action(global_id);
    }
  }

  schedule_update_task();
}

template <typename I>
void ImageMap<I>::update_images(const std::string &mirror_uuid,
                                MirrorEntities &&added_entities,
                                MirrorEntities &&removed_entities) {
  dout(5) << "mirror_uuid=" << mirror_uuid << ", " << "added_count="
          << added_entities.size() << ", " << "removed_count="
          << removed_entities.size() << dendl;

  {
    std::lock_guard locker{m_lock};
    if (m_shutting_down) {
      return;
    }

    if (!removed_entities.empty()) {
      update_images_removed(mirror_uuid, removed_entities);
    }
    if (!added_entities.empty()) {
      update_images_added(mirror_uuid, added_entities);
    }
  }

  schedule_update_task();
}

template <typename I>
void ImageMap<I>::handle_peer_ack(const GlobalId &global_id, int r) {
  dout (20) << "global_id=" << global_id << ", r=" << r << dendl;

  continue_action({global_id}, r);
}

template <typename I>
void ImageMap<I>::init(Context *on_finish) {
  dout(20) << dendl;

  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  std::string policy_type = cct->_conf.get_val<string>("rbd_mirror_image_policy_type");

  if (policy_type == "none" || policy_type == "simple") {
    m_policy.reset(image_map::SimplePolicy::create(m_ioctx));
  } else {
    ceph_abort(); // not really needed as such, but catch it.
  }

  dout(20) << "mapping policy=" << policy_type << dendl;

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
    std::lock_guard timer_lock{m_threads->timer_lock};

    {
      std::lock_guard locker{m_lock};
      ceph_assert(!m_shutting_down);

      m_shutting_down = true;
      m_policy.reset();
    }

    if (m_timer_task != nullptr) {
      m_threads->timer->cancel_event(m_timer_task);
      m_timer_task = nullptr;
    }
    if (m_rebalance_task != nullptr) {
      m_threads->timer->cancel_event(m_rebalance_task);
      m_rebalance_task = nullptr;
    }
  }

  wait_for_async_ops(on_finish);
}

template <typename I>
void ImageMap<I>::filter_instance_ids(
    const std::vector<std::string> &instance_ids,
    std::vector<std::string> *filtered_instance_ids, bool removal) const {
  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  std::string policy_type = cct->_conf.get_val<string>("rbd_mirror_image_policy_type");

  if (policy_type != "none") {
    *filtered_instance_ids = instance_ids;
    return;
  }

  if (removal) {
    // propagate removals for external instances
    for (auto& instance_id : instance_ids) {
      if (instance_id != m_instance_id) {
        filtered_instance_ids->push_back(instance_id);
      }
    }
  } else if (std::find(instance_ids.begin(), instance_ids.end(),
                       m_instance_id) != instance_ids.end()) {
    // propagate addition only for local instance
    filtered_instance_ids->push_back(m_instance_id);
  }
}

} // namespace mirror
} // namespace rbd

template class rbd::mirror::ImageMap<librbd::ImageCtx>;
