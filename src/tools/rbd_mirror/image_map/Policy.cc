// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"

#include "librbd/Utils.h"
#include "Policy.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_map::Policy: " << this \
                           << " " << __func__ << ": "

namespace rbd {
namespace mirror {
namespace image_map {

namespace {

bool is_instance_action(ActionType action_type) {
  switch (action_type) {
  case ACTION_TYPE_ACQUIRE:
  case ACTION_TYPE_RELEASE:
    return true;
  case ACTION_TYPE_NONE:
  case ACTION_TYPE_MAP_UPDATE:
  case ACTION_TYPE_MAP_REMOVE:
    break;
  }
  return false;
}

} // anonymous namespace

using ::operator<<;
using librbd::util::unique_lock_name;

Policy::Policy(librados::IoCtx &ioctx)
  : m_ioctx(ioctx),
    m_map_lock(ceph::make_shared_mutex(
     unique_lock_name("rbd::mirror::image_map::Policy::m_map_lock", this))) {

  // map should at least have once instance
  std::string instance_id = stringify(ioctx.get_instance_id());
  m_map.emplace(instance_id, std::set<std::string>{});
}

void Policy::init(
    const std::map<std::string, cls::rbd::MirrorImageMap> &image_mapping) {
  dout(20) << dendl;

  std::unique_lock map_lock{m_map_lock};
  for (auto& it : image_mapping) {
    ceph_assert(!it.second.instance_id.empty());
    auto map_result = m_map[it.second.instance_id].emplace(it.first);
    ceph_assert(map_result.second);

    auto image_state_result = m_image_states.emplace(
      it.first, ImageState{it.second.instance_id, it.second.mapped_time});
    ceph_assert(image_state_result.second);

    // ensure we (re)send image acquire actions to the instance
    auto& image_state = image_state_result.first->second;
    auto start_action = set_state(&image_state,
                                  StateTransition::STATE_INITIALIZING, false);
    ceph_assert(start_action);
  }
}

LookupInfo Policy::lookup(const std::string &global_image_id) {
  dout(20) << "global_image_id=" << global_image_id << dendl;

  std::shared_lock map_lock{m_map_lock};
  LookupInfo info;

  auto it = m_image_states.find(global_image_id);
  if (it != m_image_states.end()) {
    info.instance_id = it->second.instance_id;
    info.mapped_time = it->second.mapped_time;
  }
  return info;
}

bool Policy::add_image(const std::string &global_image_id) {
  dout(5) << "global_image_id=" << global_image_id << dendl;

  std::unique_lock map_lock{m_map_lock};
  auto image_state_result = m_image_states.emplace(global_image_id,
                                                   ImageState{});
  auto& image_state = image_state_result.first->second;
  if (image_state.state == StateTransition::STATE_INITIALIZING) {
    // avoid duplicate acquire notifications upon leader startup
    return false;
  }

  return set_state(&image_state, StateTransition::STATE_ASSOCIATING, false);
}

bool Policy::remove_image(const std::string &global_image_id) {
  dout(5) << "global_image_id=" << global_image_id << dendl;

  std::unique_lock map_lock{m_map_lock};
  auto it = m_image_states.find(global_image_id);
  if (it == m_image_states.end()) {
    return false;
  }

  auto& image_state = it->second;
  return set_state(&image_state, StateTransition::STATE_DISSOCIATING, false);
}

void Policy::add_instances(const InstanceIds &instance_ids,
                           GlobalImageIds* global_image_ids) {
  dout(5) << "instance_ids=" << instance_ids << dendl;

  std::unique_lock map_lock{m_map_lock};
  for (auto& instance : instance_ids) {
    ceph_assert(!instance.empty());
    m_map.emplace(instance, std::set<std::string>{});
  }

  // post-failover, remove any dead instances and re-shuffle their images
  if (m_initial_update) {
    dout(5) << "initial instance update" << dendl;
    m_initial_update = false;

    std::set<std::string> alive_instances(instance_ids.begin(),
                                          instance_ids.end());
    InstanceIds dead_instances;
    for (auto& map_pair : m_map) {
      if (alive_instances.find(map_pair.first) == alive_instances.end()) {
        dead_instances.push_back(map_pair.first);
      }
    }

    if (!dead_instances.empty()) {
      remove_instances(m_map_lock, dead_instances, global_image_ids);
    }
  }

  GlobalImageIds shuffle_global_image_ids;
  do_shuffle_add_instances(m_map, m_image_states.size(), &shuffle_global_image_ids);
  dout(5) << "shuffling global_image_ids=[" << shuffle_global_image_ids
          << "]" << dendl;
  for (auto& global_image_id : shuffle_global_image_ids) {
    auto it = m_image_states.find(global_image_id);
    ceph_assert(it != m_image_states.end());

    auto& image_state = it->second;
    if (set_state(&image_state, StateTransition::STATE_SHUFFLING, false)) {
      global_image_ids->emplace(global_image_id);
    }
  }
}

void Policy::remove_instances(const InstanceIds &instance_ids,
                              GlobalImageIds* global_image_ids) {
  std::unique_lock map_lock{m_map_lock};
  remove_instances(m_map_lock, instance_ids, global_image_ids);
}

void Policy::remove_instances(const ceph::shared_mutex& lock,
                              const InstanceIds &instance_ids,
                              GlobalImageIds* global_image_ids) {
  ceph_assert(ceph_mutex_is_wlocked(m_map_lock));
  dout(5) << "instance_ids=" << instance_ids << dendl;

  for (auto& instance_id : instance_ids) {
    auto map_it = m_map.find(instance_id);
    if (map_it == m_map.end()) {
      continue;
    }

    auto& instance_global_image_ids = map_it->second;
    if (instance_global_image_ids.empty()) {
      m_map.erase(map_it);
      continue;
    }

    m_dead_instances.insert(instance_id);
    dout(5) << "force shuffling: instance_id=" << instance_id << ", "
            << "global_image_ids=[" << instance_global_image_ids << "]"<< dendl;
    for (auto& global_image_id : instance_global_image_ids) {
      auto it = m_image_states.find(global_image_id);
      ceph_assert(it != m_image_states.end());

      auto& image_state = it->second;
      if (is_state_scheduled(image_state,
                             StateTransition::STATE_DISSOCIATING)) {
        // don't shuffle images that no longer exist
        continue;
      }

      if (set_state(&image_state, StateTransition::STATE_SHUFFLING, true)) {
        global_image_ids->emplace(global_image_id);
      }
    }
  }
}

ActionType Policy::start_action(const std::string &global_image_id) {
  std::unique_lock map_lock{m_map_lock};

  auto it = m_image_states.find(global_image_id);
  ceph_assert(it != m_image_states.end());

  auto& image_state = it->second;
  auto& transition = image_state.transition;
  ceph_assert(transition.action_type != ACTION_TYPE_NONE);

  dout(5) << "global_image_id=" << global_image_id << ", "
          << "state=" << image_state.state << ", "
          << "action_type=" << transition.action_type << dendl;
  if (transition.start_policy_action) {
    execute_policy_action(global_image_id, &image_state,
                          *transition.start_policy_action);
    transition.start_policy_action = boost::none;
  }
  return transition.action_type;
}

bool Policy::finish_action(const std::string &global_image_id, int r) {
  std::unique_lock map_lock{m_map_lock};

  auto it = m_image_states.find(global_image_id);
  ceph_assert(it != m_image_states.end());

  auto& image_state = it->second;
  auto& transition = image_state.transition;
  dout(5) << "global_image_id=" << global_image_id << ", "
          << "state=" << image_state.state << ", "
          << "action_type=" << transition.action_type << ", "
          << "r=" << r << dendl;

  // retry on failure unless it's an RPC message to an instance that is dead
  if (r < 0 &&
      (!is_instance_action(image_state.transition.action_type) ||
       image_state.instance_id == UNMAPPED_INSTANCE_ID ||
       m_dead_instances.find(image_state.instance_id) ==
         m_dead_instances.end())) {
    return true;
  }

  auto finish_policy_action = transition.finish_policy_action;
  StateTransition::transit(image_state.state, &image_state.transition);
  if (transition.finish_state) {
    // in-progress state machine complete
    ceph_assert(StateTransition::is_idle(*transition.finish_state));
    image_state.state = *transition.finish_state;
    image_state.transition = {};
  }

  if (StateTransition::is_idle(image_state.state) && image_state.next_state) {
    // advance to pending state machine
    bool start_action = set_state(&image_state, *image_state.next_state, false);
    ceph_assert(start_action);
  }

  // image state may get purged in execute_policy_action()
  bool pending_action = image_state.transition.action_type != ACTION_TYPE_NONE;
  if (finish_policy_action) {
    execute_policy_action(global_image_id, &image_state, *finish_policy_action);
  }

  return pending_action;
}

void Policy::execute_policy_action(
    const std::string& global_image_id, ImageState* image_state,
    StateTransition::PolicyAction policy_action) {
  dout(5) << "global_image_id=" << global_image_id << ", "
          << "policy_action=" << policy_action << dendl;

  switch (policy_action) {
  case StateTransition::POLICY_ACTION_MAP:
    map(global_image_id, image_state);
    break;
  case StateTransition::POLICY_ACTION_UNMAP:
    unmap(global_image_id, image_state);
    break;
  case StateTransition::POLICY_ACTION_REMOVE:
    if (image_state->state == StateTransition::STATE_UNASSOCIATED) {
      ceph_assert(image_state->instance_id == UNMAPPED_INSTANCE_ID);
      ceph_assert(!image_state->next_state);
      m_image_states.erase(global_image_id);
    }
    break;
  }
}

void Policy::map(const std::string& global_image_id, ImageState* image_state) {
  ceph_assert(ceph_mutex_is_wlocked(m_map_lock));

  std::string instance_id = image_state->instance_id;
  if (instance_id != UNMAPPED_INSTANCE_ID && !is_dead_instance(instance_id)) {
    return;
  }
  if (is_dead_instance(instance_id)) {
    unmap(global_image_id, image_state);
  }

  instance_id = do_map(m_map, global_image_id);
  ceph_assert(!instance_id.empty());
  dout(5) << "global_image_id=" << global_image_id << ", "
          << "instance_id=" << instance_id << dendl;

  image_state->instance_id = instance_id;
  image_state->mapped_time = ceph_clock_now();

  auto ins = m_map[instance_id].emplace(global_image_id);
  ceph_assert(ins.second);
}

void Policy::unmap(const std::string &global_image_id,
                   ImageState* image_state) {
  ceph_assert(ceph_mutex_is_wlocked(m_map_lock));

  std::string instance_id = image_state->instance_id;
  if (instance_id == UNMAPPED_INSTANCE_ID) {
    return;
  }

  dout(5) << "global_image_id=" << global_image_id << ", "
          << "instance_id=" << instance_id << dendl;

  ceph_assert(!instance_id.empty());
  m_map[instance_id].erase(global_image_id);
  image_state->instance_id = UNMAPPED_INSTANCE_ID;
  image_state->mapped_time = {};

  if (is_dead_instance(instance_id) && m_map[instance_id].empty()) {
    dout(5) << "removing dead instance_id=" << instance_id << dendl;
    m_map.erase(instance_id);
    m_dead_instances.erase(instance_id);
  }
}

bool Policy::is_image_shuffling(const std::string &global_image_id) {
  ceph_assert(ceph_mutex_is_locked(m_map_lock));

  auto it = m_image_states.find(global_image_id);
  ceph_assert(it != m_image_states.end());
  auto& image_state = it->second;

  // avoid attempting to re-shuffle a pending shuffle
  auto result = is_state_scheduled(image_state,
                                   StateTransition::STATE_SHUFFLING);
  dout(20) << "global_image_id=" << global_image_id << ", "
           << "result=" << result << dendl;
  return result;
}

bool Policy::can_shuffle_image(const std::string &global_image_id) {
  ceph_assert(ceph_mutex_is_locked(m_map_lock));

  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  int migration_throttle = cct->_conf.get_val<uint64_t>(
    "rbd_mirror_image_policy_migration_throttle");

  auto it = m_image_states.find(global_image_id);
  ceph_assert(it != m_image_states.end());
  auto& image_state = it->second;

  utime_t last_shuffled_time = image_state.mapped_time;

  // idle images that haven't been recently remapped can shuffle
  utime_t now = ceph_clock_now();
  auto result = (StateTransition::is_idle(image_state.state) &&
                 ((migration_throttle <= 0) ||
                  (now - last_shuffled_time >= migration_throttle)));
  dout(10) << "global_image_id=" << global_image_id << ", "
           << "migration_throttle=" << migration_throttle << ", "
           << "last_shuffled_time=" << last_shuffled_time << ", "
           << "result=" << result << dendl;
  return result;
}

bool Policy::set_state(ImageState* image_state, StateTransition::State state,
                       bool ignore_current_state) {
  if (!ignore_current_state && image_state->state == state) {
    image_state->next_state = boost::none;
    return false;
  } else if (StateTransition::is_idle(image_state->state)) {
    image_state->state = state;
    image_state->next_state = boost::none;

    StateTransition::transit(image_state->state, &image_state->transition);
    ceph_assert(image_state->transition.action_type != ACTION_TYPE_NONE);
    ceph_assert(!image_state->transition.finish_state);
    return true;
  }

  image_state->next_state = state;
  return false;
}

bool Policy::is_state_scheduled(const ImageState& image_state,
                                StateTransition::State state) const {
  return (image_state.state == state ||
          (image_state.next_state && *image_state.next_state == state));
}

} // namespace image_map
} // namespace mirror
} // namespace rbd
