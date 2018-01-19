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
                           << " " << __func__

namespace rbd {
namespace mirror {
namespace image_map {

using librbd::util::unique_lock_name;

const std::string Policy::UNMAPPED_INSTANCE_ID("");

Policy::Policy(librados::IoCtx &ioctx)
  : m_ioctx(ioctx),
    m_map_lock(unique_lock_name("rbd::mirror::image_map::Policy::m_map_lock", this)) {

  // map should at least have once instance
  std::string instance_id = stringify(ioctx.get_instance_id());
  add_instances({instance_id}, nullptr);
}

void Policy::init(const std::map<std::string, cls::rbd::MirrorImageMap> &image_mapping) {
  dout(20) << dendl;

  RWLock::WLocker map_lock(m_map_lock);

  for (auto const &it : image_mapping) {
    map(it.first, it.second.instance_id, it.second.mapped_time, m_map_lock);
  }
}

Policy::LookupInfo Policy::lookup(const std::string &global_image_id) {
  dout(20) << ": global_image_id=" << global_image_id << dendl;

  RWLock::RLocker map_lock(m_map_lock);
  return lookup(global_image_id, m_map_lock);
}

bool Policy::add_image(const std::string &global_image_id,
                       Context *on_update, Context *on_acquire, Context *on_finish) {
  dout(20) << ": global_image_id=" << global_image_id << dendl;

  RWLock::WLocker map_lock(m_map_lock);

  auto it = m_actions.find(global_image_id);
  if (it == m_actions.end()) {
    m_actions.emplace(global_image_id, ActionState());
  }

  Action action = Action::create_add_action(on_update, on_acquire, on_finish);
  return queue_action(global_image_id, action);
}

bool Policy::remove_image(const std::string &global_image_id,
                          Context *on_release, Context *on_remove, Context *on_finish) {
  dout(20) << ": global_image_id=" << global_image_id << dendl;

  RWLock::WLocker map_lock(m_map_lock);

  on_finish = new FunctionContext([this, global_image_id, on_finish](int r) {
      {
        RWLock::WLocker map_lock(m_map_lock);
        if (!actions_pending(global_image_id, m_map_lock)) {
          m_actions.erase(global_image_id);
        }
      }

      if (on_finish != nullptr) {
        on_finish->complete(r);
      }
    });

  Action action = Action::create_remove_action(on_release, on_remove, on_finish);
  return queue_action(global_image_id, action);
}

bool Policy::shuffle_image(const std::string &global_image_id,
                           Context *on_release, Context *on_update,
                           Context *on_acquire, Context *on_finish) {
  dout(20) << ": global_image_id=" << global_image_id << dendl;

  RWLock::WLocker map_lock(m_map_lock);

  Action action = Action::create_shuffle_action(on_release, on_update, on_acquire, on_finish);
  return queue_action(global_image_id, action);
}

void Policy::add_instances(const std::vector<std::string> &instance_ids,
                           std::set<std::string> *remap_global_image_ids) {
  dout(20) << ": adding " << instance_ids.size() << " instance(s)" << dendl;

  RWLock::WLocker map_lock(m_map_lock);

  for (auto const &instance : instance_ids) {
    dout(10) << ": adding instance_id=" << instance << dendl;
    m_map.emplace(instance, std::set<std::string>{});
  }

  if (remap_global_image_ids != nullptr) {
    do_shuffle_add_instances(instance_ids, remap_global_image_ids);
  }
}

void Policy::remove_instances(const std::vector<std::string> &instance_ids,
                              std::set<std::string> *remap_global_image_ids) {
  dout(20) << ": removing " << instance_ids.size() << " instance(s)" << dendl;

  RWLock::WLocker map_lock(m_map_lock);

  for (auto const &instance : instance_ids) {
    dout(10) << ": removing instance_id=" << instance << dendl;
    for (auto const &global_image_id : m_map[instance]) {
      if (!actions_pending(global_image_id, m_map_lock)) {
        remap_global_image_ids->emplace(global_image_id);
      }
    }
  }

  m_dead_instances.insert(instance_ids.begin(), instance_ids.end());
}

// new actions are always started from a stable (idle) state since
// actions either complete successfully ending up in an idle state
// or get aborted due to peer being blacklisted.
void Policy::start_next_action(const std::string &global_image_id) {
  RWLock::WLocker map_lock(m_map_lock);

  auto it = m_actions.find(global_image_id);
  assert(it != m_actions.end());
  assert(!it->second.actions.empty());

  ActionState &action_state = it->second;
  Action &action = action_state.actions.front();

  StateTransition::ActionType action_type = action.get_action_type();
  action_state.transition = StateTransition::transit(action_type, action_state.current_state);

  StateTransition::State next_state = action_state.transition.next_state;

  dout(10) << ": global_image_id=" << global_image_id << ", action=" << action
           << ", current_state=" << action_state.current_state << ", next_state="
           << next_state << dendl;

  // invoke state context callback
  pre_execute_state_callback(global_image_id, action_type, next_state);
  m_map_lock.put_write();
  action.execute_state_callback(next_state);
  m_map_lock.get_write();
}

bool Policy::finish_action(const std::string &global_image_id, int r) {
  RWLock::WLocker map_lock(m_map_lock);

  dout(10) << ": global_image_id=" << global_image_id << ", r=" << r
           << dendl;

  auto it = m_actions.find(global_image_id);
  assert(it != m_actions.end());
  assert(!it->second.actions.empty());

  ActionState &action_state = it->second;
  Action &action = action_state.actions.front();

  bool complete;
  if (can_transit(action_state, r)) {
    complete = perform_transition(global_image_id, &action_state, &action, r != 0);
  } else {
    complete = abort_or_retry(&action_state, &action);
  }

  if (complete) {
    dout(10) << ": completing action=" << action << dendl;

    m_map_lock.put_write();
    action.execute_completion_callback(r);
    m_map_lock.get_write();

    action_state.last_idle_state.reset();
    action_state.actions.pop_front();
  }

  return !action_state.actions.empty();
}

bool Policy::queue_action(const std::string &global_image_id, const Action &action) {
  dout(20) << ": global_image_id=" << global_image_id << ", action=" << action
           << dendl;
  assert(m_map_lock.is_wlocked());

  auto it = m_actions.find(global_image_id);
  assert(it != m_actions.end());

  it->second.actions.push_back(action);
  return it->second.actions.size() == 1;
}

void Policy::rollback(ActionState *action_state) {
  dout(20) << dendl;
  assert(m_map_lock.is_wlocked());

  assert(action_state->transition.error_state);
  StateTransition::State state = action_state->transition.error_state.get();

  dout(10) << ": rolling back state=" << action_state->current_state << " -> "
           << state << dendl;
  action_state->current_state = state;
}

bool Policy::advance(const std::string &global_image_id,
                     ActionState *action_state, Action *action) {
  dout(20) << dendl;
  assert(m_map_lock.is_wlocked());

  StateTransition::State state = action_state->transition.next_state;
  if (!is_state_retriable(state)) {
    action->state_callback_complete(state);
  }

  post_execute_state_callback(global_image_id, state);

  bool reached_final_state = false;
  if (action_state->transition.final_state) {
    reached_final_state = true;
    state = action_state->transition.final_state.get();
    assert(is_idle_state(state));
  }

  dout(10) << ": advancing state=" << action_state->current_state << " -> "
           << state << dendl;
  action_state->current_state = state;

  return reached_final_state;
}

bool Policy::perform_transition(const std::string &global_image_id,
                                ActionState *action_state, Action *action, bool transition_error) {
  dout(20) << dendl;
  assert(m_map_lock.is_wlocked());

  bool complete = false;
  if (transition_error) {
    rollback(action_state);
   } else {
    complete = advance(global_image_id, action_state, action);
  }

  if (is_idle_state(action_state->current_state)) {
    action_state->last_idle_state = action_state->current_state;
    dout(10) << ": transition reached idle state=" << action_state->current_state
             << dendl;
  }

  return complete;
}

bool Policy::abort_or_retry(ActionState *action_state, Action *action) {
  dout(20) << dendl;
  assert(m_map_lock.is_wlocked());

  StateTransition::State state = action_state->transition.next_state;
  bool complete = !is_state_retriable(state);

  if (complete) {
    // we aborted, so the context need not be freed later
    action->state_callback_complete(state);

    if (action_state->last_idle_state) {
      dout(10) << ": using last idle state=" << action_state->last_idle_state.get()
               << " as current state" << dendl;
      action_state->current_state = action_state->last_idle_state.get();
    }
  }

  return complete;
}

void Policy::pre_execute_state_callback(const std::string &global_image_id,
                                        StateTransition::ActionType action_type,
                                        StateTransition::State state) {
  assert(m_map_lock.is_wlocked());

  dout(10) << ": global_image_id=" << global_image_id << ", action_type="
           << action_type << ", state=" << state << dendl;

  utime_t map_time = generate_image_map_timestamp(action_type);
  switch (state) {
  case StateTransition::STATE_UPDATE_MAPPING:
    map(global_image_id, map_time);
    break;
  case StateTransition::STATE_ASSOCIATED:
  case StateTransition::STATE_DISASSOCIATED:
  case StateTransition::STATE_REMOVE_MAPPING:
    break;
  case StateTransition::STATE_UNASSIGNED:
  default:
    assert(false);
  }
}

void Policy::post_execute_state_callback(const std::string &global_image_id, StateTransition::State state) {
  assert(m_map_lock.is_wlocked());

  dout(10) << ": global_image_id=" << global_image_id << ", state=" << state << dendl;

  switch (state) {
  case StateTransition::STATE_DISASSOCIATED:
    unmap(global_image_id);
    break;
  case StateTransition::STATE_ASSOCIATED:
  case StateTransition::STATE_UPDATE_MAPPING:
  case StateTransition::STATE_REMOVE_MAPPING:
    break;
  default:
  case StateTransition::STATE_UNASSIGNED:
    assert(false);
  }
}

bool Policy::actions_pending(const std::string &global_image_id, const RWLock &lock) {
  dout(20) << ": global_image_id=" << global_image_id << dendl;
  assert(m_map_lock.is_locked());

  auto it = m_actions.find(global_image_id);
  assert(it != m_actions.end());

  return !it->second.actions.empty();
}

Policy::LookupInfo Policy::lookup(const std::string &global_image_id, const RWLock &lock) {
  assert(m_map_lock.is_locked());

  LookupInfo info;

  for (auto it = m_map.begin(); it != m_map.end(); ++it) {
    if (it->second.find(global_image_id) != it->second.end()) {
      info.instance_id = it->first;
      info.mapped_time = get_image_mapped_timestamp(global_image_id);
    }
  }

  return info;
}

void Policy::map(const std::string &global_image_id, const std::string &instance_id,
                 utime_t map_time, const RWLock &lock) {
  assert(m_map_lock.is_wlocked());

  auto ins = m_map[instance_id].emplace(global_image_id);
  assert(ins.second);

  set_image_mapped_timestamp(global_image_id, map_time);
}

void Policy::unmap(const std::string &global_image_id, const std::string &instance_id,
                   const RWLock &lock) {
  assert(m_map_lock.is_wlocked());

  m_map[instance_id].erase(global_image_id);

  if (is_dead_instance(instance_id) && m_map[instance_id].empty()) {
    dout(10) << ": removing dead instance_id=" << instance_id << dendl;
    m_map.erase(instance_id);
    m_dead_instances.erase(instance_id);
  }
}

void Policy::map(const std::string &global_image_id, utime_t map_time) {
  dout(20) << ": global_image_id=" << global_image_id << dendl;
  assert(m_map_lock.is_wlocked());

  LookupInfo info = lookup(global_image_id, m_map_lock);
  std::string instance_id = info.instance_id;

  if (instance_id != UNMAPPED_INSTANCE_ID && !is_dead_instance(instance_id)) {
    return;
  }
  if (is_dead_instance(instance_id)) {
    unmap(global_image_id, instance_id, m_map_lock);
  }

  instance_id = do_map(global_image_id);
  map(global_image_id, instance_id, map_time, m_map_lock);
}

void Policy::unmap(const std::string &global_image_id) {
  dout(20) << ": global_image_id=" << global_image_id << dendl;
  assert(m_map_lock.is_wlocked());

  LookupInfo info = lookup(global_image_id, m_map_lock);
  if (info.instance_id == UNMAPPED_INSTANCE_ID) {
    return;
  }

  unmap(global_image_id, info.instance_id, m_map_lock);
}

bool Policy::can_shuffle_image(const std::string &global_image_id) {
  dout(20) << ": global_image_id=" << global_image_id << dendl;
  assert(m_map_lock.is_locked());

  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  int migration_throttle = cct->_conf->get_val<int64_t>("rbd_mirror_image_policy_migration_throttle");

  utime_t last_shuffled_time = get_image_mapped_timestamp(global_image_id);
  dout(10) << ": migration_throttle=" << migration_throttle << ", last_shuffled_time="
           << last_shuffled_time << dendl;

  utime_t now = ceph_clock_now();
  return !actions_pending(global_image_id, m_map_lock) &&
    !(migration_throttle > 0 && (now - last_shuffled_time < migration_throttle));
}

} // namespace image_map
} // namespace mirror
} // namespace rbd
