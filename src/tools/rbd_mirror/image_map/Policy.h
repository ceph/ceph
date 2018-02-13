// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_POLICY_H
#define CEPH_RBD_MIRROR_IMAGE_MAP_POLICY_H

#include <map>
#include <tuple>
#include <boost/optional.hpp>

#include "common/RWLock.h"
#include "cls/rbd/cls_rbd_types.h"
#include "include/rados/librados.hpp"
#include "Action.h"

class Context;

namespace rbd {
namespace mirror {
namespace image_map {

class Policy {
public:
  Policy(librados::IoCtx &ioctx);

  virtual ~Policy() {
  }

  // init -- called during initialization
  void init(const std::map<std::string, cls::rbd::MirrorImageMap> &image_mapping);

  // lookup an image from the map
  struct LookupInfo {
    std::string instance_id = UNMAPPED_INSTANCE_ID;
    utime_t mapped_time;
  };
  LookupInfo lookup(const std::string &global_image_id);

  // add, remove, shuffle
  bool add_image(const std::string &global_image_id,
                 Context *on_update, Context *on_acquire, Context *on_finish);
  bool remove_image(const std::string &global_image_id,
                    Context *on_release, Context *on_remove, Context *on_finish);
  bool shuffle_image(const std::string &global_image_id,
                     Context *on_release, Context *on_update,
                     Context *on_acquire, Context *on_finish);

  // shuffle images when instances are added/removed
  void add_instances(const std::vector<std::string> &instance_ids,
                     std::set<std::string> *remap_global_image_ids);
  void remove_instances(const std::vector<std::string> &instance_ids,
                        std::set<std::string> *remap_global_image_ids);

  void start_next_action(const std::string &global_image_id);
  bool finish_action(const std::string &global_image_id, int r);

  static const std::string UNMAPPED_INSTANCE_ID;

private:
  typedef std::list<Action> Actions;

  struct ActionState {
    Actions actions;                                                          // list of pending actions

    StateTransition::State current_state = StateTransition::STATE_UNASSIGNED; // current state
    boost::optional<StateTransition::State> last_idle_state;                  // last successfull idle
                                                                              // state transition

    StateTransition::Transition transition;                                   // (cached) next transition

    utime_t map_time;                                                         // (re)mapped time
  };

  // for the lack of a better function name
  bool is_state_retriable(StateTransition::State state) {
    return state == StateTransition::STATE_UPDATE_MAPPING ||
           state == StateTransition::STATE_REMOVE_MAPPING ||
           state == StateTransition::STATE_ASSOCIATED;
  }
  // can the state machine transit advance (on success) or rollback
  // (on failure).
  bool can_transit(const ActionState &action_state, int r) {
    assert(m_map_lock.is_locked());
    return r == 0 || action_state.transition.error_state;
  }

  void set_image_mapped_timestamp(const std::string &global_image_id, utime_t time) {
    assert(m_map_lock.is_wlocked());

    auto it = m_actions.find(global_image_id);
    assert(it != m_actions.end());
    it->second.map_time = time;
  }
  utime_t get_image_mapped_timestamp(const std::string &global_image_id) {
    assert(m_map_lock.is_locked());

    auto it = m_actions.find(global_image_id);
    assert(it != m_actions.end());
    return it->second.map_time;
  }

  librados::IoCtx &m_ioctx;
  std::map<std::string, ActionState> m_actions;
  std::set<std::string> m_dead_instances;

  bool is_idle_state(StateTransition::State state) {
    if (state == StateTransition::STATE_UNASSIGNED ||
        state == StateTransition::STATE_ASSOCIATED ||
        state == StateTransition::STATE_DISASSOCIATED) {
      return true;
    }

    return false;
  }

  // generate image map time based on action type
  utime_t generate_image_map_timestamp(StateTransition::ActionType action_type) {
    // for a shuffle action (image remap) use current time as
    // map time, historical time otherwise.
    utime_t time;
    if (action_type == StateTransition::ACTION_TYPE_SHUFFLE) {
      time = ceph_clock_now();
    } else {
      time = utime_t(0, 0);
    }

    return time;
  }

  bool queue_action(const std::string &global_image_id, const Action &action);
  bool actions_pending(const std::string &global_image_id, const RWLock &lock);

  LookupInfo lookup(const std::string &global_image_id, const RWLock &lock);
  void map(const std::string &global_image_id,
           const std::string &instance_id, utime_t map_time, const RWLock &lock);
  void unmap(const std::string &global_image_id, const std::string &instance_id, const RWLock &lock);

  // map an image
  void map(const std::string &global_image_id, utime_t map_time);
  // unmap (remove) an image from the map
  void unmap(const std::string &global_image_id);

  // state transition related..
  void pre_execute_state_callback(const std::string &global_image_id,
                                  StateTransition::ActionType action_type, StateTransition::State state);
  void post_execute_state_callback(const std::string &global_image_id, StateTransition::State state);

  void rollback(ActionState *action_state);
  bool advance(const std::string &global_image_id, ActionState *action_state, Action *action);

  bool perform_transition(const std::string &global_image_id, ActionState *action_state,
                          Action *action, bool transition_error);
  bool abort_or_retry(ActionState *action_state, Action *action);

protected:
  typedef std::map<std::string, std::set<std::string> > InstanceToImageMap;

  RWLock m_map_lock;        // protects m_map, m_shuffled_timestamp
  InstanceToImageMap m_map; // instance_id -> global_id map

  bool is_dead_instance(const std::string instance_id) {
    assert(m_map_lock.is_locked());
    return m_dead_instances.find(instance_id) != m_dead_instances.end();
  }

  bool can_shuffle_image(const std::string &global_image_id);

  // map an image (global image id) to an instance
  virtual std::string do_map(const std::string &global_image_id) = 0;

  // shuffle images when instances are added/removed
  virtual void do_shuffle_add_instances(const std::vector<std::string> &instance_ids,
                                        std::set<std::string> *remap_global_image_ids) = 0;
};

} // namespace image_map
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_POLICY_H
