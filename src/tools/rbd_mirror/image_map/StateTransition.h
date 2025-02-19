// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_STATE_TRANSITION_H
#define CEPH_RBD_MIRROR_IMAGE_MAP_STATE_TRANSITION_H

#include "tools/rbd_mirror/image_map/Types.h"
#include <boost/optional.hpp>
#include <map>

namespace rbd {
namespace mirror {
namespace image_map {

class StateTransition {
public:
  enum State {
    STATE_UNASSOCIATED,
    STATE_INITIALIZING,
    STATE_ASSOCIATING,
    STATE_ASSOCIATED,
    STATE_SHUFFLING,
    STATE_DISSOCIATING
  };

  enum PolicyAction {
    POLICY_ACTION_MAP,
    POLICY_ACTION_UNMAP,
    POLICY_ACTION_REMOVE
  };

  struct Transition {
    // image map action
    ActionType action_type = ACTION_TYPE_NONE;

    // policy internal action
    boost::optional<PolicyAction> start_policy_action;
    boost::optional<PolicyAction> finish_policy_action;

    // state machine complete
    boost::optional<State> finish_state;

    Transition() {
    }
    Transition(ActionType action_type,
               const boost::optional<PolicyAction>& start_policy_action,
               const boost::optional<PolicyAction>& finish_policy_action,
               const boost::optional<State>& finish_state)
      : action_type(action_type), start_policy_action(start_policy_action),
        finish_policy_action(finish_policy_action), finish_state(finish_state) {
    }
  };

  static bool is_idle(State state) {
    return (state == STATE_UNASSOCIATED || state == STATE_ASSOCIATED);
  }

  static void transit(State state, Transition* transition);

private:
  typedef std::pair<State, ActionType> TransitionKey;
  typedef std::map<TransitionKey, Transition> TransitionTable;

  // image transition table
  static const TransitionTable s_transition_table;
};

std::ostream &operator<<(std::ostream &os, const StateTransition::State &state);
std::ostream &operator<<(std::ostream &os,
                         const StateTransition::PolicyAction &policy_action);

} // namespace image_map
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_STATE_TRANSITION_H
