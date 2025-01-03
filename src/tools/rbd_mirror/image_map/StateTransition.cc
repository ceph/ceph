// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <ostream>
#include "include/ceph_assert.h"
#include "StateTransition.h"

namespace rbd {
namespace mirror {
namespace image_map {

std::ostream &operator<<(std::ostream &os,
                         const StateTransition::State &state) {
  switch(state) {
  case StateTransition::STATE_INITIALIZING:
    os << "INITIALIZING";
    break;
  case StateTransition::STATE_ASSOCIATING:
    os << "ASSOCIATING";
    break;
  case StateTransition::STATE_ASSOCIATED:
    os << "ASSOCIATED";
    break;
  case StateTransition::STATE_SHUFFLING:
    os << "SHUFFLING";
    break;
  case StateTransition::STATE_DISSOCIATING:
    os << "DISSOCIATING";
    break;
  case StateTransition::STATE_UNASSOCIATED:
    os << "UNASSOCIATED";
    break;
  }
  return os;
}

std::ostream &operator<<(std::ostream &os,
                         const StateTransition::PolicyAction &policy_action) {
  switch(policy_action) {
  case StateTransition::POLICY_ACTION_MAP:
    os << "MAP";
    break;
  case StateTransition::POLICY_ACTION_UNMAP:
    os << "UNMAP";
    break;
  case StateTransition::POLICY_ACTION_REMOVE:
    os << "REMOVE";
    break;
  }
  return os;
}

const StateTransition::TransitionTable StateTransition::s_transition_table {
  // state             current_action           Transition
  // ---------------------------------------------------------------------------
  {{STATE_INITIALIZING, ACTION_TYPE_NONE},       {ACTION_TYPE_ACQUIRE, {}, {},
                                                  {}}},
  {{STATE_INITIALIZING, ACTION_TYPE_ACQUIRE},    {ACTION_TYPE_NONE, {}, {},
                                                  {STATE_ASSOCIATED}}},

  {{STATE_ASSOCIATING,  ACTION_TYPE_NONE},       {ACTION_TYPE_MAP_UPDATE,
                                                  {POLICY_ACTION_MAP}, {}, {}}},
  {{STATE_ASSOCIATING,  ACTION_TYPE_MAP_UPDATE}, {ACTION_TYPE_ACQUIRE, {}, {},
                                                  {}}},
  {{STATE_ASSOCIATING,  ACTION_TYPE_ACQUIRE},    {ACTION_TYPE_NONE, {}, {},
                                                  {STATE_ASSOCIATED}}},

  {{STATE_DISSOCIATING, ACTION_TYPE_NONE},       {ACTION_TYPE_RELEASE, {},
                                                 {POLICY_ACTION_UNMAP}, {}}},
  {{STATE_DISSOCIATING, ACTION_TYPE_RELEASE},    {ACTION_TYPE_MAP_REMOVE, {},
                                                  {POLICY_ACTION_REMOVE}, {}}},
  {{STATE_DISSOCIATING, ACTION_TYPE_MAP_REMOVE}, {ACTION_TYPE_NONE, {},
                                                  {}, {STATE_UNASSOCIATED}}},

  {{STATE_SHUFFLING,    ACTION_TYPE_NONE},       {ACTION_TYPE_RELEASE, {},
                                                  {POLICY_ACTION_UNMAP}, {}}},
  {{STATE_SHUFFLING,    ACTION_TYPE_RELEASE},    {ACTION_TYPE_MAP_UPDATE,
                                                  {POLICY_ACTION_MAP}, {}, {}}},
  {{STATE_SHUFFLING,    ACTION_TYPE_MAP_UPDATE}, {ACTION_TYPE_ACQUIRE, {}, {},
                                                  {}}},
  {{STATE_SHUFFLING,    ACTION_TYPE_ACQUIRE},    {ACTION_TYPE_NONE, {}, {},
                                                  {STATE_ASSOCIATED}}}
};

void StateTransition::transit(State state, Transition* transition) {
  auto it = s_transition_table.find({state, transition->action_type});
  ceph_assert(it != s_transition_table.end());

  *transition = it->second;
}

} // namespace image_map
} // namespace mirror
} // namespace rbd
