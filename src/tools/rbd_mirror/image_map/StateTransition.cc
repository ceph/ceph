// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <ostream>
#include "include/assert.h"
#include "StateTransition.h"

namespace rbd {
namespace mirror {
namespace image_map {

std::ostream &operator<<(std::ostream &os, const StateTransition::ActionType &action_type) {
  switch (action_type) {
  case StateTransition::ACTION_TYPE_ADD:
    os << "ADD_IMAGE";
    break;
  case StateTransition::ACTION_TYPE_REMOVE:
    os << "REMOVE_IMAGE";
    break;
  case StateTransition::ACTION_TYPE_SHUFFLE:
    os << "SHUFFLE_IMAGE";
    break;
  default:
    os << "UNKNOWN (" << static_cast<uint32_t>(action_type) << ")";
  }

  return os;
}

std::ostream &operator<<(std::ostream &os, const StateTransition::State &state) {
  switch(state) {
  case StateTransition::STATE_UNASSIGNED:
    os << "UNASSIGNED";
    break;
  case StateTransition::STATE_ASSOCIATED:
    os << "ASSOCIATED";
    break;
  case StateTransition::STATE_DISASSOCIATED:
    os << "DISASSOCIATED";
    break;
  case StateTransition::STATE_UPDATE_MAPPING:
    os << "UPDATE_MAPPING";
    break;
  case StateTransition::STATE_REMOVE_MAPPING:
    os << "REMOVE_MAPPING";
    break;
  case StateTransition::STATE_COMPLETE:
    os << "COMPLETE";
    break;
  }

  return os;
}

const StateTransition::TransitionTable StateTransition::transition_table[] = {
  // action_type         current_state                   Transition
  // -------------------------------------------------------------------------------
  ACTION_TYPE_ADD,     STATE_UNASSIGNED,      Transition(STATE_UPDATE_MAPPING),
  ACTION_TYPE_ADD,     STATE_UPDATE_MAPPING,  Transition(STATE_ASSOCIATED, STATE_ASSOCIATED,
                                                         STATE_UNASSIGNED),

  ACTION_TYPE_REMOVE,  STATE_ASSOCIATED,      Transition(STATE_DISASSOCIATED),
  ACTION_TYPE_REMOVE,  STATE_DISASSOCIATED,   Transition(STATE_REMOVE_MAPPING, STATE_UNASSIGNED),

  ACTION_TYPE_SHUFFLE, STATE_ASSOCIATED,      Transition(STATE_DISASSOCIATED),
  ACTION_TYPE_SHUFFLE, STATE_DISASSOCIATED,   Transition(STATE_UPDATE_MAPPING),
  ACTION_TYPE_SHUFFLE, STATE_UPDATE_MAPPING,  Transition(STATE_ASSOCIATED, STATE_ASSOCIATED,
                                                         STATE_DISASSOCIATED),
};

const StateTransition::Transition &StateTransition::transit(ActionType action_type, State state) {
  for (auto const &entry : transition_table) {
    if (entry.action_type == action_type && entry.current_state == state) {
      return entry.transition;
    }
  }

  assert(false);
}

} // namespace image_map
} // namespace mirror
} // namespace rbd
