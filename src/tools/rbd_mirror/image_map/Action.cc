// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <ostream>
#include "include/Context.h"
#include "Action.h"

namespace rbd {
namespace mirror {
namespace image_map {

std::ostream &operator<<(std::ostream &os, const Action &action) {
  os << "[action_type=" << action.get_action_type() << "]";
  return os;
}

Action::Action(StateTransition::ActionType action_type)
  : action_type(action_type) {
}

Action Action::create_add_action(Context *on_update, Context *on_acquire, Context *on_finish) {
  Action action(StateTransition::ACTION_TYPE_ADD);
  action.context_map.emplace(StateTransition::STATE_UPDATE_MAPPING, on_update);
  action.context_map.emplace(StateTransition::STATE_ASSOCIATED, on_acquire);
  action.context_map.emplace(StateTransition::STATE_COMPLETE, on_finish);

  return action;
}

Action Action::create_remove_action(Context *on_release, Context *on_remove, Context *on_finish) {
  Action action(StateTransition::ACTION_TYPE_REMOVE);
  action.context_map.emplace(StateTransition::STATE_DISASSOCIATED, on_release);
  action.context_map.emplace(StateTransition::STATE_REMOVE_MAPPING, on_remove);
  action.context_map.emplace(StateTransition::STATE_COMPLETE, on_finish);

  return action;
}

Action Action::create_shuffle_action(Context *on_release, Context *on_update, Context *on_acquire,
                                     Context *on_finish) {
  Action action(StateTransition::ACTION_TYPE_SHUFFLE);
  action.context_map.emplace(StateTransition::STATE_DISASSOCIATED, on_release);
  action.context_map.emplace(StateTransition::STATE_UPDATE_MAPPING, on_update);
  action.context_map.emplace(StateTransition::STATE_ASSOCIATED, on_acquire);
  action.context_map.emplace(StateTransition::STATE_COMPLETE, on_finish);

  return action;
}

StateTransition::ActionType Action::get_action_type() const {
  return action_type;
}

void Action::execute_state_callback(StateTransition::State state) {
  auto it = context_map.find(state);
  if (it != context_map.end() && it->second != nullptr) {
    it->second->complete(0);
  }
}

void Action::state_callback_complete(StateTransition::State state) {
  auto it = context_map.find(state);
  if (it != context_map.end()) {
    it->second = nullptr;
  }
}

void Action::execute_completion_callback(int r) {
  Context *on_finish = nullptr;

  for (auto &ctx : context_map) {
    Context *on_state = nullptr;
    std::swap(ctx.second, on_state);

    if (ctx.first == StateTransition::STATE_COMPLETE) {
      on_finish = on_state;
    } else if (on_state != nullptr) {
      delete on_state;
    }
  }

  if (on_finish != nullptr) {
    on_finish->complete(r);
  }
}

} // namespace image_map
} // namespace mirror
} // namespace rbd
