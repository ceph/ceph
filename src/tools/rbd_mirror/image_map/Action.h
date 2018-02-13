// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_ACTION_H
#define CEPH_RBD_MIRROR_IMAGE_MAP_ACTION_H

#include <map>
#include "StateTransition.h"

class Context;

namespace rbd {
namespace mirror {
namespace image_map {

struct Action {
public:
  static Action create_add_action(Context *on_update, Context *on_acquire, Context *on_finish);
  static Action create_remove_action(Context *on_release, Context *on_remove, Context *on_finish);
  static Action create_shuffle_action(Context *on_release, Context *on_update, Context *on_acquire,
                                      Context *on_finish);

  void execute_state_callback(StateTransition::State state);
  void state_callback_complete(StateTransition::State state);
  void execute_completion_callback(int r);

  StateTransition::ActionType get_action_type() const;

private:
  Action(StateTransition::ActionType action_type);

  StateTransition::ActionType action_type;                 // action type for this action
  std::map<StateTransition::State, Context *> context_map; // map sub action type to context callback
};

std::ostream &operator<<(std::ostream &os, const Action &action);

} // namespace image_map
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_ACTION_H
