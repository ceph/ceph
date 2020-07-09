import logging
from enum import Enum, unique
from typing import Dict

log = logging.getLogger(__name__)

@unique
class State(Enum):
    STATE_UNASSOCIATED   = 0
    STATE_INITIALIZING   = 1
    STATE_ASSOCIATING    = 2
    STATE_ASSOCIATED     = 3
    STATE_SHUFFLING      = 4
    STATE_DISASSOCIATING = 5

@unique
class ActionType(Enum):
    ACTION_TYPE_NONE       = 0
    ACTION_TYPE_MAP_UPDATE = 1
    ACTION_TYPE_MAP_REMOVE = 2
    ACTION_TYPE_ACQUIRE    = 3
    ACTION_TYPE_RELEASE    = 4

@unique
class PolicyAction(Enum):
    POLICY_ACTION_MAP    = 0
    POLICY_ACTION_UNMAP  = 1
    POLICY_ACTION_REMOVE = 2

class TransitionKey(object):
    def __init__(self, state, action_type):
        self.transition_key = [state, action_type]

    def __hash__(self):
        return hash(tuple(self.transition_key))

    def __eq__(self, other):
        return self.transition_key == other.transition_key

    def __neq__(self, other):
        return not(self == other)

class Transition(object):
    def __init__(self, action_type, start_policy_action=None,
                 finish_policy_action=None, final_state=None):
        self.action_type = action_type
        self.start_policy_action = start_policy_action
        self.finish_policy_action = finish_policy_action
        self.final_state = final_state

    def __str__(self):
        return "[action_type={0}, start_policy_action={1}, finish_policy_action={2}, final_state={3}".format(
            self.action_type, self.start_policy_action, self.finish_policy_action, self.final_state)

class StateTransition(object):
    transition_table = {} # type: Dict[TransitionKey, Transition]

    @staticmethod
    def transit(state, action_type):
        try:
            return StateTransition.transition_table[TransitionKey(state, action_type)]
        except KeyError:
            raise Exception()

    @staticmethod
    def is_idle(state):
        return state in (State.STATE_UNASSOCIATED, State.STATE_ASSOCIATED)

StateTransition.transition_table = {
    TransitionKey(State.STATE_INITIALIZING, ActionType.ACTION_TYPE_NONE) : Transition(ActionType.ACTION_TYPE_ACQUIRE),
    TransitionKey(State.STATE_INITIALIZING, ActionType.ACTION_TYPE_ACQUIRE) : Transition(ActionType.ACTION_TYPE_NONE,
                                                                                         final_state=State.STATE_ASSOCIATED),

    TransitionKey(State.STATE_ASSOCIATING, ActionType.ACTION_TYPE_NONE) : Transition(ActionType.ACTION_TYPE_MAP_UPDATE,
                                                                                     start_policy_action=PolicyAction.POLICY_ACTION_MAP),
    TransitionKey(State.STATE_ASSOCIATING, ActionType.ACTION_TYPE_MAP_UPDATE) : Transition(ActionType.ACTION_TYPE_ACQUIRE),
    TransitionKey(State.STATE_ASSOCIATING, ActionType.ACTION_TYPE_ACQUIRE) : Transition(ActionType.ACTION_TYPE_NONE,
                                                                                        final_state=State.STATE_ASSOCIATED),

    TransitionKey(State.STATE_DISASSOCIATING, ActionType.ACTION_TYPE_NONE) : Transition(ActionType.ACTION_TYPE_RELEASE,
                                                                                        finish_policy_action=PolicyAction.POLICY_ACTION_UNMAP),
    TransitionKey(State.STATE_DISASSOCIATING, ActionType.ACTION_TYPE_RELEASE) : Transition(ActionType.ACTION_TYPE_MAP_REMOVE,
                                                                                           finish_policy_action=PolicyAction.POLICY_ACTION_REMOVE),
    TransitionKey(State.STATE_DISASSOCIATING, ActionType.ACTION_TYPE_MAP_REMOVE) : Transition(ActionType.ACTION_TYPE_NONE,
                                                                                              final_state=State.STATE_UNASSOCIATED),

    TransitionKey(State.STATE_SHUFFLING, ActionType.ACTION_TYPE_NONE) : Transition(ActionType.ACTION_TYPE_RELEASE,
                                                                                   finish_policy_action=PolicyAction.POLICY_ACTION_UNMAP),
    TransitionKey(State.STATE_SHUFFLING, ActionType.ACTION_TYPE_RELEASE) : Transition(ActionType.ACTION_TYPE_MAP_UPDATE,
                                                                                      start_policy_action=PolicyAction.POLICY_ACTION_MAP),
    TransitionKey(State.STATE_SHUFFLING, ActionType.ACTION_TYPE_MAP_UPDATE) : Transition(ActionType.ACTION_TYPE_ACQUIRE),
    TransitionKey(State.STATE_SHUFFLING, ActionType.ACTION_TYPE_ACQUIRE) : Transition(ActionType.ACTION_TYPE_NONE,
                                                                                      final_state=State.STATE_ASSOCIATED),
    }
