import logging
from enum import Enum, unique
from typing import Dict

log = logging.getLogger(__name__)

@unique
class State(Enum):
    UNASSOCIATED   = 0
    INITIALIZING   = 1
    ASSOCIATING    = 2
    ASSOCIATED     = 3
    SHUFFLING      = 4
    DISASSOCIATING = 5

@unique
class ActionType(Enum):
    NONE       = 0
    MAP_UPDATE = 1
    MAP_REMOVE = 2
    ACQUIRE    = 3
    RELEASE    = 4

@unique
class PolicyAction(Enum):
    MAP    = 0
    UNMAP  = 1
    REMOVE = 2

class TransitionKey:
    def __init__(self, state, action_type):
        self.transition_key = [state, action_type]

    def __hash__(self):
        return hash(tuple(self.transition_key))

    def __eq__(self, other):
        return self.transition_key == other.transition_key

    def __neq__(self, other):
        return not(self == other)

class Transition:
    def __init__(self, action_type, start_policy_action=None,
                 finish_policy_action=None, final_state=None):
        self.action_type = action_type
        self.start_policy_action = start_policy_action
        self.finish_policy_action = finish_policy_action
        self.final_state = final_state

    def __str__(self):
        return "[action_type={0}, start_policy_action={1}, finish_policy_action={2}, final_state={3}".format(
            self.action_type, self.start_policy_action, self.finish_policy_action, self.final_state)

class StateTransition:
    transition_table = {} # type: Dict[TransitionKey, Transition]

    @staticmethod
    def transit(state, action_type):
        try:
            return StateTransition.transition_table[TransitionKey(state, action_type)]
        except KeyError:
            raise Exception()

    @staticmethod
    def is_idle(state):
        return state in (State.UNASSOCIATED, State.ASSOCIATED)

StateTransition.transition_table = {
    TransitionKey(State.INITIALIZING, ActionType.NONE) : Transition(ActionType.ACQUIRE),
    TransitionKey(State.INITIALIZING, ActionType.ACQUIRE) : Transition(ActionType.NONE,
                                                                       final_state=State.ASSOCIATED),

    TransitionKey(State.ASSOCIATING, ActionType.NONE) : Transition(ActionType.MAP_UPDATE,
                                                                   start_policy_action=PolicyAction.MAP),
    TransitionKey(State.ASSOCIATING, ActionType.MAP_UPDATE) : Transition(ActionType.ACQUIRE),
    TransitionKey(State.ASSOCIATING, ActionType.ACQUIRE) : Transition(ActionType.NONE,
                                                                      final_state=State.ASSOCIATED),

    TransitionKey(State.DISASSOCIATING, ActionType.NONE) : Transition(ActionType.RELEASE,
                                                                      finish_policy_action=PolicyAction.UNMAP),
    TransitionKey(State.DISASSOCIATING, ActionType.RELEASE) : Transition(ActionType.MAP_REMOVE,
                                                                         finish_policy_action=PolicyAction.REMOVE),
    TransitionKey(State.DISASSOCIATING, ActionType.MAP_REMOVE) : Transition(ActionType.NONE,
                                                                            final_state=State.UNASSOCIATED),

    TransitionKey(State.SHUFFLING, ActionType.NONE) : Transition(ActionType.RELEASE,
                                                                 finish_policy_action=PolicyAction.UNMAP),
    TransitionKey(State.SHUFFLING, ActionType.RELEASE) : Transition(ActionType.MAP_UPDATE,
                                                                    start_policy_action=PolicyAction.MAP),
    TransitionKey(State.SHUFFLING, ActionType.MAP_UPDATE) : Transition(ActionType.ACQUIRE),
    TransitionKey(State.SHUFFLING, ActionType.ACQUIRE) : Transition(ActionType.NONE,
                                                                    final_state=State.ASSOCIATED),
    }
