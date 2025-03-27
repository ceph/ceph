import errno

from typing import Dict

from ...exception import OpSmException
from .subvolume_attrs import SubvolumeTypes, SubvolumeStates, SubvolumeActions

class TransitionKey(object):
    def __init__(self, subvol_type, state, action_type):
        self.transition_key = [subvol_type, state, action_type]

    def __hash__(self):
        return hash(tuple(self.transition_key))

    def __eq__(self, other):
        return self.transition_key == other.transition_key

    def __neq__(self, other):
        return not(self == other)

class SubvolumeOpSm(object):
    transition_table: Dict = {}

    @staticmethod
    def is_complete_state(state):
        if not isinstance(state, SubvolumeStates):
            raise OpSmException(-errno.EINVAL, "unknown state '{0}'".format(state))
        return state == SubvolumeStates.STATE_COMPLETE

    @staticmethod
    def is_failed_state(state):
        if not isinstance(state, SubvolumeStates):
            raise OpSmException(-errno.EINVAL, "unknown state '{0}'".format(state))
        return state == SubvolumeStates.STATE_FAILED or state == SubvolumeStates.STATE_CANCELED

    @staticmethod
    def is_init_state(stm_type, state):
        if not isinstance(state, SubvolumeStates):
            raise OpSmException(-errno.EINVAL, "unknown state '{0}'".format(state))
        return state == SubvolumeOpSm.get_init_state(stm_type)

    @staticmethod
    def get_init_state(stm_type):
        if not isinstance(stm_type, SubvolumeTypes):
            raise OpSmException(-errno.EINVAL, "unknown state machine '{0}'".format(stm_type))
        init_state =  SubvolumeOpSm.transition_table[TransitionKey(stm_type,
                                                     SubvolumeStates.STATE_INIT,
                                                     SubvolumeActions.ACTION_NONE)]
        if not init_state:
            raise OpSmException(-errno.ENOENT, "initial state for state machine '{0}' not found".format(stm_type))
        return init_state

    @staticmethod
    def transition(stm_type, current_state, action):
        if not isinstance(stm_type, SubvolumeTypes):
            raise OpSmException(-errno.EINVAL, "unknown state machine '{0}'".format(stm_type))
        if not isinstance(current_state, SubvolumeStates):
            raise OpSmException(-errno.EINVAL, "unknown state '{0}'".format(current_state))
        if not isinstance(action, SubvolumeActions):
            raise OpSmException(-errno.EINVAL, "unknown action '{0}'".format(action))

        transition = SubvolumeOpSm.transition_table[TransitionKey(stm_type, current_state, action)]
        if not transition:
            raise OpSmException(-errno.EINVAL, "invalid action '{0}' on current state {1} for state machine '{2}'".format(action, current_state, stm_type))

        return transition

SubvolumeOpSm.transition_table = {
    # state transitions for state machine type TYPE_NORMAL
    TransitionKey(SubvolumeTypes.TYPE_NORMAL,
                  SubvolumeStates.STATE_INIT,
                  SubvolumeActions.ACTION_NONE) : SubvolumeStates.STATE_COMPLETE,

    TransitionKey(SubvolumeTypes.TYPE_NORMAL,
                  SubvolumeStates.STATE_COMPLETE,
                  SubvolumeActions.ACTION_RETAINED) : SubvolumeStates.STATE_RETAINED,

    # state transitions for state machine type TYPE_CLONE
    TransitionKey(SubvolumeTypes.TYPE_CLONE,
                  SubvolumeStates.STATE_INIT,
                  SubvolumeActions.ACTION_NONE) : SubvolumeStates.STATE_PENDING,

    TransitionKey(SubvolumeTypes.TYPE_CLONE,
                  SubvolumeStates.STATE_PENDING,
                  SubvolumeActions.ACTION_SUCCESS) : SubvolumeStates.STATE_INPROGRESS,

    TransitionKey(SubvolumeTypes.TYPE_CLONE,
                  SubvolumeStates.STATE_PENDING,
                  SubvolumeActions.ACTION_CANCELLED) : SubvolumeStates.STATE_CANCELED,

    TransitionKey(SubvolumeTypes.TYPE_CLONE,
                  SubvolumeStates.STATE_INPROGRESS,
                  SubvolumeActions.ACTION_SUCCESS) : SubvolumeStates.STATE_COMPLETE,

    TransitionKey(SubvolumeTypes.TYPE_CLONE,
                  SubvolumeStates.STATE_INPROGRESS,
                  SubvolumeActions.ACTION_CANCELLED) : SubvolumeStates.STATE_CANCELED,

    TransitionKey(SubvolumeTypes.TYPE_CLONE,
                  SubvolumeStates.STATE_INPROGRESS,
                  SubvolumeActions.ACTION_FAILED) : SubvolumeStates.STATE_FAILED,

    TransitionKey(SubvolumeTypes.TYPE_CLONE,
                  SubvolumeStates.STATE_COMPLETE,
                  SubvolumeActions.ACTION_RETAINED) : SubvolumeStates.STATE_RETAINED,

    TransitionKey(SubvolumeTypes.TYPE_CLONE,
                  SubvolumeStates.STATE_CANCELED,
                  SubvolumeActions.ACTION_RETAINED) : SubvolumeStates.STATE_RETAINED,

    TransitionKey(SubvolumeTypes.TYPE_CLONE,
                  SubvolumeStates.STATE_FAILED,
                  SubvolumeActions.ACTION_RETAINED) : SubvolumeStates.STATE_RETAINED,
}
