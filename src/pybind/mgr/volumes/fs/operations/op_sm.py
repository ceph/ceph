import errno

from ..exception import OpSmException

class OpSm(object):
    INIT_STATE_KEY = 'init'

    FAILED_STATE = 'failed'
    FINAL_STATE  = 'complete'
    CANCEL_STATE = 'canceled'

    OP_SM_SUBVOLUME = {
        INIT_STATE_KEY : FINAL_STATE,
    }

    OP_SM_CLONE = {
        INIT_STATE_KEY : 'pending',
        'pending'           : ('in-progress', (FAILED_STATE, CANCEL_STATE)),
        'in-progress'       : (FINAL_STATE, (FAILED_STATE, CANCEL_STATE)),
    }

    STATE_MACHINES_TYPES = {
        "subvolume" : OP_SM_SUBVOLUME,
        "clone"     : OP_SM_CLONE,
    }

    @staticmethod
    def is_final_state(state):
        return state == OpSm.FINAL_STATE

    @staticmethod
    def is_failed_state(state):
        return state == OpSm.FAILED_STATE or state == OpSm.CANCEL_STATE

    @staticmethod
    def is_init_state(stm_type, state):
        stm = OpSm.STATE_MACHINES_TYPES.get(stm_type, None)
        if not stm:
            raise OpSmException(-errno.ENOENT, "state machine type '{0}' not found".format(stm_type))
        init_state = stm.get(OpSm.INIT_STATE_KEY, None)
        return init_state == state

    @staticmethod
    def get_init_state(stm_type):
        stm = OpSm.STATE_MACHINES_TYPES.get(stm_type, None)
        if not stm:
            raise OpSmException(-errno.ENOENT, "state machine type '{0}' not found".format(stm_type))
        init_state = stm.get(OpSm.INIT_STATE_KEY, None)
        if not init_state:
            raise OpSmException(-errno.ENOENT, "initial state unavailable for state machine '{0}'".format(stm_type))
        return init_state

    @staticmethod
    def get_next_state(stm_type, current_state, ret):
        stm = OpSm.STATE_MACHINES_TYPES.get(stm_type, None)
        if not stm:
            raise OpSmException(-errno.ENOENT, "state machine type '{0}' not found".format(stm_type))
        next_state = stm.get(current_state, None)
        if not next_state:
            raise OpSmException(-errno.EINVAL, "invalid current state '{0}'".format(current_state))
        if ret == 0:
            return next_state[0]
        elif ret == -errno.EINTR:
            return next_state[1][1]
        else:
            return next_state[1][0]
