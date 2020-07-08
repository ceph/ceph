import errno
from enum import Enum, unique

from ...exception import VolumeException

@unique
class SubvolumeTypes(Enum):
    TYPE_NORMAL  = "subvolume"
    TYPE_CLONE   = "clone"

    @staticmethod
    def from_value(value):
        if value == "subvolume":
            return SubvolumeTypes.TYPE_NORMAL
        if value == "clone":
            return SubvolumeTypes.TYPE_CLONE

        raise VolumeException(-errno.EINVAL, "invalid subvolume type '{0}'".format(value))

@unique
class SubvolumeStates(Enum):
    STATE_INIT          = 'init'
    STATE_PENDING       = 'pending'
    STATE_INPROGRESS    = 'in-progress'
    STATE_FAILED        = 'failed'
    STATE_COMPLETE      = 'complete'
    STATE_CANCELED      = 'canceled'
    STATE_RETAINED      = 'snapshot-retained'

    @staticmethod
    def from_value(value):
        if value == "init":
            return SubvolumeStates.STATE_INIT
        if value == "pending":
            return SubvolumeStates.STATE_PENDING
        if value == "in-progress":
            return SubvolumeStates.STATE_INPROGRESS
        if value == "failed":
            return SubvolumeStates.STATE_FAILED
        if value == "complete":
            return SubvolumeStates.STATE_COMPLETE
        if value == "canceled":
            return SubvolumeStates.STATE_CANCELED
        if value == "snapshot-retained":
            return SubvolumeStates.STATE_RETAINED

        raise VolumeException(-errno.EINVAL, "invalid state '{0}'".format(value))

@unique
class SubvolumeActions(Enum):
    ACTION_NONE         = 0
    ACTION_SUCCESS      = 1
    ACTION_FAILED       = 2
    ACTION_CANCELLED    = 3
    ACTION_RETAINED     = 4

@unique
class SubvolumeFeatures(Enum):
    FEATURE_SNAPSHOT_CLONE          = "snapshot-clone"
    FEATURE_SNAPSHOT_RETENTION      = "snapshot-retention"
    FEATURE_SNAPSHOT_AUTOPROTECT    = "snapshot-autoprotect"
