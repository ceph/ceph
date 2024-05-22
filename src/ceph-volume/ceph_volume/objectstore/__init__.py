from . import lvmbluestore
from . import rawbluestore

mapping = {
    'LVM': {
        'bluestore': lvmbluestore.LvmBlueStore
    },
    'RAW': {
        'bluestore': rawbluestore.RawBlueStore
    }
}
