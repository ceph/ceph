from . import lvmbluestore
from . import rawbluestore
from . import baseobjectstore
from typing import Any, Dict


mapping: Dict[str, Any] = {
    'LVM': {
        'bluestore': lvmbluestore.LvmBlueStore
    },
    'RAW': {
        'bluestore': rawbluestore.RawBlueStore
    }
}
