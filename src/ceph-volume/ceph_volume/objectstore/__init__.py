from . import lvm
from . import raw
from typing import Any, Dict
from enum import Enum


class ObjectStore(str, Enum):
    bluestore: str = 'bluestore'
    seastore: str = 'seastore'

mapping: Dict[str, Any] = {
    'LVM': {
        ObjectStore.bluestore: lvm.Lvm,
        ObjectStore.seastore: lvm.Lvm
    },
    'RAW': {
        ObjectStore.bluestore: raw.Raw
    }
}
