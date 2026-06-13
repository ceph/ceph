from . import lvm
from . import raw
from .seastore_lvm import SeastoreLvm
from .seastore_raw import SeastoreRaw
from typing import Any, Dict
from enum import Enum


class ObjectStore(str, Enum):
    bluestore: str = 'bluestore'
    seastore: str = 'seastore'

mapping: Dict[str, Any] = {
    'LVM': {
        ObjectStore.bluestore: lvm.Lvm,
        ObjectStore.seastore: SeastoreLvm,
    },
    'RAW': {
        ObjectStore.bluestore: raw.Raw,
        ObjectStore.seastore: SeastoreRaw,
    }
}
