from enum import Enum, auto
from .base import MCMAgentBase

class SizeUnit(Enum):
    B = "B"
    KiB = "KiB"
    MiB = "MiB"
    GiB = "GiB"
    TiB = "TiB"
    PiB = "PiB"

class Size(MCMAgentBase):
    def __init__(self, value: float, unit: SizeUnit):
        self.value = value
        self.unit = SizeUnit