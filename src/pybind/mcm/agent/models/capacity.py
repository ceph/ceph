from typing import Tuple
from .size import SizeUnit
from .size import Size
from .base import MCMAgentBase
from dataclasses import dataclass

@dataclass
class Capacity(MCMAgentBase):
    def __init__(self, used_bytes: float, total_bytes: float):
        self.used_bytes = used_bytes
        self.total_bytes = total_bytes
        self.usage = 0
        if total_bytes > 0:
            self.usage = (used_bytes * 100)/total_bytes
        size_converted_val, size_converted_unit = self.convert_size(used_bytes)
        self.used = Size(size_converted_val, size_converted_unit)
        size_converted_val, size_converted_unit = self.convert_size(total_bytes)
        self.total = Size(size_converted_val, size_converted_unit)

    def convert_size(self, size_bytes: float) -> Tuple[float, SizeUnit]:
        if size_bytes < 0:
            raise ValueError("Size must be non-negative")
        units = [
            SizeUnit.B,
            SizeUnit.KiB,
            SizeUnit.MiB,
            SizeUnit.GiB,
            SizeUnit.TiB,
            SizeUnit.PiB,
        ]
        idx = 0
        while size_bytes >= 1024 and idx < len(units) - 1:
            size_bytes /= 1024
            idx += 1
        return round(size_bytes, 2), units[idx]