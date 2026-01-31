from typing import List, Dict, Any


class RawBlock():
    def __init__(self, block_name: str, blocks: List['RawBlock'] = [], values: Dict[str, Any] = {}):
        if not values:  # workaround mutable default argument
            values = {}
        if not blocks:  # workaround mutable default argument
            blocks = []
        self.block_name = block_name
        self.blocks = blocks
        self.values = values

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, RawBlock):
            return False
        return self.block_name == other.block_name and \
            self.blocks == other.blocks and \
            self.values == other.values

    def __repr__(self) -> str:
        return f'RawBlock({self.block_name!r}, {self.blocks!r}, {self.values!r})'
