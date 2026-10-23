from enum import Enum
from typing import Optional


class UpgradeType(str, Enum):
    FULL = 'full'
    STAGGERED = 'staggered'
    BOOTSTRAP = 'bootstrap'

    def __str__(self) -> str:
        return self.value


class UpgradeStatus(str, Enum):
    STARTED = 'started'
    STOPPED = 'stopped'
    COMPLETE = 'complete'

    def __str__(self) -> str:
        return self.value


class CephVersionEntry:

    def __init__(
        self,
        version: str,
        upgrade_type: UpgradeType,
        status: UpgradeStatus,
        command_options: Optional[dict],
        config_dump: Optional[list],
    ):
        self.version = version
        self.upgrade_type = upgrade_type
        self.status = status
        self.command_options = command_options
        self.config_dump = config_dump

    def to_json(self) -> dict:
        return {
            'version': self.version,
            'upgrade_type': self.upgrade_type,
            'status': self.status,
            'command_options': self.command_options,
            'config_dump': self.config_dump,
        }
