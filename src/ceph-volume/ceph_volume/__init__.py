import os
import logging
from dataclasses import dataclass
from typing import Any, Optional

@dataclass
class SysInfo:
    devices: dict[str, list]

sys_info = SysInfo(devices={})
sys_info.devices = dict()

logger = logging.getLogger(__name__)
BEING_REPLACED_HEADER: str = 'CEPH_DEVICE_BEING_REPLACED'


class AllowLoopDevices:
    allow = False
    warned = False

    @classmethod
    def __call__(cls) -> bool:
        val = os.environ.get("CEPH_VOLUME_ALLOW_LOOP_DEVICES", "false").lower()
        if val not in ("false", 'no', '0'):
            cls.allow = True
            if not cls.warned:
                logger.warning(
                    "CEPH_VOLUME_ALLOW_LOOP_DEVICES is set in your "
                    "environment, so we will allow the use of unattached loop"
                    " devices as disks. This feature is intended for "
                    "development purposes only and will never be supported in"
                    " production. Issues filed based on this behavior will "
                    "likely be ignored."
                )
                cls.warned = True
        return cls.allow


class UnloadedConfig(object):
    """
    This class is used as the default value for conf.ceph so that if
    a configuration file is not successfully loaded then it will give
    a nice error message when values from the config are used.
    """
    def __getattr__(self, *a):
        raise RuntimeError("No valid ceph configuration file was loaded.")


allow_loop_devices = AllowLoopDevices()

@dataclass
class Config:
    ceph: Any = UnloadedConfig()
    cluster: Optional[str] = None
    verbosity: Optional[int] = None
    path: Optional[str] = None
    log_path: str = ''
    dmcrypt_no_workqueue: bool = False

conf = Config()

__version__ = "1.0.0"

__release__ = "tentacle"
