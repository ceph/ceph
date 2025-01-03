import os
import logging
from collections import namedtuple


sys_info = namedtuple('sys_info', ['devices'])
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
conf = namedtuple('config', ['ceph', 'cluster', 'verbosity', 'path', 'log_path', 'dmcrypt_no_workqueue'])
conf.ceph = UnloadedConfig()
conf.dmcrypt_no_workqueue = None

__version__ = "1.0.0"

__release__ = "squid"
