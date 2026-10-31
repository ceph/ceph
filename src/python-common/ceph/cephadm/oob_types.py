import re
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Union

from ceph.utils import size_to_bytes


class OOBError(ValueError):
    pass


# the default of the oob_memory option
OOB_MEMORY_DEFAULT = 2 * 1024**3

# what a daemon locks beyond oob_memory: the provider's own buffers and
# queues
OOB_MEMLOCK_HEADROOM = 1024**3

ALL_DEVICES = 'all'
UNLIMITED = 'unlimited'

# an RDMA device name as ibv_devices lists it, such as mlx5_0 or irdma0
_DEVICE_RE = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_.-]*$')


@dataclass(frozen=True)
class OOBSpec:
    """
    The oob section of an RGW or OSD spec, which gives the service's
    containers what out-of-band (S3 over RDMA) transports need:

      oob:
        rdma_devices: [mlx5_0, ...] | all   (optional) RDMA devices to pass in
        memlock: 4G | unlimited             (optional) locked memory limit,
                                            default oob_memory plus 1G
        raw_sockets: true | false           (optional) CAP_NET_RAW, for the
                                            uet provider
    """

    rdma_devices: Union[str, List[str], None] = None
    memlock: Union[str, int, None] = None
    raw_sockets: bool = False

    _KNOWN_KEYS = {'rdma_devices', 'memlock', 'raw_sockets'}

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> 'OOBSpec':
        if not isinstance(obj, Mapping):
            raise OOBError(f'oob must be a dict, got {type(obj).__name__}')

        unknown = set(obj.keys()) - cls._KNOWN_KEYS
        if unknown:
            raise OOBError(f'Unknown oob field(s): {sorted(unknown)!r}')

        spec = cls(
            rdma_devices=obj.get('rdma_devices'),
            memlock=obj.get('memlock'),
            raw_sockets=obj.get('raw_sockets', False),
        )
        spec.validate()
        return spec

    def validate(self) -> None:
        devs = self.rdma_devices
        if devs is not None and devs != ALL_DEVICES:
            if not isinstance(devs, list) or not devs:
                raise OOBError(
                    '"oob.rdma_devices" must be "all" or a non-empty list '
                    'of RDMA device names'
                )
            for d in devs:
                if not isinstance(d, str) or not _DEVICE_RE.match(d):
                    raise OOBError(
                        f'Invalid RDMA device name {d!r} in oob.rdma_devices '
                        '(expected a name that ibv_devices lists, such as '
                        'mlx5_0)'
                    )

        if self.memlock is not None and self.memlock != UNLIMITED:
            if isinstance(self.memlock, bool):
                raise OOBError(
                    '"oob.memlock" must be a size such as 4G, or "unlimited"'
                )
            try:
                n = size_to_bytes(self.memlock)
            except ValueError as e:
                raise OOBError(f'"oob.memlock": {e}')
            if n <= 0:
                raise OOBError('"oob.memlock" must be more than zero')

        if not isinstance(self.raw_sockets, bool):
            raise OOBError('"oob.raw_sockets" must be true or false')

    def memlock_bytes(self, oob_memory: Optional[int]) -> int:
        """
        The locked memory limit of a daemon whose oob_memory is the given
        value (None: the option's default), or -1 for no limit.
        """
        if self.memlock == UNLIMITED:
            return -1
        if self.memlock is not None:
            return size_to_bytes(self.memlock)
        return (oob_memory or OOB_MEMORY_DEFAULT) + OOB_MEMLOCK_HEADROOM

    def container_config(self, oob_memory: Optional[int]) -> Dict[str, Any]:
        """
        The oob config that the mgr sends with a daemon's deployment, which
        cephadm turns into container arguments on the host.
        """
        out: Dict[str, Any] = {
            'memlock': self.memlock_bytes(oob_memory),
            'raw_sockets': self.raw_sockets,
        }
        if self.rdma_devices is not None:
            out['rdma_devices'] = self.rdma_devices
        return out
