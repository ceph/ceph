# oob.py - container and daemon arguments for out-of-band transports

import os

from typing import Any, Dict, List, Optional

from .exceptions import Error


ALL_DEVICES = 'all'

# where the host's devices are found, for tests to change
SYSFS = '/sys'
DEVROOT = '/dev'


def rdma_uverbs_devices(sysfs: Optional[str] = None) -> Dict[str, str]:
    """Map the name of each RDMA device on the host (mlx5_0, irdma0, ...) to
    the uverbs character device that opens it.
    """
    base = os.path.join(sysfs or SYSFS, 'class', 'infiniband_verbs')
    try:
        entries = sorted(os.listdir(base))
    except FileNotFoundError:
        return {}
    devices = {}
    for entry in entries:
        if not entry.startswith('uverbs'):
            continue
        try:
            with open(os.path.join(base, entry, 'ibdev')) as f:
                name = f.read().strip()
        except OSError:
            continue
        if name:
            devices[name] = f'/dev/infiniband/{entry}'
    return devices


def oob_container_args(
    oob: Dict[str, Any],
    privileged: bool,
    sysfs: Optional[str] = None,
    devroot: Optional[str] = None,
) -> List[str]:
    """Container arguments for a daemon whose deployment carries the oob
    config that the mgr generates (see ceph.cephadm.oob_types.OOBSpec):
    a locked memory limit, the named RDMA devices and the RDMA connection
    manager, and CAP_NET_RAW when raw_sockets is set.

    A privileged container sees every device and holds every capability
    already, so it gets the memory limit only.
    """
    args = []
    memlock = oob.get('memlock')
    if memlock is not None:
        args.append(f'--ulimit=memlock={int(memlock)}:{int(memlock)}')
    if privileged:
        return args

    wanted = oob.get('rdma_devices')
    if wanted:
        have = rdma_uverbs_devices(sysfs)
        names = sorted(have) if wanted == ALL_DEVICES else list(wanted)
        if not names:
            raise Error('oob: no RDMA devices found on this host')
        missing = [n for n in names if n not in have]
        if missing:
            raise Error(
                f'oob: RDMA device(s) {", ".join(missing)} not found on this '
                f'host (found: {", ".join(sorted(have)) or "none"})'
            )
        for name in names:
            args.append(f'--device={have[name]}')
        # the verbs provider sets up its connections through it
        rdma_cm = os.path.join(devroot or DEVROOT, 'infiniband', 'rdma_cm')
        if not os.path.exists(rdma_cm):
            raise Error(
                f'oob: {rdma_cm} not found on this host (load the rdma_ucm '
                'kernel module)'
            )
        args.append('--device=/dev/infiniband/rdma_cm')

    if oob.get('raw_sockets'):
        args.append('--cap-add=NET_RAW')
    return args


def oob_process_args(oob: Dict[str, Any], daemon_type: str) -> List[str]:
    """Daemon arguments for a daemon whose deployment carries an oob config.

    An OSD switches to the ceph user before it reads the monitor
    configuration, so it cannot see there that uet needs CAP_NET_RAW. The
    command line tells it to keep that capability.
    """
    if daemon_type == 'osd' and oob.get('raw_sockets'):
        return ['--oob-keep-net-raw=true']
    return []
