# nvme.py - read-only description of a local NVMe namespace

import ctypes
import fcntl
import logging
import os
import re
import struct

from typing import Any, Dict, List

from .exceptions import Error

logger = logging.getLogger()

# NVME_IOCTL_ADMIN_CMD. Identify is an admin command; the I/O command ioctl
# (0xC0484E43) does not serve it.
NVME_IOCTL_ADMIN_CMD = 0xC0484E41

NVME_ADMIN_IDENTIFY = 0x06
CNS_NAMESPACE = 0x00
CNS_CONTROLLER = 0x01

IDENTIFY_BYTES = 4096
DEFAULT_TIMEOUT_MS = 10000

# struct nvme_passthru_cmd from include/uapi/linux/nvme_ioctl.h, 72 bytes.
_PASSTHRU_FORMAT = '<BBHIIIQQIIIIIIIIII'
_PASSTHRU_FIELDS = (
    'opcode', 'flags', 'rsvd1', 'nsid', 'cdw2', 'cdw3', 'metadata', 'addr',
    'metadata_len', 'data_len', 'cdw10', 'cdw11', 'cdw12', 'cdw13', 'cdw14',
    'cdw15', 'timeout_ms', 'result',
)

_NAMESPACE_PATH = re.compile(r'/dev/nvme\d+n\d+')
_CONTROLLER_NAME = re.compile(r'nvme\d+')


def unpack_passthru(raw: bytes) -> Dict[str, int]:
    """Readable form of a packed nvme_passthru_cmd. Used by the tests."""
    return dict(zip(_PASSTHRU_FIELDS, struct.unpack(_PASSTHRU_FORMAT, raw)))


def describe_device(device_path: str) -> Dict[str, Any]:
    """
    Describe one NVMe namespace: its PCI identity and both Identify
    structures, returned raw.

    Nothing here interprets the Identify data; the caller decides what the
    bytes mean.
    """
    if not _NAMESPACE_PATH.fullmatch(device_path):
        raise Error(f'{device_path!r} is not an NVMe namespace path '
                    '(expected /dev/nvmeXnY)')

    namespace = os.path.basename(device_path)

    # A namespace the host does not have is reported as a fact rather than an
    # error. The caller treats an error as "the probe is broken" and aborts OSD
    # creation; no OSD can be created on a device that is not here, so there is
    # nothing to abort. Every other failure still raises.
    if not os.path.isdir(f'/sys/class/block/{namespace}'):
        return {'device': device_path, 'present': False}

    nsid = _read_nsid(namespace)
    vendor, device = _read_pci_ids(namespace)
    return {
        'device': device_path,
        'present': True,
        'pci_vendor': vendor,
        'pci_device': device,
        'nsid': nsid,
        'identify_controller': identify(
            device_path, CNS_CONTROLLER, nsid=0).hex(),
        'identify_namespace': identify(
            device_path, CNS_NAMESPACE, nsid=nsid).hex(),
    }


def identify(device_path: str, cns: int, nsid: int) -> bytes:
    """Issue one Identify admin command and return its 4096-byte response."""
    buf = ctypes.create_string_buffer(IDENTIFY_BYTES)
    cmd = bytearray(struct.pack(
        _PASSTHRU_FORMAT,
        NVME_ADMIN_IDENTIFY,        # opcode
        0,                          # flags
        0,                          # rsvd1
        nsid,
        0, 0,                       # cdw2, cdw3
        0,                          # metadata
        ctypes.addressof(buf),      # addr
        0,                          # metadata_len
        IDENTIFY_BYTES,             # data_len
        cns,                        # cdw10: CNS
        0, 0, 0, 0, 0,              # cdw11..cdw15
        DEFAULT_TIMEOUT_MS,
        0,                          # result
    ))

    try:
        fd = os.open(device_path, os.O_RDONLY)
    except OSError as e:
        raise Error(f'cannot open {device_path}: {e}')
    try:
        # Only host-side failures come back as -1/OSError. When the controller
        # itself completes the command with an error -- unsupported CNS,
        # invalid field, namespace not ready -- the kernel returns the NVMe
        # status as a positive value and leaves the buffer untouched, so an
        # unchecked return means reporting 4096 zero bytes as a good Identify.
        status = fcntl.ioctl(fd, NVME_IOCTL_ADMIN_CMD, cmd)
    except OSError as e:
        raise Error(f'NVMe Identify (CNS {cns}) on {device_path} failed: {e}')
    finally:
        os.close(fd)
    if status:
        raise Error(
            f'NVMe Identify (CNS {cns}) on {device_path} returned status '
            f'0x{status:x}')
    return buf.raw


def _read_nsid(namespace: str) -> int:
    path = f'/sys/class/block/{namespace}/nsid'
    try:
        with open(path) as f:
            return int(f.read().strip())
    except (OSError, ValueError) as e:
        raise Error(f'cannot read the namespace id from {path}: {e}')


def _read_pci_ids(namespace: str) -> List[str]:
    """PCI vendor and device ids of the controller behind this namespace."""
    for directory in _pci_candidates(namespace):
        ids = []
        for field in ('vendor', 'device'):
            try:
                with open(os.path.join(directory, field)) as f:
                    ids.append(f.read().strip())
            except OSError:
                break
        if len(ids) == 2:
            return ids
    raise Error(f'cannot read the PCI ids for {namespace} from sysfs')


def _pci_candidates(namespace: str) -> List[str]:
    """
    Where the controller's PCI directory may sit.

    A plain namespace hangs off its controller. With NVMe multipath the node
    hangs off the subsystem instead, with the controllers (nvme0, nvme1, ...)
    below it, so look one level further down as well.
    """
    base = f'/sys/class/block/{namespace}/device'
    candidates = [os.path.join(base, 'device')]
    try:
        entries = sorted(os.listdir(base))
    except OSError:
        entries = []
    candidates.extend(
        os.path.join(base, entry, 'device')
        for entry in entries if _CONTROLLER_NAME.fullmatch(entry)
    )
    return candidates
