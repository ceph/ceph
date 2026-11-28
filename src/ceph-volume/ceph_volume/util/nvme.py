import logging
import os
import fcntl
import re
import struct
import ctypes
from typing import NamedTuple

from ceph_volume import process, terminal
from ceph_volume.util import disk, nvme_sysfs

logger = logging.getLogger(__name__)


class FcmDedupInfo(NamedTuple):
    """FCM dedup capability information from NVMe Identify Controller."""
    supported: bool
    slot_size_bytes: int
    dvs: int
    lba_size_bytes: int


# NVMe ioctl constant: _IOWR('N', 0x41, struct nvme_passthru_cmd)
# Encodes direction=read+write, magic='N', nr=0x41, size=72 bytes
NVME_IOCTL_ADMIN_CMD = 0xC0484E41
NVME_ADMIN_IDENTIFY = 0x06


class _NvmePassthruCmd(ctypes.Structure):
    """
    struct nvme_passthru_cmd from linux/nvme_ioctl.h.

    Must match the kernel layout exactly: 72 bytes, no padding needed because
    all fields are naturally aligned (uint64 fields at offset 16 and 24 are
    both 8-byte aligned).

    Using ctypes.Structure (not struct.pack) so that fcntl.ioctl can write
    the kernel response back into the object in-place.
    """
    _fields_ = [
        ('opcode',       ctypes.c_uint8),
        ('flags',        ctypes.c_uint8),
        ('rsvd1',        ctypes.c_uint16),
        ('nsid',         ctypes.c_uint32),
        ('cdw2',         ctypes.c_uint32),
        ('cdw3',         ctypes.c_uint32),
        ('metadata',     ctypes.c_uint64),
        ('addr',         ctypes.c_uint64),
        ('metadata_len', ctypes.c_uint32),
        ('data_len',     ctypes.c_uint32),
        ('cdw10',        ctypes.c_uint32),
        ('cdw11',        ctypes.c_uint32),
        ('cdw12',        ctypes.c_uint32),
        ('cdw13',        ctypes.c_uint32),
        ('cdw14',        ctypes.c_uint32),
        ('cdw15',        ctypes.c_uint32),
        ('timeout_ms',   ctypes.c_uint32),
        ('result',       ctypes.c_uint32),   # output: NVMe completion dword 0
    ]


def _nvme_identify(fd: int, nsid: int, cns: int) -> bytes:
    """
    Issue an NVMe Identify command and return the 4096-byte response.

    :param fd:   open file descriptor to an NVMe character device (/dev/nvme0)
                 or namespace block device (/dev/nvme0n1)
    :param nsid: namespace ID (0 for controller identify, 1 for namespace identify)
    :param cns:  CNS field in CDW10 (0x01 = Identify Controller, 0x00 = Identify Namespace)
    :raises OSError: if the ioctl fails
    """
    resp = (ctypes.c_char * 4096)()
    cmd = _NvmePassthruCmd()
    cmd.opcode   = NVME_ADMIN_IDENTIFY
    cmd.nsid     = nsid
    cmd.addr     = ctypes.addressof(resp)
    cmd.data_len = 4096
    cmd.cdw10    = cns
    fcntl.ioctl(fd, NVME_IOCTL_ADMIN_CMD, cmd)
    return bytes(resp)


def _align_up(value: int, alignment: int) -> int:
    """Round value up to the next multiple of alignment."""
    return ((value + alignment - 1) // alignment) * alignment


def fcm_dedup_info(device: str) -> FcmDedupInfo:
    """
    Probe an NVMe device for FCM dedup capability via kernel ioctl.

    Issues two NVMe Identify commands:
      1. Identify Controller (CNS=0x01, nsid=0) on the controller character
         device (/dev/nvmeX) to read FCM fields:
           - DS  (bytes 3408–3409): dedup Supported
           - DSS (byte  3410):      dedup Slot Size as power-of-2 (typically 14 → 16 KiB)
           - DVS (bytes 3412–3413): dedup Version (0 = not supported)
      2. Identify Namespace (CNS=0x00, nsid=1) on the namespace device
         (/dev/nvmeXnY) to read the active LBA format size from FLBAS/LBAF.

    FCM capability matrix:
        DS == 0 AND DVS == 0  →  not FCM-capable
        DS == 1 AND DVS < 4   →  FCM drive too old; log error, treat as non-FCM
        DS == 1 AND DVS >= 4  →  supported

    Returns FcmDedupInfo(supported=False) for non-NVMe devices or any device
    that is not FCM-capable. All ioctl errors are caught and logged.
    """
    if not device:
        raise ValueError('device path is required')

    # Non-NVMe devices: return not-supported immediately
    if not device.startswith('/dev/nvme'):
        return FcmDedupInfo(False, 0, 0, 0)

    # Derive controller character device: /dev/nvme0n1 -> /dev/nvme0
    match = re.match(r'^(/dev/nvme\d+)(?:n\d+)?$', device)
    if not match:
        logger.debug('Cannot parse NVMe device path %s', device)
        return FcmDedupInfo(False, 0, 0, 0)
    ctrl_dev = match.group(1)

    try:
        # Ioctl 1: Identify Controller on the controller character device
        fd = os.open(ctrl_dev, os.O_RDONLY | os.O_NONBLOCK)
        try:
            ctrl_data = _nvme_identify(fd, nsid=0, cns=0x01)
        finally:
            os.close(fd)

        # Parse FCM fields from Identify Controller response
        ds  = struct.unpack_from('<H', ctrl_data, 3408)[0]
        dss = ctrl_data[3410]
        dvs = struct.unpack_from('<H', ctrl_data, 3412)[0]

        if ds == 0 and dvs == 0:
            return FcmDedupInfo(False, 0, 0, 0)

        # Sanity check: DSS > 16 would give slot size > 64 KiB.
        # Known FCM hardware uses DSS = 14 (16 KiB) or DSS = 15 (32 KiB).
        # Reject DSS > 16 to catch firmware bugs or struct parsing errors.
        # Raise this limit if future hardware uses 64 KiB+ slots.
        if dss > 16:
            logger.warning('FCM DSS value %d exceeds known maximum of 16 for %s', dss, device)
            return FcmDedupInfo(False, 0, 0, 0)

        # FCM dedup space reservation requires DVS >= 4.
        # Drives reporting a lower version (including the DS=1/DVS=0 spec quirk)
        # are not supported; fall through to standard non-FCM provisioning.
        if dvs < 4:
            logger.error(
                'FCM drive %s reports DVS=%d which is too low to support FCM dedup '
                '(minimum required: DVS=4); treating as non-FCM device',
                device, dvs
            )
            return FcmDedupInfo(False, 0, 0, 0)

        slot_size_bytes = 2 ** dss

        # Ioctl 2: Identify Namespace on the namespace block device to get LBA size
        # device may already be the namespace (nvme0n1) or the controller (nvme0)
        ns_dev = device if re.search(r'n\d+$', device) else device + 'n1'
        fd = os.open(ns_dev, os.O_RDONLY | os.O_NONBLOCK)
        try:
            ns_data = _nvme_identify(fd, nsid=1, cns=0x00)
        finally:
            os.close(fd)

        # Parse active LBA format from Identify Namespace response
        # FLBAS (byte 26): bits 3:0 select the active LBA Format descriptor
        flbas = ns_data[26]
        lba_format_index = flbas & 0x0F
        # Each LBAF descriptor is 4 bytes starting at offset 128.
        # Byte 2 of each descriptor holds LBADS (LBA Data Size as power-of-2).
        lbaf_offset = 128 + lba_format_index * 4
        lbads = ns_data[lbaf_offset + 2]

        if lbads not in (9, 12):
            # 9 → 512 B, 12 → 4096 B; anything else is non-standard
            logger.warning('Non-standard LBADS value %d for %s', lbads, device)

        lba_size_bytes = 2 ** lbads

        logger.info('FCM dedup detected: device=%s DS=%d DVS=%d DSS=%d '
                    'slot_size=%d lba_size=%d',
                    device, ds, dvs, dss, slot_size_bytes, lba_size_bytes)

        return FcmDedupInfo(True, slot_size_bytes, dvs, lba_size_bytes)

    except (OSError, IOError) as e:
        logger.warning('Unable to probe FCM dedup capability for %s: %s', device, e)
        return FcmDedupInfo(False, 0, 0, 0)
    except Exception as e:
        logger.warning('Unexpected error probing FCM dedup for %s: %s', device, e)
        return FcmDedupInfo(False, 0, 0, 0)


def fcm_compute_reservation(size: int, slot_size_bytes: int) -> dict:
    """
    Compute FCM dedup reservation sizes based on Lodestone layout.

    The outer boundary R (usable space) is calculated from the total capacity C
    (requested LV size) using the Lodestone historical formula:
        R + S_old + FP + E = C
        S_old = R * 10%
        FP    = R * 0.1% + 2 GB
        E     = R * 0.05%
        => R = (C - 2 GB) / 1.1015

    The reserved band [R, C) is subdivided into four regions:
    - E (future reserved):  R * 0.05%,       aligned up to 16 KiB
    - FP (fingerprint DB):  R * 0.10% + 2GB, aligned up to 512 KiB
    - M (extra reserved):   R * 0.15% + 2GB, aligned up to 512 KiB
    - S (dedup slots):      remainder (C - R - E - FP - M), aligned down to slot_size

    Args:
        size: Requested LV size in bytes (before reservation)
        slot_size_bytes: FCM slot size from DSS field (typically 16 KiB)

    Returns:
        dict with:
            'total_bytes': int - Total reservation to subtract from LV size
            'regions': dict - Individual aligned region sizes (S, M, FP, E)
    """
    GiB_2 = 2 * 1024 * 1024 * 1024
    KiB_512 = 512 * 1024
    KiB_16 = 16 * 1024

    # Coerce disk.Size or other numeric types to int
    c_bytes = int(getattr(size, 'b', size))

    # Validate minimum size (fingerprint DB must be >= 4 GiB, requires drive >= ~20 GiB)
    MIN_SIZE_GIB = 20
    if c_bytes < MIN_SIZE_GIB * 1024 * 1024 * 1024:
        raise ValueError(
            f'Drive size {c_bytes} bytes too small for FCM dedup '
            f'(minimum {MIN_SIZE_GIB} GiB required)'
        )

    # 1. Calculate usable size R
    r_bytes = int((c_bytes - GiB_2) / 1.1015)

    # 2. Calculate E: R * 0.05%, 16 KiB aligned
    e_raw = int(r_bytes * 0.0005)
    e_aligned = _align_up(e_raw, KiB_16)

    # 3. Calculate FP: R * 0.10% + 2GB, 512 KiB aligned
    fp_raw = int(r_bytes * 0.0010 + GiB_2)
    fp_aligned = _align_up(fp_raw, KiB_512)

    # 4. Calculate M: R * 0.15% + 2GB, 512 KiB aligned
    m_raw = int(r_bytes * 0.0015 + GiB_2)
    m_aligned = _align_up(m_raw, KiB_512)

    # 5. Calculate S: remainder, slot-size aligned (aligned down to whole slots)
    s_raw = c_bytes - r_bytes - e_aligned - fp_aligned - m_aligned
    if s_raw < 0:
        raise ValueError(f'Invalid S region size {s_raw} for drive size {c_bytes}')
    s_aligned = (s_raw // slot_size_bytes) * slot_size_bytes

    total = c_bytes - r_bytes

    return {
        'total_bytes': total,
        'regions': {
            'S': s_aligned,
            'M': m_aligned,
            'FP': fp_aligned,
            'E': e_aligned
        }
    }


def fcm_reservation_lbas(lv_size_bytes: int, reservation_total_bytes: int,
                         lba_size_bytes: int) -> tuple:
    """
    Compute FCM reservation as LBA values.

    Args:
        lv_size_bytes: TOTAL original LV size (before reservation subtraction).
                       This is the full device/partition size that was requested,
                       not the adjusted size after subtracting the reservation.
        reservation_total_bytes: Total reservation size (from fcm_compute_reservation)
        lba_size_bytes: LBA size (512 or 4096)

    Returns:
        (base_lba, size_lba) where:
            base_lba = starting LBA of the reserved region (at end of device)
                     = (lv_size_bytes - reservation_total_bytes) / lba_size_bytes
            size_lba = size of reserved region in LBAs
                     = reservation_total_bytes / lba_size_bytes

    Note: Both divisions produce exact integers (no remainder) because
    reservation_total_bytes is already aligned to lba_size_bytes.

    The reserved region occupies LBAs [base_lba, base_lba + size_lba),
    placed at the end of the device.
    """
    base_lba = (lv_size_bytes - reservation_total_bytes) // lba_size_bytes
    size_lba = reservation_total_bytes // lba_size_bytes
    return (base_lba, size_lba)


def resolve(device: str) -> str:
    """
    Resolve the device path.

    We expect a valid 'device' here. If it's missing, that's a caller bug,
    so fail fast instead.
    """
    if not device:
        raise ValueError('device path is required')
    return os.path.realpath(device)


def is_namespace(resolved_device: str) -> bool:
    """
    Return True if this looks like a whole NVMe namespace we can format.

    We only format whole NVMe devices (e.g. /dev/nvme0n1). Partitions like
    /dev/nvme0n1p1 and non-block-device paths are intentionally skipped.
    """
    if not nvme_sysfs.is_whole_nvme_namespace_name(os.path.basename(resolved_device)):
        return False
    if not disk.is_device(resolved_device):
        # disk.is_device() already excludes partitions
        logger.debug('Skipping NVMe format for non-whole-disk device %s', resolved_device)
        return False
    return True


def format(resolved_device: str) -> bool:
    """
    Best-effort NVMe namespace format.

    Returns True only if `nvme format` succeeds. Otherwise return False and
    fall back to the normal mkfs flow.
    """
    # When ceph-volume runs inside a container, it sets I_AM_IN_A_CONTAINER=1.
    run_on_host = bool(os.environ.get('I_AM_IN_A_CONTAINER', ''))
    command = ['nvme', 'format', resolved_device, '--force']
    logger.info('Formatting NVMe namespace %s prior to ceph-volume mkfs', resolved_device)
    try:
        _, _, rc = process.call(
            command,
            run_on_host=run_on_host,
            show_command=True,
            terminal_verbose=True,
            verbose_on_failure=True,
        )
    except (FileNotFoundError, PermissionError) as exc:
        logger.warning('Unable to execute nvme CLI for %s: %s', resolved_device, exc)
        return False
    if rc != 0:
        logger.warning(
            'nvme format failed for %s (rc=%s); using default mkfs workflow',
            resolved_device,
            rc,
        )
        return False
    terminal.info('nvme format completed for {}'.format(resolved_device))
    return True


def preformat(device: str) -> bool:
    """
    Resolve, validate, then format an NVMe namespace (when applicable).

    This is the main entrypoint used by ceph-volume: it returns True only
    when we actually formatted the device.
    """
    resolved_device = resolve(device)
    if not is_namespace(resolved_device):
        return False
    return format(resolved_device)

