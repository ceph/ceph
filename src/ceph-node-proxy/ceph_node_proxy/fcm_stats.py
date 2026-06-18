import ctypes
import fcntl
import glob
import os
import struct
from typing import Optional, TypedDict

from ceph_node_proxy.util import get_logger

SYSFS_BLOCK = "/sys/block"

NVME_ADMIN_OPC_GET_LOG_PAGE = 0x02
NVME_NSID_ALL = 0xFFFFFFFF
FCM_LOG_PAGE_ID = 0xCA
FCM_LOG_PAGE_LEN = 32
FCM_LOG_PAGE_OFFSET = 280
FCM_RATIO_DISPLAY_MIN_LOG_UTIL_PERCENT = 0.05

_IOC_NRBITS = 8
_IOC_TYPEBITS = 8
_IOC_SIZEBITS = 14
_IOC_DIRBITS = 2
_IOC_NRSHIFT = 0
_IOC_TYPESHIFT = _IOC_NRSHIFT + _IOC_NRBITS
_IOC_SIZESHIFT = _IOC_TYPESHIFT + _IOC_TYPEBITS
_IOC_DIRSHIFT = _IOC_SIZESHIFT + _IOC_SIZEBITS


def ioc(dir_: int, type_: str, nr: int, size: int) -> int:
    return (
        (dir_ << _IOC_DIRSHIFT)
        | (ord(type_) << _IOC_TYPESHIFT)
        | (nr << _IOC_NRSHIFT)
        | (size << _IOC_SIZESHIFT)
    )


class _NvmePassthruCmd64(ctypes.Structure):
    _fields_ = [
        ("opcode", ctypes.c_uint8),
        ("flags", ctypes.c_uint8),
        ("rsvd1", ctypes.c_uint16),
        ("nsid", ctypes.c_uint32),
        ("cdw2", ctypes.c_uint32),
        ("cdw3", ctypes.c_uint32),
        ("metadata", ctypes.c_uint64),
        ("addr", ctypes.c_uint64),
        ("metadata_len", ctypes.c_uint32),
        ("data_len", ctypes.c_uint32),
        ("cdw10", ctypes.c_uint32),
        ("cdw11", ctypes.c_uint32),
        ("cdw12", ctypes.c_uint32),
        ("cdw13", ctypes.c_uint32),
        ("cdw14", ctypes.c_uint32),
        ("cdw15", ctypes.c_uint32),
        ("timeout_ms", ctypes.c_uint32),
        ("rsvd2", ctypes.c_uint32),
        ("result", ctypes.c_uint64),
    ]


NVME_IOCTL_ADMIN64_CMD = ioc(
    3, "N", 0x47, ctypes.sizeof(_NvmePassthruCmd64)
)

logger = get_logger(__name__)


class FCMStatsData(TypedDict):
    device: str
    model: str
    serial_number: str
    valid: bool
    phy_size_bytes: int
    phy_util_bytes: int
    log_size_bytes: int
    log_util_bytes: int
    phy_util_percent: float
    log_util_percent: float
    compression_ratio: float
    compression_ratio_str: str
    savings_bytes: int
    compression_ratio_display: str
    savings_display: str
    phy_usage_display: str
    log_usage_display: str
    status: dict[str, str]


def read_sysfs(path: str) -> str:
    try:
        with open(path, encoding="utf-8") as handle:
            return handle.read().strip()
    except OSError:
        return ""


def read_sysfs_block(device: str, attribute: str) -> str:
    return read_sysfs(os.path.join(SYSFS_BLOCK, device, attribute))


def list_nvme_namespace_names() -> list[str]:
    return sorted(
        os.path.basename(path)
        for path in glob.glob(f"{SYSFS_BLOCK}/nvme*")
        if "c" not in os.path.basename(path)
    )


def is_fcm_device(device: str) -> bool:
    model = read_sysfs_block(device, "device/model")
    return "FCM" in model


def query_nvme_log_page(device: str) -> Optional[bytes]:
    device_path = f"/dev/{device}"
    numd = (FCM_LOG_PAGE_LEN // 4) - 1
    numdl = numd & 0xFFFF
    numdu = (numd >> 16) & 0xFFFF

    buffer = (ctypes.c_uint8 * FCM_LOG_PAGE_LEN)()
    command = _NvmePassthruCmd64()
    command.opcode = NVME_ADMIN_OPC_GET_LOG_PAGE
    command.nsid = NVME_NSID_ALL
    command.addr = ctypes.addressof(buffer)
    command.data_len = FCM_LOG_PAGE_LEN
    command.cdw10 = FCM_LOG_PAGE_ID | (numdl << 16)
    command.cdw11 = numdu
    command.cdw12 = FCM_LOG_PAGE_OFFSET & 0xFFFFFFFF
    command.cdw13 = (FCM_LOG_PAGE_OFFSET >> 32) & 0xFFFFFFFF

    fd = os.open(device_path, os.O_RDONLY)
    try:
        fcntl.ioctl(fd, NVME_IOCTL_ADMIN64_CMD, command)
    except OSError as exc:
        logger.debug(
            "NVMe log page 0xCA query failed for device %s: %s",
            device,
            exc,
        )
        return None
    finally:
        os.close(fd)

    if command.result:
        logger.debug(
            "NVMe log page 0xCA returned non-zero status 0x%x for device %s",
            command.result,
            device,
        )
        return None

    return bytes(buffer)


def compression_ratio_str(ratio: float) -> str:
    ratio_text = f"{ratio:.1f}"
    if ratio_text.endswith("0"):
        return f"{int(ratio)}:1"
    return f"{ratio_text}:1"


def human_readable(capacity: int, dec_places: int = 1) -> str:
    suffixes = ["b", "KB", "MB", "GB", "TB", "PB"]
    size = float(capacity)
    unit = suffixes[0]
    for unit in suffixes:
        if size < 1000:
            break
        size /= 1000
    return f"{size:.{dec_places}f} {unit}"


def fcm_usage_display(util_bytes: int, util_percent: float) -> str:
    return f"{human_readable(util_bytes)}({int(util_percent)}%)"


def empty_fcm_display_fields() -> dict[str, str]:
    return {
        "compression_ratio_display": "",
        "savings_display": "",
        "phy_usage_display": "",
        "log_usage_display": "",
    }


def apply_fcm_display_fields(stats: FCMStatsData) -> FCMStatsData:
    display = empty_fcm_display_fields()
    if not stats["valid"]:
        return {**stats, **display}

    display["savings_display"] = human_readable(stats["savings_bytes"])
    display["phy_usage_display"] = fcm_usage_display(
        stats["phy_util_bytes"], stats["phy_util_percent"]
    )
    display["log_usage_display"] = fcm_usage_display(
        stats["log_util_bytes"], stats["log_util_percent"]
    )
    if stats["log_util_percent"] > FCM_RATIO_DISPLAY_MIN_LOG_UTIL_PERCENT:
        display["compression_ratio_display"] = stats["compression_ratio_str"]
    return {**stats, **display}


def read_fcm_stats(device: str) -> FCMStatsData:
    model = read_sysfs_block(device, "device/model")
    serial_number = read_sysfs_block(device, "device/serial")
    invalid = apply_fcm_display_fields({
        "device": device,
        "model": model,
        "serial_number": serial_number,
        "valid": False,
        "phy_size_bytes": 0,
        "phy_util_bytes": 0,
        "log_size_bytes": 0,
        "log_util_bytes": 0,
        "phy_util_percent": 0.0,
        "log_util_percent": 0.0,
        "compression_ratio": 0.0,
        "compression_ratio_str": "",
        "savings_bytes": 0,
        "status": {"health": "Unknown", "state": "Unavailable"},
        "compression_ratio_display": "",
        "savings_display": "",
        "phy_usage_display": "",
        "log_usage_display": "",
    })

    raw_bytes = query_nvme_log_page(device)
    if raw_bytes is None:
        return invalid

    try:
        phy_size_bytes = struct.unpack("<Q", raw_bytes[:8])[0]
        phy_util_bytes = struct.unpack("<Q", raw_bytes[8:16])[0]
        log_size_bytes = struct.unpack("<Q", raw_bytes[16:24])[0]
        log_util_bytes = struct.unpack("<Q", raw_bytes[24:32])[0]
    except struct.error as exc:
        logger.error(
            "Problem unpacking usage stats from log page 0xCA for device %s: %s",
            device,
            exc,
        )
        return invalid

    phy_util_percent = (
        (phy_util_bytes / phy_size_bytes) * 100 if phy_size_bytes else 0.0
    )
    log_util_percent = (
        (log_util_bytes / log_size_bytes) * 100 if log_size_bytes else 0.0
    )
    compression_ratio = (
        log_util_bytes / phy_util_bytes if phy_util_bytes else 0.0
    )
    savings_bytes = log_util_bytes - phy_util_bytes

    return apply_fcm_display_fields({
        "device": device,
        "model": model,
        "serial_number": serial_number,
        "valid": True,
        "phy_size_bytes": phy_size_bytes,
        "phy_util_bytes": phy_util_bytes,
        "log_size_bytes": log_size_bytes,
        "log_util_bytes": log_util_bytes,
        "phy_util_percent": phy_util_percent,
        "log_util_percent": log_util_percent,
        "compression_ratio": compression_ratio,
        "compression_ratio_str": compression_ratio_str(compression_ratio),
        "savings_bytes": savings_bytes,
        "status": {"health": "OK", "state": "Enabled"},
        "compression_ratio_display": "",
        "savings_display": "",
        "phy_usage_display": "",
        "log_usage_display": "",
    })


def collect_fcm_stats() -> dict[str, FCMStatsData]:
    stats: dict[str, FCMStatsData] = {}
    for device in list_nvme_namespace_names():
        if not is_fcm_device(device):
            continue
        stats[device] = read_fcm_stats(device)
    return stats
