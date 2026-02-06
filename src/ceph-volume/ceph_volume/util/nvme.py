import logging
import os

from ceph_volume import process, terminal
from ceph_volume.util import disk

logger = logging.getLogger(__name__)


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
    if not resolved_device.startswith('/dev/nvme'):
        return False
    if not disk.is_device(resolved_device):
        # disk.is_device() already excludes partitions
        logger.info('Skipping NVMe format for non-whole-disk device %s', resolved_device)
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

