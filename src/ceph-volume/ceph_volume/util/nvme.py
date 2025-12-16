import logging
import os
from typing import Optional

from ceph_volume import process, terminal
from ceph_volume.util import disk

logger = logging.getLogger(__name__)


def _resolve_device_path(device: Optional[str]) -> str:
    if not device:
        return ''
    return os.path.realpath(device)


def _is_nvme_namespace(device: str) -> bool:
    """
    Return True when the provided device path refers to a full NVMe namespace
    that can be formatted directly with nvme-cli.
    """
    if not device:
        return False

    resolved = _resolve_device_path(device)
    if not resolved.startswith('/dev/nvme'):
        return False

    if disk.is_partition(resolved):
        logger.info('Skipping NVMe format for partition %s', resolved)
        return False

    if not disk.is_device(resolved):
        logger.info('Skipping NVMe format for non-block device %s', resolved)
        return False

    return True


def preformat_namespace(device: Optional[str]) -> bool:
    """
    Run an NVMe format on the provided device path if it is an NVMe namespace.

    Returns True when the namespace was formatted successfully, False otherwise.
    """
    resolved = _resolve_device_path(device)
    if not _is_nvme_namespace(resolved):
        return False

    # When ceph-volume runs inside a container, it sets I_AM_IN_A_CONTAINER=1
    run_on_host = bool(os.environ.get('I_AM_IN_A_CONTAINER', False))
    command = ['nvme', 'format', resolved, '--force']
    logger.info('Formatting NVMe namespace %s prior to ceph-volume mkfs', resolved)
    try:
        _, _, rc = process.call(
            command,
            run_on_host=run_on_host,
            show_command=True,
            terminal_verbose=True,
            verbose_on_failure=True
        )
    except OSError as exc:
        logger.warning('Unable to execute nvme CLI for %s: %s', resolved, exc)
        return False

    if rc != 0:
        logger.warning('nvme format failed for %s (rc=%s); using default mkfs workflow', resolved, rc)
        return False

    terminal.info('nvme format completed for {}'.format(resolved))
    return True

