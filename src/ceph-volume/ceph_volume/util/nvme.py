import logging
import os
from dataclasses import dataclass, field
from typing import Mapping

from ceph_volume import process, terminal
from ceph_volume.util import disk

logger = logging.getLogger(__name__)


@dataclass
class NvmeNamespaceFormatter:
    env: Mapping[str, str] = field(default_factory=lambda: os.environ)

    def resolve(self, device: str) -> str:
        """
        Resolve the device path.

        We expect a valid 'device' here. If it's missing, that's a caller bug,
        so fail fast instead.
        """
        if not device:
            raise ValueError('device path is required')
        return os.path.realpath(device)

    def is_namespace(self, resolved_device: str) -> bool:
        """
        Return True if this looks like a whole NVMe namespace we can format.

        We only format whole NVMe devices (e.g. /dev/nvme0n1). Partitions like
        /dev/nvme0n1p1 and non-block-device paths are intentionally skipped.
        """
        if not resolved_device.startswith('/dev/nvme'):
            return False
        if disk.is_partition(resolved_device):
            logger.info('Skipping NVMe format for partition %s', resolved_device)
            return False
        if not disk.is_device(resolved_device):
            logger.info('Skipping NVMe format for non-block device %s', resolved_device)
            return False
        return True

    def format(self, resolved_device: str) -> bool:
        """
        Best-effort NVMe namespace format.

        Returns True only if `nvme format` succeeds. Otherwise return False and
        fall back to the normal mkfs flow.
        """
        # When ceph-volume runs inside a container, it sets I_AM_IN_A_CONTAINER=1.
        run_on_host = bool(self.env.get('I_AM_IN_A_CONTAINER', ''))
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

    def preformat(self, device: str) -> bool:
        """
        Resolve, validate, then format an NVMe namespace (when applicable).

        This is the main entrypoint used by ceph-volume: it returns True only
        when we actually formatted the device.
        """
        resolved = self.resolve(device)
        if not self.is_namespace(resolved):
            return False
        return self.format(resolved)


def preformat_namespace(device: str) -> bool:
    """
    Entry point that attempts to NVMe-format the device,
    but only when it appears to be a full NVMe namespace.

    Returns True only when we actually ran `nvme format` successfully.
    Otherwise returns False and the caller continues with the normal workflow.
    """
    return NvmeNamespaceFormatter().preformat(device)

