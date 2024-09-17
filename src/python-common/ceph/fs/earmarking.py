"""
Module: CephFS Volume Earmarking

This module provides the `CephFSVolumeEarmarking` class, which is designed to manage the earmarking
of subvolumes within a CephFS filesystem. The earmarking mechanism allows
administrators to tag specific subvolumes with identifiers that indicate their intended use
such as NFS or SMB, ensuring that only one file service is assigned to a particular subvolume
at a time. This is crucial to prevent data corruption in environments where
mixed protocol support (NFS and SMB) is not yet available.

Key Features:
- **Set Earmark**: Assigns an earmark to a subvolume.
- **Get Earmark**: Retrieves the existing earmark of a subvolume, if any.
- **Remove Earmark**: Removes the earmark from a subvolume, making it available for reallocation.
- **Validate Earmark**: Ensures that the earmark follows the correct format and only uses
supported top-level scopes.
"""

import errno
import enum
import logging
from typing import Optional, Tuple

log = logging.getLogger(__name__)

XATTR_SUBVOLUME_EARMARK_NAME = 'user.ceph.subvolume.earmark'


class EarmarkTopScope(enum.Enum):
    NFS = "nfs"
    SMB = "smb"


class EarmarkException(Exception):
    def __init__(self, error_code: int, error_message: str) -> None:
        self.errno = error_code
        self.error_str = error_message

    def to_tuple(self) -> Tuple[int, Optional[str], str]:
        return self.errno, "", self.error_str

    def __str__(self) -> str:
        return f"{self.errno} ({self.error_str})"


class CephFSVolumeEarmarking:
    def __init__(self, fs, path: str) -> None:
        self.fs = fs
        self.path = path

    def _handle_cephfs_error(self, e: Exception, action: str) -> None:
        if isinstance(e, ValueError):
            raise EarmarkException(errno.EINVAL, f"Invalid earmark specified: {e}") from e
        elif isinstance(e, OSError):
            log.error(f"Error {action} earmark: {e}")
            raise EarmarkException(-e.errno, e.strerror) from e
        else:
            log.error(f"Unexpected error {action} earmark: {e}")
            raise EarmarkException(errno.EIO, "Unexpected error") from e

    def _validate_earmark(self, earmark: str) -> bool:
        """
        Validates that the earmark string is either empty or composed of parts separated by scopes,
        with the top-level scope being either 'nfs' or 'smb'.

        :param earmark: The earmark string to validate.
        :return: True if valid, False otherwise.
        """
        if not earmark or earmark in (scope.value for scope in EarmarkTopScope):
            return True

        parts = earmark.split('.')

        if parts[0] not in (scope.value for scope in EarmarkTopScope):
            return False

        # Check if all parts are non-empty (to ensure valid dot-separated format)
        return all(parts)

    def get_earmark(self) -> Optional[str]:
        try:
            earmark_value = (
                self.fs.getxattr(self.path, XATTR_SUBVOLUME_EARMARK_NAME)
                .decode('utf-8')
            )
            return earmark_value
        except Exception as e:
            self._handle_cephfs_error(e, "getting")
            return None

    def set_earmark(self, earmark: str):
        # Validate the earmark before attempting to set it
        if not self._validate_earmark(earmark):
            raise EarmarkException(
                errno.EINVAL,
                f"Invalid earmark specified: '{earmark}'. "
                "A valid earmark should either be empty or start with 'nfs' or 'smb', "
                "followed by dot-separated non-empty components."
                )

        try:
            self.fs.setxattr(self.path, XATTR_SUBVOLUME_EARMARK_NAME, earmark.encode('utf-8'), 0)
            log.info(f"Earmark '{earmark}' set on {self.path}.")
        except Exception as e:
            self._handle_cephfs_error(e, "setting")

    def clear_earmark(self) -> None:
        self.set_earmark("")
