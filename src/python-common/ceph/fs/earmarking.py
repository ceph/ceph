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
from typing import List, NamedTuple, Optional, Tuple, Protocol

log = logging.getLogger(__name__)

XATTR_SUBVOLUME_EARMARK_NAME = 'user.ceph.subvolume.earmark'


class FSOperations(Protocol):
    """Protocol class representing the file system operations earmarking
    classes will perform.
    """

    def setxattr(
        self, path: str, key: str, value: bytes, flags: int
    ) -> None: ...

    def getxattr(self, path: str, key: str) -> bytes: ...


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


class EarmarkContents(NamedTuple):
    top: 'EarmarkTopScope'
    subsections: List[str]


class EarmarkParseError(ValueError):
    pass


class CephFSVolumeEarmarking:
    def __init__(self, fs: FSOperations, path: str) -> None:
        self.fs = fs
        self.path = path

    def _handle_cephfs_error(self, e: Exception, action: str) -> Optional[str]:
        if isinstance(e, ValueError):
            raise EarmarkException(errno.EINVAL, f"Invalid earmark specified: {e}") from e
        elif isinstance(e, OSError):
            if e.errno == errno.ENODATA:
                # Return empty string when earmark is not set
                log.info(f"No earmark set for the path while {action}. Returning empty result.")
                return ''
            else:
                log.error(f"Error {action} earmark: {e}")
                raise EarmarkException(-e.errno, e.strerror) from e
        else:
            log.error(f"Unexpected error {action} earmark: {e}")
            raise EarmarkException(errno.EFAULT, f"Unexpected error {action} earmark: {e}") from e

    @staticmethod
    def parse_earmark(value: str) -> Optional[EarmarkContents]:
        """
        Parse an earmark value. Returns None if the value is an empty string.
        Raises EarmarkParseError if the top-level scope is not valid or the earmark
        string is not properly structured.
        Returns an EarmarkContents for valid earmark values.

        :param value: The earmark string to parse.
        :return: An EarmarkContents instance if valid, None if empty.
        """
        if not value:
            return None

        parts = value.split('.')

        # Check if the top-level scope is valid
        if parts[0] not in (scope.value for scope in EarmarkTopScope):
            raise EarmarkParseError(f"Invalid top-level scope: {parts[0]}")

        # Check if all parts are non-empty to ensure valid dot-separated format
        if not all(parts):
            raise EarmarkParseError("Earmark contains empty sections.")

        # Return parsed earmark with top scope and subsections
        return EarmarkContents(top=EarmarkTopScope(parts[0]), subsections=parts[1:])

    def _validate_earmark(self, earmark: str) -> bool:
        """
        Validates the earmark string further by checking specific conditions for scopes like 'smb'.

        :param earmark: The earmark string to validate.
        :return: True if valid, False otherwise.
        """
        try:
            parsed = self.parse_earmark(earmark)
        except EarmarkParseError:
            return False

        # If parsed is None, it's considered valid since the earmark is empty
        if not parsed:
            return True

        # Specific validation for 'smb' scope
        if parsed.top == EarmarkTopScope.SMB:
            # Valid formats: 'smb' or 'smb.cluster.{cluster_id}'
            if not (len(parsed.subsections) == 0 or
                    (len(parsed.subsections) == 2 and
                    parsed.subsections[0] == 'cluster' and parsed.subsections[1])):
                return False

        return True

    def get_earmark(self) -> Optional[str]:
        try:
            earmark_value = (
                self.fs.getxattr(self.path, XATTR_SUBVOLUME_EARMARK_NAME)
                .decode('utf-8')
            )
            return earmark_value
        except Exception as e:
            return self._handle_cephfs_error(e, "getting")

    def set_earmark(self, earmark: str) -> None:
        # Validate the earmark before attempting to set it
        if not self._validate_earmark(earmark):
            raise EarmarkException(
                errno.EINVAL,
                f"Invalid earmark specified: '{earmark}'. "
                "A valid earmark should either be empty or start with 'nfs' or 'smb', "
                "followed by dot-separated non-empty components or simply set "
                "'smb.cluster.{cluster_id}' for the smb intra-cluster scope."
                )

        try:
            self.fs.setxattr(self.path, XATTR_SUBVOLUME_EARMARK_NAME, earmark.encode('utf-8'), 0)
            log.info(f"Earmark '{earmark}' set on {self.path}.")
        except Exception as e:
            self._handle_cephfs_error(e, "setting")

    def clear_earmark(self) -> None:
        self.set_earmark("")
