"""
Module: CephFS Volume Encryption Tag

This module provides the `CephFSVolumeEncryptionTag` class, which is designed to manage encryption tags
of subvolumes within a CephFS filesystem. The encryption tag mechanism allows
administrators to tag specific subvolumes with identifiers that indicate encryption information,
such as a keyid or other itentifier tags.

Key Features:
- **Set Encryption Tag**: Assigns an tag to a subvolume.
- **Get Encryption Tag**: Retrieves the existing tag of a subvolume, if any.
- **Remove Tag**: Removes the tag from a subvolume, making it available for reallocation.
supported top-level scopes.
"""

import errno
import enum
import logging
from typing import Optional, Tuple

log = logging.getLogger(__name__)

XATTR_SUBVOLUME_ENCTAG_NAME = 'user.ceph.subvolume.enctag'


class EncryptionTagException(Exception):
    def __init__(self, error_code: int, error_message: str) -> None:
        self.errno = error_code
        self.error_str = error_message

    def to_tuple(self) -> Tuple[int, Optional[str], str]:
        return self.errno, "", self.error_str

    def __str__(self) -> str:
        return f"{self.errno} ({self.error_str})"


class CephFSVolumeEncryptionTag:
    ENCTAG_MAX = 255

    def __init__(self, fs, path: str) -> None:
        self.fs = fs
        self.path = path

    def _handle_cephfs_error(self, e: Exception, action: str) -> None:
        if isinstance(e, ValueError):
            raise EncryptionTagException(-errno.EINVAL, f"Invalid encryption tag specified: {e}") from e
        elif isinstance(e, OSError):
            log.error(f"Error {action} encryption tag: {e}")
            raise EncryptionTagException(-e.errno, e.strerror) from e
        else:
            log.error(f"Unexpected error {action} encryption tag: {e}")
            raise EncryptionTagException(-errno.EIO, "Unexpected error") from e

    def get_tag(self) -> Optional[str]:
        try:
            enc_tag_value = (
                self.fs.getxattr(self.path, XATTR_SUBVOLUME_ENCTAG_NAME)
                .decode('utf-8')
            )
            return enc_tag_value
        except Exception as e:
            self._handle_cephfs_error(e, "getting")
            return None

    def set_tag(self, enc_tag: str):
        try:
            if len(enc_tag) > self.ENCTAG_MAX:
                raise ValueError(f"length '{len(enc_tag)} > {self.ENCTAG_MAX}'")

            self.fs.setxattr(self.path, XATTR_SUBVOLUME_ENCTAG_NAME, enc_tag.encode('utf-8'), 0)
            log.info(f"Encryption Tag '{enc_tag}' set on {self.path}.")
        except Exception as e:
            self._handle_cephfs_error(e, "setting")

    def clear_tag(self) -> None:
        self.set_tag("")
