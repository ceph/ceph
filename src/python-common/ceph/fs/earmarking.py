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

import sys
import errno
import enum
import logging

from typing import (
    Any,
    Dict,
    List,
    NamedTuple,
    Optional,
    Protocol,
    TYPE_CHECKING,
    Tuple,
    Type,
    Union,
    cast,
)

if sys.version_info >= (3, 11):  # pragma: no cover
    from typing import Self
elif TYPE_CHECKING:  # pragma: no cover
    from typing_extensions import Self
else:  # pragma: no cover
    # fallback type that should be ignored at runtime
    Self = Any  # type: ignore

log = logging.getLogger(__name__)

XATTR_SUBVOLUME_EARMARK_NAME = 'user.ceph.subvolume.earmark'


class EarmarkTopScope(enum.Enum):
    NFS = "nfs"
    SMB = "smb"


class FSOperations(Protocol):
    """Protocol class representing the file system operations earmarking
    classes will perform.
    """

    def setxattr(
        self, path: str, key: str, value: bytes, flags: int
    ) -> None: ...

    def getxattr(self, path: str, key: str) -> bytes: ...


class EarmarkContents(Protocol):
    @property
    def top(self) -> EarmarkTopScope: ...

    @property
    def subsections(self) -> List[str]: ...

    def __str__(self) -> str: ...

    def upgrades(self, other: 'EarmarkContents') -> bool: ...

    @classmethod
    def parse(cls, value: str) -> Self: ...


class EarmarkError(Exception):
    pass


class EarmarkException(EarmarkError):
    def __init__(self, error_code: int, error_message: str) -> None:
        self.errno = error_code
        self.error_str = error_message

    def to_tuple(self) -> Tuple[int, Optional[str], str]:
        return self.errno, "", self.error_str

    def __str__(self) -> str:
        return f"{self.errno} ({self.error_str})"


class EarmarkParseError(ValueError, EarmarkError):
    pass


class EarmarkConflictError(EarmarkError):
    def __init__(
        self, msg: str, current: Any = None, wanted: Any = None
    ) -> None:
        super().__init__(msg)
        self.current_earmark = current
        self.wanted_earmark = wanted


class NFSEarmark(NamedTuple):
    top: EarmarkTopScope
    # to be backwards compatible we allow freeform subsections in nfs
    # (for now) but we may want to get stricter about this in the
    # future since this is never used in practice
    subsections: List[str]

    def __str__(self) -> str:
        return f'{self.top.value}'

    @classmethod
    def parse(cls, value: str) -> Self:
        parts = value.split('.')
        if parts[0] != EarmarkTopScope.NFS.value:
            raise EarmarkParseError(
                f'wrong top scope for NFS earmark: {value!r}'
            )
        for part in parts[1:]:
            if not part:
                raise EarmarkParseError(
                    f'empty subsection in NFS earmark: {value!r}'
                )
        return cls(EarmarkTopScope.NFS, parts[1:])

    @classmethod
    def default(cls) -> Self:
        return cls(EarmarkTopScope.NFS, [])

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, str):
            try:
                _other = self.parse(other)
            except EarmarkParseError:
                return False
        elif isinstance(other, self.__class__):
            _other = other
        else:
            return NotImplemented
        return (
            self.top is _other.top and self.subsections == _other.subsections
        )

    def upgrades(self, current: EarmarkContents) -> bool:
        if self.top != current.top:
            raise EarmarkConflictError(
                f'earmark has already been set by {current.top.value}',
                current,
                self,
            )
        # No need to upgrade nfs at this time
        return False


class SMBEarmark(NamedTuple):
    top: EarmarkTopScope
    cluster_id: str

    @property
    def subsections(self) -> List[str]:
        return [] if not self.cluster_id else ['cluster', self.cluster_id]

    def __str__(self) -> str:
        earmark = f'{self.top.value}'
        if self.cluster_id:
            earmark = f'{earmark}.cluster.{self.cluster_id}'
        return earmark

    @classmethod
    def parse(cls, value: str) -> Self:
        cid = ''
        parts = value.split('.')
        if parts[0] != EarmarkTopScope.SMB.value:
            raise EarmarkParseError(
                f'wrong top scope for SMB earmark: {value!r}'
            )
        if len(parts) > 3:
            raise EarmarkParseError(
                f'too many subsections for SMB earmark: {value!r}'
            )
        elif len(parts) == 3:
            cflag, cid = parts[1:]
            if cflag != 'cluster' or not cid:
                raise EarmarkParseError(
                    f'invalid subsection in SMB earmark: {value!r}'
                )
        elif len(parts) == 2:
            raise EarmarkParseError(
                f'too few subsections for SMB earmark: {value!r}'
            )
        return cls(EarmarkTopScope.SMB, cid)

    @classmethod
    def from_cluster_id(cls, cluster_id: str) -> Self:
        return cls(EarmarkTopScope.SMB, cluster_id)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, str):
            try:
                _other = self.parse(other)
            except EarmarkParseError:
                return False
        elif isinstance(other, self.__class__):
            _other = other
        else:
            return NotImplemented
        return self.top is _other.top and self.cluster_id == _other.cluster_id

    def upgrades(self, current: EarmarkContents) -> bool:
        if self.top != current.top:
            raise EarmarkConflictError(
                f'earmark has already been set by {current.top.value}',
                current,
                self,
            )
        ce = cast(SMBEarmark, current)
        if not ce.cluster_id:
            return True
        if ce.cluster_id == self.cluster_id:
            return False
        raise EarmarkConflictError(
            f'earmark has already been set by smb cluster {ce.cluster_id}',
            current,
            self,
        )


_earmark_types: Dict[EarmarkTopScope, Type[EarmarkContents]] = {
    EarmarkTopScope.NFS: NFSEarmark,
    EarmarkTopScope.SMB: SMBEarmark,
}


def parse_earmark(value: str) -> Optional[EarmarkContents]:
    """Given an earmark string return an EarmarkContents object from
    parsing the string. If the value is empty return None.
    If the value can not be parsed raise EarmarkParseError.
    """
    if not value:
        return None
    _top = value.split('.', 1)[0]
    try:
        top = EarmarkTopScope(_top)
    except ValueError:
        raise EarmarkParseError(f"Invalid top-level scope: {_top}")
    return _earmark_types[top].parse(value)


class CephFSVolumeEarmarking:
    def __init__(self, fs: FSOperations, path: str) -> None:
        self.fs = fs
        self.path = path

    def _handle_cephfs_error(
        self, e: Exception, action: str
    ) -> Optional[str]:
        if isinstance(e, ValueError):
            raise EarmarkException(
                errno.EINVAL, f"Invalid earmark specified: {e}"
            ) from e
        elif isinstance(e, OSError):
            if e.errno == errno.ENODATA:
                # Return empty string when earmark is not set
                log.info(
                    f"No earmark set for the path while {action}. Returning empty result."
                )
                return ''
            else:
                raise EarmarkException(-e.errno, e.strerror) from e
        else:
            raise EarmarkException(
                errno.EFAULT, f"Unexpected error {action} earmark: {e}"
            ) from e

    def _validate_earmark(self, earmark: Union[str, EarmarkContents]) -> bool:
        """
        Validates the earmark. If the earmark is a string, it will be parsed
        and checked.

        :param earmark: The earmark string to validate.
        :return: True if valid, False otherwise.
        """
        if not isinstance(earmark, str):
            return True
        try:
            parse_earmark(earmark)
        except EarmarkParseError:
            return False
        return True

    def get_earmark(self) -> Optional[str]:
        try:
            earmark_value = self.fs.getxattr(
                self.path, XATTR_SUBVOLUME_EARMARK_NAME
            ).decode('utf-8')
            return earmark_value
        except Exception as e:
            return self._handle_cephfs_error(e, "getting")

    def get_parsed_earmark(self) -> Optional[EarmarkContents]:
        earmark_value = self.get_earmark()
        if earmark_value is None:
            return None
        return parse_earmark(earmark_value)

    def set_earmark(self, earmark: Union[str, EarmarkContents]) -> None:
        # Validate the earmark before attempting to set it
        if not self._validate_earmark(earmark):
            raise EarmarkException(
                errno.EINVAL,
                f"Invalid earmark specified: '{earmark}'. "
                "A valid earmark should either be empty or start with 'nfs' or 'smb', "
                "followed by dot-separated non-empty components or simply set "
                "'smb.cluster.{cluster_id}' for the smb intra-cluster scope.",
            )

        try:
            self.fs.setxattr(
                self.path,
                XATTR_SUBVOLUME_EARMARK_NAME,
                str(earmark).encode('utf-8'),
                0,
            )
            log.info(f"Earmark '{earmark}' set on {self.path}.")
        except Exception as e:
            self._handle_cephfs_error(e, "setting")

    def clear_earmark(self) -> None:
        self.set_earmark("")

    def test_and_set(
        self, earmark: EarmarkContents
    ) -> Tuple[bool, Optional[EarmarkContents]]:
        current = self.get_parsed_earmark()
        if not _upgrade_earmark(current, earmark):
            return False, current
        self.set_earmark(earmark)
        return True, earmark


def _upgrade_earmark(
    current: Optional[EarmarkContents],
    wanted: EarmarkContents,
) -> bool:
    """Given two earmarks, current and wanted, return True if the wanted
    earmark is an "upgrade" from the current earmark.
    If the current earmark is None/falsey then the earmark may be upgraded.
    If the earmarks are equal they do not need to be upgraded (returns false).
    Otherwise, the `upgrades` method of the wanted earmark will be passed
    the current earmark.
    This function will raise a EarmarkConflictError if the earmarks are
    totally incompatible.
    """
    if not current:
        return True
    if current == wanted:
        return False
    return wanted.upgrades(current)
