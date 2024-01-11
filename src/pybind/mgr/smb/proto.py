"""Assorted protocols/interfaces and types used in the smb mgr module."""

from typing import (
    TYPE_CHECKING,
    Any,
    Collection,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    TypeVar,
)

import sys

from ceph.deployment.service_spec import SMBSpec

# this uses a version check as opposed to a try/except because this
# form makes mypy happy and try/except doesn't.
if sys.version_info >= (3, 8):
    from typing import Protocol
elif TYPE_CHECKING:  # pragma: no cover
    # typing_extensions will not be available for the real mgr server
    from typing_extensions import Protocol
else:  # pragma: no cover
    # fallback type that is acceptable to older python on prod. builds
    class Protocol:  # type: ignore
        pass


if sys.version_info >= (3, 11):
    from typing import Self
elif TYPE_CHECKING:  # pragma: no cover
    # typing_extensions will not be available for the real mgr server
    from typing_extensions import Self
else:  # pragma: no cover
    # fallback type that should be ignored at runtime
    Self = Any  # type: ignore


Simplified = Dict[str, Any]
SimplifiedList = List[Simplified]


class Simplifiable(Protocol):
    def to_simplified(self) -> Simplified:
        ...  # pragma: no cover


EntryKey = Tuple[str, str]


class ConfigEntry(Protocol):
    """A protocol for describing a configuration object that can be kept within
    a configuration store. Has the ability to identify itself either by a
    relative key or by a global URI value.
    """

    def get(self) -> Simplified:
        ...  # pragma: no cover

    def set(self, obj: Simplified) -> None:
        ...  # pragma: no cover

    def remove(self) -> bool:
        ...  # pragma: no cover

    def exists(self) -> bool:
        ...  # pragma: no cover

    @property
    def uri(self) -> str:
        ...  # pragma: no cover

    @property
    def full_key(self) -> EntryKey:
        ...  # pragma: no cover


class ConfigStore(Protocol):
    """A protocol for describing a configuration data store capable of
    retaining and tracking configuration entry objects.
    """

    def __getitem__(self, key: EntryKey) -> ConfigEntry:
        ...  # pragma: no cover

    def namespaces(self) -> Collection[str]:
        ...  # pragma: no cover

    def contents(self, ns: str) -> Collection[str]:
        ...  # pragma: no cover

    def __iter__(self) -> Iterator[EntryKey]:
        ...  # pragma: no cover

    def remove(self, ns: EntryKey) -> bool:
        ...  # pragma: no cover


class PathResolver(Protocol):
    """A protocol describing a type that can map volumes, subvolumes, and
    paths to real paths within a cephfs system.
    """

    def resolve(
        self, volume: str, subvolumegroup: str, subvolume: str, path: str
    ) -> str:
        """Return the path within the volume for the given subvolume and path.
        No other checking is performed.
        """
        ...  # pragma: no cover

    def resolve_exists(
        self, volume: str, subvolumegroup: str, subvolume: str, path: str
    ) -> str:
        """Return the path within the volume for the given subvolume and path.
        Raises an exception if the path does not exist or is not a directory.
        """
        ...  # pragma: no cover


class OrchSubmitter(Protocol):
    """A protocol describing a type that can submit a SMBSpec to ceph's
    orchestration system.
    """

    def submit_smb_spec(self, smb_spec: SMBSpec) -> None:
        ...  # pragma: no cover

    def remove_smb_service(self, service_name: str) -> None:
        ...  # pragma: no cover


class MonCommandIssuer(Protocol):
    """A protocol describing the minimal interface that can issue mon commands."""

    def mon_command(
        self, cmd_dict: dict, inbuf: Optional[str] = None
    ) -> Tuple[int, str, str]:
        ...  # pragma: no cover


class AccessAuthorizer(Protocol):
    """A protocol for a type that can requrest cephx caps needed for file
    system access.
    """

    def authorize_entity(
        self, volume: str, entity: str, caps: str = ''
    ) -> None:
        ...  # pragma: no cover


T = TypeVar('T')


# TODO: move to a utils.py
def one(lst: List[T]) -> T:
    if len(lst) != 1:
        raise ValueError("list does not contain exactly one element")
    return lst[0]


class IsNoneError(ValueError):
    pass


def checked(v: Optional[T]) -> T:
    """Ensures the provided value is not a None or raises a IsNoneError.
    Intended use is similar to an `assert v is not None` but more usable in
    one-liners and list/dict/etc comprehensions.
    """
    if v is None:
        raise IsNoneError('value is None')
    return v
