"""Assorted protocols/interfaces and types used in the smb mgr module."""

from typing import (
    TYPE_CHECKING,
    Any,
    Collection,
    ContextManager,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
)

import sys

from ceph.deployment.service_spec import SMBSpec
from ceph.fs.earmarking import EarmarkTopScope

# this uses a version check as opposed to a try/except because this
# form makes mypy happy and try/except doesn't.
if sys.version_info >= (3, 8):  # pragma: no cover
    from typing import Protocol
elif TYPE_CHECKING:  # pragma: no cover
    # typing_extensions will not be available for the real mgr server
    from typing_extensions import Protocol
else:  # pragma: no cover
    # fallback type that is acceptable to older python on prod. builds
    class Protocol:  # type: ignore
        pass


if sys.version_info >= (3, 11):  # pragma: no cover
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
FindParams = Dict[str, Any]


class ResourceKey(Protocol):
    """An object representing a key for a singular object in a store.
    This key may be comprised of one or more input values.
    """

    def __str__(self) -> str:
        ...  # pragma: no cover


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


class ConfigStoreListing(Protocol):
    """A protocol for describing the content-listing methods of a config store."""

    def namespaces(self) -> Collection[str]:
        ...  # pragma: no cover

    def contents(self, ns: str) -> Collection[str]:
        ...  # pragma: no cover

    def __iter__(self) -> Iterator[EntryKey]:
        ...  # pragma: no cover


class ConfigStore(ConfigStoreListing, Protocol):
    """A protocol for describing a configuration data store capable of
    retaining and tracking configuration entry objects.
    """

    def __getitem__(self, key: EntryKey) -> ConfigEntry:
        ...  # pragma: no cover

    def remove(self, ns: EntryKey) -> bool:
        ...  # pragma: no cover


class FindingConfigStore(ConfigStore, Protocol):
    """A protocol for a config store that can more efficiently find
    items within the the store.
    """

    def find_entries(
        self, ns: str, params: FindParams
    ) -> Collection[ConfigEntry]:
        """Find entries in the store matching the given params.
        Params is a dict that will be compared to the same keys/attributes of
        the objects being searched. Only exact matches will be returned.
        """
        ...  # pragma: no cover


class TransactingConfigStore(ConfigStore, Protocol):
    """A protocol for a config store that supports transactions.
    Using the transactions can make using the store more robust or
    efficient.
    """

    def transaction(self) -> ContextManager[None]:
        """Return a context manager that wraps a transaction. What exactly
        this means depends on the store. Typically this would wrap a database
        transaction.
        """
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


class EarmarkResolver(Protocol):
    """A protocol for a type that can resolve earmarks for subvolumes."""

    def get_earmark(self, path: str, volume: str) -> Optional[str]:
        ...  # pragma: no cover

    def set_earmark(self, path: str, volume: str, earmark: str) -> None:
        ...  # pragma: no cover

    def check_earmark(
        self, earmark: str, top_level_scope: EarmarkTopScope
    ) -> bool:
        ...  # pragma: no cover
