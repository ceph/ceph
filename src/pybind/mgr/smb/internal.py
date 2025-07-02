"""Support for working with the internal data store and the strucutured
resources that the internal store holds.
"""
from typing import Collection, Tuple, Type, TypeVar, Union

from . import resources
from .enums import ConfigNS, State
from .proto import (
    ConfigEntry,
    ConfigStore,
    ConfigStoreListing,
    EntryKey,
    ResourceKey,
    Self,
    Simplifiable,
)
from .resources import SMBResource
from .utils import one

T = TypeVar('T')


class ResourceIDKey:
    """A ResourceKey for resources with only one ID."""

    def __init__(self, resource_id: str) -> None:
        self.resource_id = resource_id

    def __str__(self) -> str:
        return self.resource_id


class ChildResourceIDKey:
    """A ResourceKey for resources with both a parent and child ID."""

    def __init__(self, parent_id: str, resource_id: str) -> None:
        self.parent_id = parent_id
        self.resource_id = resource_id
        self._key = f'{parent_id}.{resource_id}'

    def __str__(self) -> str:
        return self._key


class ResourceEntry:
    """Base class for resource entry getter/setter objects."""

    namespace: ConfigNS

    def __init__(self, key: str, config_entry: ConfigEntry) -> None:
        self.key = key
        self.config_entry = config_entry

    @property
    def uri(self) -> str:
        """Return a unique URI for this store entry."""
        return self.config_entry.uri

    def get(self) -> SMBResource:
        """Fetch a single smb resource object from the underlying store."""
        return one(resources.load(self.config_entry.get()))

    def get_resource_type(self, cls: Type[T]) -> T:
        """Fetch an smb resource matching the supplied type from the
        underlying store.
        """
        obj = self.get()
        if not isinstance(obj, cls):
            raise TypeError(f"{obj!r} is not a {cls}")
        return obj

    def set(self, resource: Simplifiable) -> None:
        """Given a serializable resource object, save it to the store."""
        self.config_entry.set(resource.to_simplified())

    def create_or_update(self, resource: Simplifiable) -> State:
        """Given a serializable resource object, save it to the store,
        returning a state value indicating if the object was created
        or updated.
        """
        try:
            previous = self.config_entry.get()
        except KeyError:
            previous = None
        current = resource.to_simplified()
        if current == previous:
            return State.PRESENT
        self.config_entry.set(current)
        return State.CREATED if previous is None else State.UPDATED

    def remove(self) -> bool:
        """Remove an object from the underlying store."""
        return self.config_entry.remove()

    @classmethod
    def ids(
        cls, store: ConfigStoreListing
    ) -> Union[Collection[str], Collection[Tuple[str, str]]]:
        """Return a collection of id values representing all entries
        in a particular namespace within the store.
        """
        raise NotImplementedError()

    @classmethod
    def from_store_by_key(
        cls, store: ConfigStore, key: Union[str, ResourceKey]
    ) -> Self:
        """Return a new resource entry object bound to the given store
        and identified by the given key.
        """
        _key = str(key)
        return cls(_key, store[str(cls.namespace), _key])

    @classmethod
    def to_key(cls, resource: SMBResource) -> ResourceKey:
        """Return a key object uniquely identifiying a resource within a
        particular namespace.
        """
        raise NotImplementedError()


class CommonResourceEntry(ResourceEntry):
    """Common resource getter/setter for objects with a single ID value in the
    smb internal data store(s).
    """

    @classmethod
    def from_store(cls, store: ConfigStore, resource_id: str) -> Self:
        return cls.from_store_by_key(store, resource_id)

    @classmethod
    def ids(cls, store: ConfigStoreListing) -> Collection[str]:
        return store.contents(str(cls.namespace))


class ClusterEntry(CommonResourceEntry):
    """Cluster resource getter/setter for the smb internal data store(s)."""

    namespace = ConfigNS.CLUSTERS
    _for_resource = (resources.Cluster, resources.RemovedCluster)

    @classmethod
    def to_key(cls, resource: SMBResource) -> ResourceKey:
        assert isinstance(resource, cls._for_resource)
        return ResourceIDKey(resource.cluster_id)

    def get_cluster(self) -> resources.Cluster:
        return self.get_resource_type(resources.Cluster)


class ShareEntry(ResourceEntry):
    """Share resource getter/setter for the smb internal data store(s)."""

    namespace = ConfigNS.SHARES
    _for_resource = (resources.Share, resources.RemovedShare)

    @classmethod
    def from_store(
        cls, store: ConfigStore, cluster_id: str, share_id: str
    ) -> Self:
        key = ChildResourceIDKey(cluster_id, share_id)
        return cls.from_store_by_key(store, str(key))

    @classmethod
    def ids(cls, store: ConfigStoreListing) -> Collection[Tuple[str, str]]:
        return [_split(k) for k in store.contents(str(cls.namespace))]

    @classmethod
    def to_key(cls, resource: SMBResource) -> ResourceKey:
        assert isinstance(resource, cls._for_resource)
        return ChildResourceIDKey(resource.cluster_id, resource.share_id)

    def get_share(self) -> resources.Share:
        return self.get_resource_type(resources.Share)


class JoinAuthEntry(CommonResourceEntry):
    """JoinAuth resource getter/setter for the smb internal data store(s)."""

    namespace = ConfigNS.JOIN_AUTHS
    _for_resource = resources.JoinAuth

    @classmethod
    def to_key(cls, resource: SMBResource) -> ResourceKey:
        assert isinstance(resource, cls._for_resource)
        return ResourceIDKey(resource.auth_id)

    def get_join_auth(self) -> resources.JoinAuth:
        return self.get_resource_type(resources.JoinAuth)


class UsersAndGroupsEntry(CommonResourceEntry):
    """UsersAndGroupsEntry resource getter/setter for the smb internal data
    store(s).
    """

    namespace = ConfigNS.USERS_AND_GROUPS
    _for_resource = resources.UsersAndGroups

    @classmethod
    def to_key(cls, resource: SMBResource) -> ResourceKey:
        assert isinstance(resource, cls._for_resource)
        return ResourceIDKey(resource.users_groups_id)

    def get_users_and_groups(self) -> resources.UsersAndGroups:
        return self.get_resource_type(resources.UsersAndGroups)


def _map_resource_entry(
    resource: Union[SMBResource, Type[SMBResource]]
) -> Type[ResourceEntry]:
    rcls = resource if isinstance(resource, type) else type(resource)
    _map = {
        resources.Cluster: ClusterEntry,
        resources.RemovedCluster: ClusterEntry,
        resources.Share: ShareEntry,
        resources.RemovedShare: ShareEntry,
        resources.JoinAuth: JoinAuthEntry,
        resources.UsersAndGroups: UsersAndGroupsEntry,
    }
    try:
        return _map[rcls]
    except KeyError:
        raise TypeError('not a valid smb resource')


def resource_entry(
    store: ConfigStore, resource: SMBResource
) -> ResourceEntry:
    """Return a bound store entry object given a resource object."""
    entry_cls = _map_resource_entry(resource)
    key = entry_cls.to_key(resource)
    return entry_cls.from_store_by_key(store, key)


def resource_key(resource: SMBResource) -> EntryKey:
    """Return a store entry key for an smb resource object."""
    entry_cls = _map_resource_entry(resource)
    key = entry_cls.to_key(resource)
    return str(entry_cls.namespace), str(key)


def _split(share_key: str) -> Tuple[str, str]:
    cluster_id, share_id = share_key.split('.', 1)
    return cluster_id, share_id
