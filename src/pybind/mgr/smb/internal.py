"""Support for working with the internal data store and the strucutured
resources that the internal store holds.
"""
from typing import Collection, Tuple, Type, TypeVar

from . import resources
from .enums import AuthMode, ConfigNS, State
from .proto import ConfigEntry, ConfigStore, Self, Simplifiable, one
from .resources import SMBResource
from .results import ErrorResult

T = TypeVar('T')


class ResourceEntry:
    """Base class for resource entry getter/setter objects."""

    namespace: ConfigNS

    def __init__(self, key: str, config_entry: ConfigEntry) -> None:
        self.key = key
        self.config_entry = config_entry

    @property
    def uri(self) -> str:
        return self.config_entry.uri

    def get(self) -> SMBResource:
        return one(resources.load(self.config_entry.get()))

    def get_resource_type(self, cls: Type[T]) -> T:
        obj = self.get()
        assert isinstance(obj, cls)
        return obj

    def set(self, resource: Simplifiable) -> None:
        self.config_entry.set(resource.to_simplified())

    def create_or_update(self, resource: Simplifiable) -> State:
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
        return self.config_entry.remove()


class ClusterEntry(ResourceEntry):
    """Cluster resource getter/setter for the smb internal data store(s)."""

    namespace = ConfigNS.CLUSTERS

    @classmethod
    def from_store(cls, store: ConfigStore, cluster_id: str) -> Self:
        return cls(cluster_id, store[str(cls.namespace), cluster_id])

    @classmethod
    def ids(cls, store: ConfigStore) -> Collection[str]:
        return store.contents(str(cls.namespace))

    def get_cluster(self) -> resources.Cluster:
        return self.get_resource_type(resources.Cluster)

    def create_or_update(self, resource: Simplifiable) -> State:
        assert isinstance(resource, resources.Cluster)
        try:
            previous = self.config_entry.get()
        except KeyError:
            previous = None
        current = resource.to_simplified()
        if current == previous:
            return State.PRESENT
        elif previous is None:
            self.config_entry.set(current)
            return State.CREATED
        # cluster is special in that is has some fields that we do not
        # permit changing.
        prev = getattr(
            resources.Cluster, '_resource_config'
        ).object_from_simplified(previous)
        if resource.auth_mode != prev.auth_mode:
            raise ErrorResult(
                resource,
                'auth_mode value may not be changed',
                status={'existing_auth_mode': prev.auth_mode},
            )
        if resource.auth_mode == AuthMode.ACTIVE_DIRECTORY:
            assert resource.domain_settings
            assert prev.domain_settings
            if resource.domain_settings.realm != prev.domain_settings.realm:
                raise ErrorResult(
                    resource,
                    'domain/realm value may not be changed',
                    status={
                        'existing_domain_realm': prev.domain_settings.realm
                    },
                )
        self.config_entry.set(current)
        return State.UPDATED


class ShareEntry(ResourceEntry):
    """Share resource getter/setter for the smb internal data store(s)."""

    namespace = ConfigNS.SHARES

    @classmethod
    def from_store(
        cls, store: ConfigStore, cluster_id: str, share_id: str
    ) -> Self:
        key = f'{cluster_id}.{share_id}'
        return cls(key, store[str(cls.namespace), key])

    @classmethod
    def ids(cls, store: ConfigStore) -> Collection[Tuple[str, str]]:
        return [_split(k) for k in store.contents(str(cls.namespace))]

    def get_share(self) -> resources.Share:
        return self.get_resource_type(resources.Share)


class JoinAuthEntry(ResourceEntry):
    """JoinAuth resource getter/setter for the smb internal data store(s)."""

    namespace = ConfigNS.JOIN_AUTHS

    @classmethod
    def from_store(cls, store: ConfigStore, auth_id: str) -> Self:
        return cls(auth_id, store[str(cls.namespace), auth_id])

    @classmethod
    def ids(cls, store: ConfigStore) -> Collection[str]:
        return store.contents(str(cls.namespace))

    def get_join_auth(self) -> resources.JoinAuth:
        return self.get_resource_type(resources.JoinAuth)


class UsersAndGroupsEntry(ResourceEntry):
    """UsersAndGroupsEntry resource getter/setter for the smb internal data
    store(s).
    """

    namespace = ConfigNS.USERS_AND_GROUPS

    @classmethod
    def from_store(cls, store: ConfigStore, auth_id: str) -> Self:
        return cls(auth_id, store[str(cls.namespace), auth_id])

    @classmethod
    def ids(cls, store: ConfigStore) -> Collection[str]:
        return store.contents(str(cls.namespace))

    def get_users_and_groups(self) -> resources.UsersAndGroups:
        return self.get_resource_type(resources.UsersAndGroups)


def _split(share_key: str) -> Tuple[str, str]:
    cluster_id, share_id = share_key.split('.', 1)
    return cluster_id, share_id
