"""Support for working with the external data stores and the items within.

The external data stores exist specifially to share configuration with
systems outside the mgr module. The naming is simpilfied and the
cluster id acts as a namespace so that external entities can be
granted access to only one cluster at a time.
"""
from typing import Collection, Tuple

from .proto import ConfigStore, EntryKey

# Convert identifiers into config entry keys


def config_key(cluster_id: str, override: bool = False) -> EntryKey:
    """Return key identifying a (cluster) config in an external store."""
    return (cluster_id, 'config.smb.override' if override else 'config.smb')


def cluster_placeholder_key(cluster_id: str) -> EntryKey:
    """Return key identifying a cluster info in an external store."""
    return (cluster_id, 'cluster-info')


def join_source_key(cluster_id: str, name: str) -> EntryKey:
    """Return key identifying a join source object in an external store."""
    # don't feed "keys" back into this function, it's only for
    # generating the key from an internal name
    assert not name.startswith('join.')
    assert not name.endswith('.json')
    return (cluster_id, f'join.{name}.json')


def users_and_groups_key(cluster_id: str, name: str) -> EntryKey:
    """Return key identifying a users-and-groups object in an external
    store.
    """
    # don't feed "keys" back into this function, it's only for
    # generating the key from an internal name
    assert not name.startswith('users-groups.')
    assert not name.endswith('.json')
    return (cluster_id, f'users-groups.{name}.json')


def spec_backup_key(cluster_id: str) -> EntryKey:
    """Return key identifying a smb service spec backup object in an external
    store.
    """
    return (cluster_id, 'spec.smb')


# Enumerate keys in a store


def stored_join_source_keys(
    store: ConfigStore, cluster_id: str
) -> Collection[str]:
    """Return a collection of names for join source objects in an external
    store.
    """
    return [k for k in store.contents(cluster_id) if k.startswith('join.')]


def stored_cluster_placeholder_keys(
    store: ConfigStore, cluster_id: str
) -> Collection[str]:
    """Return a collection of names for join source objects in an external
    store.
    """
    return [k for k in store.contents(cluster_id) if k == 'cluster-info']


def stored_usergroup_source_keys(
    store: ConfigStore, cluster_id: str
) -> Collection[str]:
    """Return a collection of names for users & groups source objects in an external
    store.
    """
    return [
        k for k in store.contents(cluster_id) if k.startswith('users-groups.')
    ]


def stored_config_keys(
    store: ConfigStore, cluster_id: str
) -> Collection[str]:
    """Return a collection of names for config objects in an external store."""
    return [
        k for k in store.contents(cluster_id) if k.startswith('config.smb')
    ]


def stored_cluster_ids(
    store: ConfigStore, *alt: ConfigStore
) -> Collection[str]:
    """Return a collection of cluster ids present in one or more external
    config stores.
    """
    ns = set(store.namespaces())
    for alt_store in alt:
        ns.update(store.namespaces())
    return ns


# Remove objcts in an external config store


def rm_cluster(store: ConfigStore, cluster_id: str) -> None:
    """Remove all objects belonging to a given cluster id in a config store."""
    knames = set(store.contents(cluster_id))
    for kname in knames:
        store.remove((cluster_id, kname))
    # if the config store needs to remove the (now empty) namespace explicitly
    # it must provide a remove_namespace method.
    rm_namespace = getattr(store, 'remove_namespace', None)
    if rm_namespace:
        rm_namespace(cluster_id)
    # TODO: this can probably be removed once the code has matured
    assert cluster_id not in store.namespaces()


def rm_other_in_ns(
    store: ConfigStore,
    namespace: str,
    expected_keys: Collection[Tuple[str, str]],
) -> None:
    """Remove all objects within a given namespace that do not appear in the
    given collection of expected_keys.
    """
    nskeys = set(key for key in store if key[0] == namespace)
    remove_keys = nskeys - set(expected_keys)
    for key in remove_keys:
        store.remove(key)
