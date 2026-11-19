"""Support for working with the external data stores and the items within.

The external data stores exist specifially to share configuration with
systems outside the mgr module. The naming is simpilfied and the
cluster id acts as a namespace so that external entities can be
granted access to only one cluster at a time.
"""
from typing import Any, Collection, Dict, List, Tuple

import json
import re

from . import config_store
from .proto import ConfigStore, EntryKey

# A config-key entry is limited by the mon option mon_config_key_max_entry_size
# (64KiB by default). Entries holding RGW credentials are chunked to fit.
RGW_STUB_MAX_BYTES = 64 * 1024
# All chunked RGW credential entries are named rgw.<chunk>.json, where chunk
# is a number. The entries are packed full, whatever credential a share uses.
RGW_CHUNK_PREFIX = 'rgw.'
_RGW_CHUNK_RE = re.compile(r'^rgw\.(\d+)\.json$')

# Convert identifiers into config entry keys


def config_key(cluster_id: str, override: bool = False) -> EntryKey:
    """Return key identifying a (cluster) config in an external store."""
    return (cluster_id, 'config.smb.override' if override else 'config.smb')


def cluster_placeholder_key(cluster_id: str) -> EntryKey:
    """Return key identifying a cluster info in an external store."""
    return (cluster_id, 'cluster-info')


def cluster_orchestration_submitted(
    store: ConfigStore, cluster_id: str
) -> bool:
    """Treat unknown orchestration history as submitted."""
    try:
        info = store[cluster_placeholder_key(cluster_id)].get()
    except KeyError:
        return True
    return info.get('orch_submitted') is not False


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


def rgw_config_key(cluster_id: str) -> EntryKey:
    """Return key for the legacy RGW credential stub in the private store.
    Older versions kept the credentials of every share in this one entry. It
    is no longer written but may still exist, and be in use by deployed
    daemons, until the cluster is next updated.
    """
    return (cluster_id, 'config.smb.rgw')


def rgw_chunk_config_key(cluster_id: str, chunk: int) -> EntryKey:
    """Return key for one numbered chunk of the RGW credential stubs in the
    private store.
    """
    return (cluster_id, f'{RGW_CHUNK_PREFIX}{chunk}.json')


def rgw_stub(shares: Dict[str, Any]) -> Dict[str, Any]:
    """Return a sambacc config:merge stub carrying the per-share options."""
    return {
        'samba-container-config': 'v0',
        'config:merge': {'shares': shares},
    }


def chunk_rgw_shares(
    shares: Dict[str, Any], max_bytes: int = RGW_STUB_MAX_BYTES
) -> List[Dict[str, Any]]:
    """Split the per-share options into as few groups as possible that each
    serialize, as a complete stub, to at most max_bytes. Shares are packed in
    the order given, filling each group before starting the next, so the
    caller controls which shares end up together. Sizes are computed from
    the json.dumps encoding that the config stores use.
    """
    base = len(json.dumps(rgw_stub({})))
    chunks: List[Dict[str, Any]] = []
    current: Dict[str, Any] = {}
    size = base
    for name in shares:
        # '"name": {...}' plus the ', ' separating it from a previous item
        item = len(json.dumps(name)) + 2 + len(json.dumps(shares[name]))
        sep = 2 if current else 0
        if current and size + sep + item > max_bytes:
            chunks.append(current)
            current, size, sep = {}, base, 0
        current[name] = shares[name]
        size += sep + item
    if current:
        chunks.append(current)
    return chunks


def stored_rgw_chunk_keys(
    store: ConfigStore, cluster_id: str
) -> List[EntryKey]:
    """Return the keys of all stored RGW credential stub chunks, in chunk
    order.
    """
    found = []
    for name in config_store.find_keys(store, cluster_id, RGW_CHUNK_PREFIX):
        match = _RGW_CHUNK_RE.match(name)
        if match:
            found.append((int(match.group(1)), (cluster_id, name)))
    return [key for _, key in sorted(found)]


def stored_rgw_stub_keys(
    store: ConfigStore, cluster_id: str
) -> List[EntryKey]:
    """Return the keys of every stored stub that must be given to the
    daemons: the legacy stub (if still present) and all credential stub
    chunks.
    """
    keys = stored_rgw_chunk_keys(store, cluster_id)
    legacy = rgw_config_key(cluster_id)
    if store[legacy].exists():
        keys.insert(0, legacy)
    return keys


def tls_credential_key(
    cluster_id: str, tls_credential_id: str, cred_type: str
) -> EntryKey:
    """Return key identifying a TLS credential in an external store."""
    suffix = {
        'cert': 'ssl.crt',
        'key': 'ssl.key',
        'ca-cert': 'ca.crt',
    }[cred_type]
    return (cluster_id, f'{tls_credential_id}.{suffix}')


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
