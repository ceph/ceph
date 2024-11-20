from typing import Collection, Dict, Iterator, Optional, cast

import json

from .proto import EntryKey, MonCommandIssuer, Protocol, Simplified


class MgrStoreProtocol(Protocol):
    """A simple protocol describing the minimal per-mgr-module (mon) store interface
    provided by the fairly giganto MgrModule class.
    """

    def get_store(self, key: str) -> Optional[str]:
        ...

    def set_store(self, key: str, val: Optional[str]) -> None:
        ...

    def get_store_prefix(self, key_prefix: str) -> Dict[str, str]:
        ...


def _ksplit(key: str, prefix: str = '') -> EntryKey:
    if prefix and key.startswith(prefix):
        plen = len(prefix)
        key = key[plen:]
    ek = tuple(key.split('/', 1))
    assert len(ek) == 2
    # the cast is needed for older mypy versions, where asserting
    # the length doesn't narrow the type
    return cast(EntryKey, ek)


def _kjoin(key: EntryKey) -> str:
    assert len(key) == 2
    return '/'.join(key)


class ModuleStoreEntry:
    """A store entry for the manager module config store."""

    def __init__(
        self, module_store: 'ModuleConfigStore', key: EntryKey
    ) -> None:
        self._store = module_store
        self._key = key
        self._store_key = self._store.PREFIX + _kjoin(key)

    def set(self, obj: Simplified) -> None:
        """Set the store entry value to that of the serialized value of obj."""
        value = json.dumps(obj)
        self._store._mstore.set_store(self._store_key, value)

    def get(self) -> Simplified:
        """Get the deserialized store entry value."""
        value = self._store._mstore.get_store(self._store_key)
        if value is None:
            raise KeyError(self._key)
        return json.loads(value)

    def remove(self) -> bool:
        """Remove the current entry from the store."""
        return self._store.remove(self.full_key)

    def exists(self) -> bool:
        """Returns true if the entry currently exists within the store."""
        return self._key in set(self._store)

    @property
    def uri(self) -> str:
        """Returns an identifier for the entry within the store."""
        ns, name = self._key
        return f'ceph-smb-resource:{ns}/{name}'

    @property
    def full_key(self) -> EntryKey:
        """Return a namespaced key for the entry."""
        return self._key


class ModuleConfigStore:
    """A store that serves as a layer on top of a mgr module's key/value store.
    Most appropriate for the smb module internal store.

    N.B. This store is ulimately backed by the same data store as the
    MonKeyConfigStore or commands like `ceph config-key ...` commands. The mgr
    C++ code that implements the three functions we use for this class
    automatically prefix the keys we provide with the module in use. These
    functions also cache. The built-in prefixing make this store less
    appropriate for use outside of the mgr module. There's little point in
    caching at this layer - at least not caching the serialized strings -
    because the mgr c++ layer is already doing that.
    """

    PREFIX = 'ceph.smb.resources/'

    def __init__(self, mstore: MgrStoreProtocol):
        self._mstore = mstore

    def __getitem__(self, key: EntryKey) -> ModuleStoreEntry:
        """Return an entry object given a namespaced entry key. This entry does
        not have to exist in the store.
        """
        return ModuleStoreEntry(self, key)

    def remove(self, key: EntryKey) -> bool:
        """Remove an entry from the store. Returns true if an entry was
        present.
        """
        # The Ceph Mgr api uses none as special token to delete the item.
        # Otherwise it only accepts strings to set.
        if key not in self:
            return False
        self._mstore.set_store(self.PREFIX + _kjoin(key), None)
        return True

    def namespaces(self) -> Collection[str]:
        """Return all namespaces currently in the store."""
        return {k[0] for k in self}

    def contents(self, ns: str) -> Collection[str]:
        """Return all subkeys currently in the namespace."""
        return [k[1] for k in self if k[0] == ns]

    def __iter__(self) -> Iterator[EntryKey]:
        """Iterate over all namespaced keys currently in the store."""
        for k in self._mstore.get_store_prefix(self.PREFIX).keys():
            yield _ksplit(k, prefix=self.PREFIX)


class MonKeyStoreEntry:
    """A config store entry for items in the global ceph mon config-key store."""

    def __init__(
        self, mon_key_store: 'MonKeyConfigStore', key: EntryKey
    ) -> None:
        self._store = mon_key_store
        self._key = key
        self._store_key = self._store.PREFIX + _kjoin(key)

    def set(self, obj: Simplified) -> None:
        """Set the store entry value to that of the serialized value of obj."""
        self._store._set_val(self._key, json.dumps(obj))

    def get(self) -> Simplified:
        """Get the deserialized store entry value."""
        return json.loads(self._store._get_val(self._key))

    def remove(self) -> bool:
        """Remove the current entry from the store."""
        return self._store.remove(self.full_key)

    def exists(self) -> bool:
        """Returns true if the entry currently exists within the store."""
        return self._key in self._store

    @property
    def uri(self) -> str:
        """Returns an identifier for the entry within the store."""
        # The rados:mon-config-key pseudo scheme is made up for the
        # purposes of communicating a key using the URI syntax with
        # other components, particularly the sambacc library.
        return f'rados:mon-config-key:{self._store_key}'

    @property
    def full_key(self) -> EntryKey:
        """Return a namespaced key for the entry."""
        return self._key


class MonKeyConfigStore:
    """A config store that wraps the global ceph mon config-key store. Unlike
    the module config store, it is not directly linked to the mgr module in
    use.

    N.B. The features that this store provide overlap with the MgrConfigStore
    but this store allows us to use the generic interface that does not
    automatically prefix keys making this store more appropriate for things we
    want stored in the mon but shareable across many components (not limited to
    just this mgr module).
    Currently, this store doesn't do any caching. Items are serialized and
    saved/fetched via the mon_command api directly.
    """

    PREFIX = 'smb/config/'

    def __init__(self, mc: MonCommandIssuer):
        self._mc = mc

    def __getitem__(self, key: EntryKey) -> MonKeyStoreEntry:
        """Return an entry object given a namespaced entry key. This entry does
        not have to exist in the store.
        """
        return MonKeyStoreEntry(self, key)

    def remove(self, key: EntryKey) -> bool:
        """Remove an entry from the store. Returns true if an entry was
        present.
        """
        if key not in self:
            return False
        self._rm(key)
        return True

    def namespaces(self) -> Collection[str]:
        """Return all namespaces currently in the store."""
        return {k[0] for k in self}

    def contents(self, ns: str) -> Collection[str]:
        """Return all subkeys currently in the namespace."""
        return [k[1] for k in self if k[0] == ns]

    def __iter__(self) -> Iterator[EntryKey]:
        """Iterate over all namespaced keys currently in the store."""
        ret, json_data, err = self._mc.mon_command(
            {
                'prefix': 'config-key dump',
                'key': self.PREFIX,
            }
        )
        if ret != 0:
            raise KeyError(
                f'config-key dump {self.PREFIX!r} failed [{ret}]: {err}'
            )
        for k in json.loads(json_data):
            yield _ksplit(k, prefix=self.PREFIX)

    def __contains__(self, key: EntryKey) -> bool:
        """Return true if the namespaced key currently exists within the store."""
        key = self.PREFIX + _kjoin(key)
        ret, _, err = self._mc.mon_command(
            {
                'prefix': 'config-key exists',
                'key': key,
            }
        )
        return ret == 0

    def _get_val(self, key: EntryKey) -> str:
        """Fetch value from mon."""
        key = self.PREFIX + _kjoin(key)
        ret, json_data, err = self._mc.mon_command(
            {
                'prefix': 'config-key get',
                'key': key,
            }
        )
        if ret != 0:
            raise KeyError(f'config-key get {key!r} failed [{ret}]: {err}')
        return json_data

    def _set_val(self, key: EntryKey, val: str) -> None:
        """Set value in mon."""
        key = self.PREFIX + _kjoin(key)
        ret, _, err = self._mc.mon_command(
            {
                'prefix': 'config-key set',
                'key': key,
                'val': val,
            }
        )
        if ret != 0:
            raise KeyError(f'config-key set failed [{ret}]: {err}')

    def _rm(self, key: EntryKey) -> str:
        """Remove value from mon."""
        key = self.PREFIX + _kjoin(key)
        ret, json_data, err = self._mc.mon_command(
            {
                'prefix': 'config-key rm',
                'key': key,
            }
        )
        if ret != 0:
            raise KeyError(f'config-key rm {key!r} failed [{ret}]: {err}')
        return json_data
