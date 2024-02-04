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
        value = json.dumps(obj)
        self._store._mstore.set_store(self._store_key, value)

    def get(self) -> Simplified:
        value = self._store._mstore.get_store(self._store_key)
        if value is None:
            raise KeyError(self._key)
        return json.loads(value)

    def remove(self) -> bool:
        return self._store.remove(self.full_key)

    def exists(self) -> bool:
        return self._key in set(self._store)

    @property
    def uri(self) -> str:
        ns, name = self._key
        return f'ceph-smb-resource:{ns}/{name}'

    @property
    def full_key(self) -> EntryKey:
        return self._key


class ModuleConfigStore:
    """A store that serve's as a layer on top of a mgr module's key/value store.
    Most appropriate for the smb module internal store.
    """

    # NOTE: This store is ulimately backed by the same data store as the `ceph
    # config-key ...` commands. In the mgr C++ code that implements the three
    # functions we need in he MgrStoreProtocol there's a cache as well. Thus
    # there's little point in caching at this layer - at least not caching the
    # serialized strings - because the mgr c++ layer is already doing that.

    PREFIX = 'ceph.smb.resources/'

    def __init__(self, mstore: MgrStoreProtocol):
        self._mstore = mstore

    def __getitem__(self, key: EntryKey) -> ModuleStoreEntry:
        return ModuleStoreEntry(self, key)

    def remove(self, key: EntryKey) -> bool:
        # The Ceph Mgr api uses none as special token to delete the item.
        # Otherwise it only accepts strings to set.
        if key not in self:
            return False
        self._mstore.set_store(self.PREFIX + _kjoin(key), None)
        return True

    def namespaces(self) -> Collection[str]:
        return {k[0] for k in self}

    def contents(self, ns: str) -> Collection[str]:
        return [k[1] for k in self if k[0] == ns]

    def __iter__(self) -> Iterator[EntryKey]:
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
        self._store._set_val(self._key, json.dumps(obj))

    def get(self) -> Simplified:
        return json.loads(self._store._get_val(self._key))

    def remove(self) -> bool:
        return self._store.remove(self.full_key)

    def exists(self) -> bool:
        return self._key in self._store

    @property
    def uri(self) -> str:
        return f'rados:mon-config-key:{self._store_key}'

    @property
    def full_key(self) -> EntryKey:
        return self._key


class MonKeyConfigStore:
    """A config store that wraps the global ceph mon config-key store. Unlike
    the module config store, it is not directly linked to the mgr module in
    use.
    """

    # N.B. this store doesn't currently do any caching. Items are serialized
    # and saved/fetched via the mon_command api directly.

    PREFIX = 'smb/config/'

    def __init__(self, mc: MonCommandIssuer):
        self._mc = mc

    def __getitem__(self, key: EntryKey) -> MonKeyStoreEntry:
        return MonKeyStoreEntry(self, key)

    def remove(self, key: EntryKey) -> bool:
        # The Ceph Mgr api uses none as special token to delete the item.
        # Otherwise it only accepts strings to set.
        if key not in self:
            return False
        self._rm(key)
        return True

    def namespaces(self) -> Collection[str]:
        return {k[0] for k in self}

    def contents(self, ns: str) -> Collection[str]:
        return [k[1] for k in self if k[0] == ns]

    def __iter__(self) -> Iterator[EntryKey]:
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
        key = self.PREFIX + _kjoin(key)
        ret, _, err = self._mc.mon_command(
            {
                'prefix': 'config-key exists',
                'key': key,
            }
        )
        return ret == 0

    def _get_val(self, key: EntryKey) -> str:
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
