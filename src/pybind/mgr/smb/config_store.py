from typing import Collection, Dict, Iterator, Optional

from .proto import ConfigEntry, ConfigStore, EntryKey, FindParams, Simplified


class MemConfigEntry:
    """A simple in-memory config store entry. Meant only for testing.
    Objects are not serialized like most other stores.
    """

    def __init__(self, store: 'MemConfigStore', ns: str, name: str) -> None:
        self._store = store
        self._ns = ns
        self._name = name

    def set(self, obj: Simplified) -> None:
        self._store._data[(self._ns, self._name)] = obj

    def get(self) -> Simplified:
        return self._store._data[(self._ns, self._name)]

    def remove(self) -> bool:
        return self._store.remove(self.full_key)

    def exists(self) -> bool:
        return (self._ns, self._name) in self._store._data

    @property
    def uri(self) -> str:
        return f'mem:{self._ns}/{self._name}'

    @property
    def full_key(self) -> EntryKey:
        return (self._ns, self._name)


class MemConfigStore:
    """A simple in-memory config store. Meant only for testing."""

    def __init__(self) -> None:
        self._data: Dict[EntryKey, Simplified] = {}

    def __getitem__(self, key: EntryKey) -> MemConfigEntry:
        return MemConfigEntry(self, key[0], key[1])

    def remove(self, key: EntryKey) -> bool:
        return self._data.pop(key, None) is not None

    def namespaces(self) -> Collection[str]:
        return {k[0] for k in self._data.keys()}

    def contents(self, ns: str) -> Collection[str]:
        return [k[1] for k in self._data.keys() if k[0] == ns]

    def __iter__(self) -> Iterator[EntryKey]:
        return iter(self._data.keys())

    # for testing only
    def overwrite(self, data: Simplified) -> None:
        self._data = {}
        for key, value in data.items():
            if isinstance(key, str):
                keyns, keyname = key.split('.', 1)
            else:
                keyns, keyname = key
            self._data[(keyns, keyname)] = value

    # for testing only
    @property
    def data(self) -> Dict[EntryKey, Simplified]:
        return self._data


class EntryCache:
    """An in-memory cache compatible with the ConfigStore interface. It should
    be used to cache *existing* ConfigEntry objects produced by/for other
    stores.
    """

    def __init__(self) -> None:
        self._entries: Dict[EntryKey, ConfigEntry] = {}

    def __getitem__(self, key: EntryKey) -> ConfigEntry:
        return self._entries[key]

    def __setitem__(self, key: EntryKey, value: ConfigEntry) -> None:
        self._entries[key] = value

    def remove(self, key: EntryKey) -> bool:
        return self._entries.pop(key, None) is not None

    def namespaces(self) -> Collection[str]:
        return {k[0] for k in self}

    def contents(self, ns: str) -> Collection[str]:
        return [kname for kns, kname in self if ns == ns]

    def __iter__(self) -> Iterator[EntryKey]:
        return iter(self._entries.keys())


class ObjectCachingEntry:
    """A config entry that wraps a different ConfigEntry and caches the
    simplified object. If the object is set the cache will be updated. If the
    object is removed the cached object will be forgotten. The cached object
    can be manually reset with the `clear_cached_obj` method.
    """

    def __init__(
        self, base_entry: ConfigEntry, *, obj: Optional[Simplified] = None
    ) -> None:
        self._base = base_entry
        self._obj = obj

    def clear_cached_obj(self) -> None:
        self._obj = None

    def set(self, obj: Simplified) -> None:
        self._obj = None  # if base.set fails, obj will be left unset
        self._base.set(obj)
        self._obj = obj

    def get(self) -> Simplified:
        if self._obj is not None:
            return self._obj
        self._obj = self._base.get()
        return self._obj

    def remove(self) -> bool:
        self._obj = None
        return self._base.remove()

    def exists(self) -> bool:
        return self._base.exists()

    @property
    def uri(self) -> str:
        return self._base.uri

    @property
    def full_key(self) -> EntryKey:
        return self._base.full_key


def find_in_store(
    store: ConfigStore, ns: str, params: FindParams
) -> Collection[ConfigEntry]:
    """Given a ConfigStore and namespace within that store, search the stored
    objects for matching parameters. Params is a dict that will be compared to
    the same keys/attributes of the objects being searched. Only exact matches
    will be returned.
    If the store implements the FindingConfigStore protocol the operation
    of finding
    """
    # is it a FindingConfigStore?
    _find_entries = getattr(store, 'find_entries', None)
    if _find_entries:
        try:
            return _find_entries(ns, params)
        except NotImplementedError:
            # Allow the store to reject any of the ns/params/whatnot with a
            # NotImplementedError even if it implements the find_entries
            # function. This will fall back to the simple-but-slow approach of
            # deserializing and examining every object.
            pass
    return _find_in_store(store, ns, params)


def _find_in_store(
    store: ConfigStore, ns: str, params: FindParams
) -> Collection[ConfigEntry]:
    """Fallback mode for find_in_store."""
    found = []
    for sub_key in store.contents(ns):
        entry = store[(ns, sub_key)]
        obj = entry.get()
        if all(obj[pkey] == pval for pkey, pval in params.items()):
            found.append(ObjectCachingEntry(entry, obj=obj))
    return found
