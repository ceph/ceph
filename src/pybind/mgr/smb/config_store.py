from typing import Collection, Dict, Iterator

from .proto import ConfigEntry, EntryKey, Simplified


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
