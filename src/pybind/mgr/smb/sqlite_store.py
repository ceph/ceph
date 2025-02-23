from typing import (
    TYPE_CHECKING,
    Any,
    Collection,
    ContextManager,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

if TYPE_CHECKING:
    from sqlite3 import Connection, Cursor
else:
    Cursor = Connection = Any

import contextlib
import copy
import json
import logging
import threading

from .config_store import ObjectCachingEntry
from .proto import (
    ConfigEntry,
    ConfigStore,
    EntryKey,
    FindParams,
    Protocol,
    Simplified,
)

log = logging.getLogger(__name__)


class DirectDBAcessor(Protocol):
    """A simple protocol describing the minimal per-mgr-module (mon) store interface
    provided by the fairly giganto MgrModule class.
    """

    @property
    def db(self) -> Connection:
        ...


class ExclusiveCursorAccessor(Protocol):
    """A wrapper protocol for describing a method for getting exclusive
    access to the db via a cursor.
    """

    def exclusive_db_cursor(self) -> ContextManager[Cursor]:
        ...


DBAcessor = Union[DirectDBAcessor, ExclusiveCursorAccessor]


def _execute(
    dbc: Cursor, query: str, *args: Any, params: Optional[FindParams] = None
) -> None:
    log.debug(
        "executing sql query: %s, args: %r, params: %r", query, args, params
    )
    if params and args:
        raise ValueError('got args and params')
    if params:
        dbc.execute(query, params)
        return
    dbc.execute(query, args)


class Table:
    """Abstract table for holding store entries."""

    def __init__(self, namespace: str, table_name: str) -> None:
        self.namespace = namespace
        self.table_name = table_name

    def create_table(self, dbc: Cursor) -> None:
        """Create a new db table."""
        raise NotImplementedError()

    def keys(self, dbc: Cursor) -> Collection[EntryKey]:
        """Return all (primary) keys in the table."""
        raise NotImplementedError()

    def fetch(self, dbc: Cursor, key: str) -> str:
        """Fetch a serialized object from the table."""
        raise NotImplementedError()

    def delete(self, dbc: Cursor, key: str) -> int:
        """Delete an item from the table."""
        raise NotImplementedError()

    def find(
        self, dbc: Cursor, params: FindParams
    ) -> Iterable[Tuple[EntryKey, str]]:
        """Find a matching object in the table."""
        raise NotImplementedError()

    def replace(self, dbc: Cursor, key: str, data: str) -> None:
        """Create or replace a serialized object in the table."""
        raise NotImplementedError()


class SimpleTable(Table):
    """A simple table that is capable of storing JSON serialized objects
    with a simple primary key. A SimpleTable ought to be capable of
    representing any kind of store entry but without any optimizations.
    """

    def create_table(self, dbc: Cursor) -> None:
        _execute(
            dbc,
            f"CREATE TABLE IF NOT EXISTS {self.table_name} ("
            "  key TEXT PRIMARY KEY NOT NULL,"
            "  obj JSON"
            ");",
        )

    def keys(self, dbc: Cursor) -> Collection[EntryKey]:
        """Return all (primary) keys in the table."""
        query = f"SELECT key FROM {self.table_name} ORDER BY rowid;"
        _execute(dbc, query)
        return set((self.namespace, r[0]) for r in dbc.fetchall())

    def fetch(self, dbc: Cursor, key: str) -> str:
        """Fetch a serialized object from the table."""
        query = f"SELECT obj FROM {self.table_name} WHERE key=?;"
        _execute(dbc, query, key)
        row = dbc.fetchone()
        if row is None:
            raise KeyError(key)
        return row[0]

    def delete(self, dbc: Cursor, key: str) -> int:
        """Delete an item from the table."""
        query = f"DELETE FROM {self.table_name} WHERE key=?;"
        _execute(dbc, query, key)
        return dbc.rowcount

    def replace(self, dbc: Cursor, key: str, data: str) -> None:
        """Create or replace a serialized object in the table."""
        query = (
            f"INSERT OR REPLACE INTO {self.table_name}"
            " (key, obj) VALUES (?, ?);"
        )
        _execute(dbc, query, key, data)


class ShareResourceTable(SimpleTable):
    """A table tuned for storing share resources.
    This table supports finding shares with particular names
    faster than walking over every share in the table, deserializing it,
    and comparing the values in python.

    Some calls making use of the find function can complete in approx. 0.0004s
    vs 0.008s on average for non-specialized versions when using around 500
    shares in a single cluster.

    This is a bit of a leaky abstraction because this table "knows"
    about the structure of a serialized Share resource implicitly.
    If the Share resource changes this may need to be kept in sync
    manually.
    """

    def create_table(self, dbc: Cursor) -> None:
        """Create a table for shares with indexes."""
        super().create_table(dbc)
        _execute(
            dbc,
            f"CREATE INDEX IF NOT EXISTS idx_{self.table_name}_cn"
            f" ON {self.table_name} ("
            "  json_extract(obj, '$.cluster_id'),"
            "  json_extract(obj, '$.name')"
            ");",
        )

    def find(
        self, dbc: Cursor, params: FindParams
    ) -> Iterable[Tuple[EntryKey, str]]:
        """Find a matching object in the table using json field matching on a
        limited set of Share fields.
        """
        query = f"SELECT key, obj FROM {self.table_name} WHERE"
        valid_columns = {'key', 'cluster_id', 'share_id', 'name'}
        where = []
        for param in params:
            if param not in valid_columns:
                log.error('can not find obj using param: %r', param)
                raise NotImplementedError('invalid parameter')
            if param == 'key':
                # a tad redundant, but why not
                where.append('key=:key')
            else:
                # the version of sqlite currently in use by ceph does not support
                # the ->> operator. use `json_extract` instead.
                where.append(f"json_extract(obj, '$.{param}') = :{param}")
        query += ' ' + ' AND '.join(where)

        _execute(dbc, query, params=params)
        for row in dbc:
            yield ((self.namespace, row[0]), row[1])


class SqliteStoreEntry:
    """A store entry for the SqliteStore."""

    def __init__(self, store: 'SqliteStore', full_key: EntryKey) -> None:
        self._store = store
        self._full_key = full_key

    def set(self, obj: Simplified) -> None:
        """Set the store entry value to that of the serialized value of obj."""
        self._store.set_object(self._full_key, obj)

    def get(self) -> Simplified:
        """Get the deserialized store entry value."""
        return self._store.get_object(self._full_key)

    def remove(self) -> bool:
        """Remove the current entry from the store."""
        return self._store.remove(self._full_key)

    def exists(self) -> bool:
        """Returns true if the entry currently exists within the store."""
        return self._full_key in self._store

    @property
    def uri(self) -> str:
        """Returns an identifier for the entry within the store."""
        ns, name = self._full_key
        return f'ceph-internal-sqlite-resource:{ns}/{name}'

    @property
    def full_key(self) -> EntryKey:
        """Return a namespaced key for the entry."""
        return self._full_key


class SqliteStore:
    """A store wrapping a sqlite3 database.

    A SqliteStore maps each store namespace to a particular db table. This means
    that unlike some stores arbitrary namespace values are not supported. The
    namespaces are fixed ahead of time and well known.

    This store is mainly aimed at providing a fast internal store suitable for
    tracking the internal module resources, in particular shares, because these
    are expected to be more numerous than the other resource types.
    """

    def __init__(self, backend: DBAcessor, tables: Iterable[Table]) -> None:
        self._backend = backend
        self._tables: Dict[str, Table] = {t.namespace: t for t in tables}
        self._prepared = False
        self._cursor: Optional[Cursor] = None
        self._db_lock = threading.Lock()

    def _prepare_tables(self) -> None:
        """Automatic/internal table preparation."""
        if self._prepared:
            return
        with self._db() as dbc:
            self.prepare(dbc)

    def prepare(self, dbc: Cursor) -> None:
        """Prepare db tables for use."""
        if self._prepared:
            return
        log.info('Preparing db tables for store')
        for tbl in self._tables.values():
            tbl.create_table(dbc)
        self._prepared = True

    def _table(self, key: Union[str, EntryKey]) -> Table:
        ns = key if isinstance(key, str) else key[0]
        return self._tables[ns]

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        """Explicitly start a DB transaction."""
        with self._db_lock, self._db():
            assert self._cursor
            self._cursor.execute('BEGIN;')
            yield None

    @contextlib.contextmanager
    def _db(self) -> Iterator[Cursor]:
        if self._cursor is not None:
            log.debug('fetching cached cursor')
            yield self._cursor
            return
        if hasattr(self._backend, 'exclusive_db_cursor'):
            log.debug('fetching exclusive db cursor')
            with self._backend.exclusive_db_cursor() as cursor:
                try:
                    self._cursor = cursor
                    yield cursor
                finally:
                    self._cursor = None
            return
        log.debug('fetching default db cursor')
        with self._backend.db:
            try:
                self._cursor = self._backend.db.cursor()
                yield self._cursor
            finally:
                self._cursor = None

    def __getitem__(self, key: EntryKey) -> SqliteStoreEntry:
        """Return an entry object given a namespaced entry key. This entry does
        not have to exist in the store.
        """
        self._prepare_tables()
        return SqliteStoreEntry(self, key)

    def remove(self, key: EntryKey) -> bool:
        """Remove an entry from the store. Returns true if an entry was
        present.
        """
        self._prepare_tables()
        with self._db() as dbc:
            _, tkey = key
            rcount = self._table(key).delete(dbc, tkey)
        return rcount > 0

    def set_object(self, key: EntryKey, obj: Simplified) -> None:
        """Create or update a simplified object in the store."""
        self._prepare_tables()
        obj_data = json.dumps(obj)
        with self._db() as dbc:
            _, tkey = key
            self._table(key).replace(dbc, tkey, obj_data)

    def get_object(self, key: EntryKey) -> Simplified:
        """Fetch a simplified object from the store."""
        self._prepare_tables()
        with self._db() as dbc:
            _, tkey = key
            obj = json.loads(self._table(key).fetch(dbc, tkey))
        return obj

    def namespaces(self) -> Collection[str]:
        """Return all namespaces currently in the store."""
        self._prepare_tables()
        return set(self._tables.keys())

    def contents(self, ns: str) -> Collection[str]:
        """Return all subkeys currently in the namespace."""
        self._prepare_tables()
        with self._db() as dbc:
            return {k for _, k in self._table(ns).keys(dbc)}

    def __iter__(self) -> Iterator[EntryKey]:
        """Iterate over all namespaced keys currently in the store."""
        self._prepare_tables()
        with self._db() as dbc:
            for ns, tbl in self._tables.items():
                for key in tbl.keys(dbc):
                    yield key

    def find_entries(
        self, ns: str, params: FindParams
    ) -> Collection[ConfigEntry]:
        """Find matching entries in the store, belonging to the specified namespace."""
        self._prepare_tables()
        with self._db() as dbc:
            return [
                ObjectCachingEntry(
                    SqliteStoreEntry(self, ekey), obj=json.loads(obj)
                )
                for ekey, obj in self._table(ns).find(dbc, params)
            ]

    @property
    def data(self) -> Dict[EntryKey, Simplified]:
        """Debugging/testing helper for dumping contents of the store."""
        out = {}
        for k in self:
            assert isinstance(k, tuple)
            out[k] = self.get_object(k)
        return out

    def overwrite(self, nd: Dict[EntryKey, Simplified]) -> None:
        """Debugging/testing helper for changing contents of the store."""
        for key, obj in nd.items():
            if isinstance(key, str):
                keyns, keyname = key.split('.', 1)
            else:
                keyns, keyname = key
            self.set_object((keyns, keyname), obj)


class Mirror:
    """A mirror configuration for a SqliteMirroringStore namespace.
    The mirror will store a copy of an object in a separate ConfigStore.
    This copy may be modified by the filter methods and combined using
    the `merge` method.
    """

    namespace: str
    store: ConfigStore

    def __init__(self, namespace: str, store: ConfigStore) -> None:
        self.namespace = namespace
        self.store = store

    def filter_object(self, obj: Simplified) -> Simplified:
        """Return a potentially modified object to be stored in the sqlite db store."""
        return obj

    def filter_mirror_object(self, obj: Simplified) -> Simplified:
        """Return a potentially modified object to be stored in the mirror store."""
        return obj

    def merge(self, obj1: Simplified, obj2: Simplified) -> Simplified:
        """Combine, if desired, the objects fetched from the sqlite db store
        and the mirror store.
        """
        obj = copy.deepcopy(obj1)
        obj.update(obj2)
        return obj


class SqliteMirroringStore(SqliteStore):
    """A store based on the SqliteStore that supports mirroring objects in
    specified namespaces from the database into a different store.

    The purpose of the mirror is to store objects in the db for the speed and
    efficiency of the db's search features while storing a copy of the object
    in a different store that will have other properties.  The mirror classes
    can configure how objects in each namespace are handled and which store
    takes precedence when fetching and merging results.
    """

    def __init__(
        self,
        backend: DBAcessor,
        tables: Iterable[Table],
        mirrors: Iterable[Mirror],
    ) -> None:
        super().__init__(backend, tables)
        self._mirrors: Dict[str, Mirror] = {m.namespace: m for m in mirrors}

    def _mirror(self, key: EntryKey) -> Optional[Mirror]:
        ns, _ = key
        return self._mirrors.get(ns)

    def set_object(self, key: EntryKey, obj: Simplified) -> None:
        """Create or update a simplified object in the store."""
        mirror = self._mirror(key)
        if mirror is None:
            log.debug("Mirroring set_object: no mirror for key %r", key)
            super().set_object(key, obj)
            return
        log.debug("Mirroring set_object: mirror=%r", mirror)
        obj_for_store = mirror.filter_object(obj)
        obj_for_mirror = mirror.filter_mirror_object(obj)
        mirror.store[key].set(obj_for_mirror)
        super().set_object(key, obj_for_store)

    def get_object(self, key: EntryKey) -> Simplified:
        """Fetch a simplified object from the store."""
        mirror = self._mirror(key)
        if mirror is None:
            log.debug("Mirroring get_object: no mirror for %r", key)
            return super().get_object(key)
        log.debug("Mirroring get_object: mirror=%r", mirror)
        obj = super().get_object(key)
        mirror_obj = mirror.store[key].get()
        return mirror.merge(obj, mirror_obj)


class MirrorJoinAuths(Mirror):
    """Mirroring configuration for objects in the join_auths namespace."""

    def __init__(self, store: ConfigStore) -> None:
        super().__init__('join_auths', store)

    def filter_object(self, obj: Simplified) -> Simplified:
        """Filter join auth data for sqlite3 store."""
        filtered = copy.deepcopy(obj)
        if 'auth' in filtered:
            filtered['auth'].pop('password', None)
        return filtered


class MirrorUsersAndGroups(Mirror):
    """Mirroring configuration for objects in the users_and_groups namespace."""

    def __init__(self, store: ConfigStore) -> None:
        super().__init__('users_and_groups', store)

    def filter_object(self, obj: Simplified) -> Simplified:
        """Filter join users and groups data for sqlite3 store."""
        filtered = copy.deepcopy(obj)
        for user in filtered.get('values', {}).get('users', []):
            # retain the key, to have the capability of knowing it was part of
            # this row, but remove the value from this object
            if 'password' in user:
                user['password'] = ''
        return filtered


def _tables(
    *,
    specialize: bool = True,
) -> List[Table]:
    """Create tables for the current smb resources.
    This function implicitly knows what resources will be needed and so
    must be manually kept in sync with the resources.py objects.
    """
    srt: Table
    if specialize:
        log.debug('using specialized shares table')
        srt = ShareResourceTable('shares', 'shares')
    else:
        log.warning('using non-specialized shares table')
        srt = SimpleTable('shares', 'shares')
    return [
        SimpleTable('clusters', 'clusters'),
        srt,
        SimpleTable('join_auths', 'join_auths'),
        SimpleTable('users_and_groups', 'users_and_groups'),
    ]


def _specialize(opts: Optional[Dict[str, str]] = None) -> bool:
    return (opts or {}).get('specialize') != 'no'


def _mirror_join_auths(opts: Optional[Dict[str, str]] = None) -> bool:
    return (opts or {}).get('mirror_join_auths') != 'no'


def _mirror_users_and_groups(opts: Optional[Dict[str, str]] = None) -> bool:
    return (opts or {}).get('mirror_users_and_groups') != 'no'


def mgr_sqlite3_db(
    mgr: Any, opts: Optional[Dict[str, str]] = None
) -> SqliteStore:
    """Set up a store for use in the real ceph mgr."""
    specialize = _specialize(opts)
    return SqliteStore(
        mgr,
        _tables(specialize=specialize),
    )


def mgr_sqlite3_db_with_mirroring(
    mgr: Any,
    mirror_store: ConfigStore,
    opts: Optional[Dict[str, str]] = None,
) -> SqliteMirroringStore:
    """Set up a store for use in the ceph mgr that will mirror some
    objects into an alternate store.
    """
    tables = _tables(specialize=_specialize(opts))
    mirrors: List[Mirror] = []
    if _mirror_join_auths(opts):
        mirrors.append(MirrorJoinAuths(mirror_store))
    if _mirror_users_and_groups(opts):
        mirrors.append(MirrorUsersAndGroups(mirror_store))
    return SqliteMirroringStore(mgr, tables, mirrors)


def memory_db(
    *,
    specialize: bool = True,
) -> SqliteStore:
    """A hack to set up the store to use an in memory sqlite db for unit
    testing.
    """
    import sqlite3

    uri = ':memory:'
    try:
        conn = sqlite3.connect(
            uri, check_same_thread=False, uri=True, autocommit=False
        )  # type: ignore[call-arg]
    except TypeError:
        conn = sqlite3.connect(
            uri, check_same_thread=False, uri=True, isolation_level=None
        )

    class InMemoryDB:
        db = conn

    return SqliteStore(
        InMemoryDB(),
        _tables(specialize=specialize),
    )
