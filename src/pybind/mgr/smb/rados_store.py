from typing import (
    TYPE_CHECKING,
    Callable,
    Collection,
    Iterator,
    Optional,
    Tuple,
)

import contextlib
import functools
import json
import logging
import time
import uuid

import rados

from .proto import EntryKey, Self, Simplified

if TYPE_CHECKING:  # pragma: no cover
    from mgr_module import MgrModule

_CHUNK_SIZE = 1024 * 1024
SMB_POOL = '.smb'

log = logging.getLogger(__name__)


class RADOSConfigEntry:
    """A store entry object for the RADOS pool based store."""

    def __init__(
        self, rados: rados.Rados, pool: str, ns: str, key: str
    ) -> None:
        self._rados = rados
        self._pool = pool
        self._ns = ns
        self._key = key
        self._ioctx = None

    @property
    def uri(self) -> str:
        """Returns an identifier for the entry within the store."""
        # The rados://<pool>/<ns>/<key> convention can be found elsewhere
        # in ceph. borrowed here for communicating resource keys to
        # other components.
        return f'rados://{self._pool}/{self._ns}/{self._key}'

    @property
    def full_key(self) -> EntryKey:
        """Return a namespaced key for the entry."""
        return (self._ns, self._key)

    def read(self) -> str:
        """Read a RAODS object."""
        log.debug('rados read of %s', self.full_key)
        with self._shared_ioctx() as ioctx:
            ioctx.set_namespace(self._ns)
            try:
                val = ioctx.read(self._key, _CHUNK_SIZE).decode()
            except rados.ObjectNotFound:
                val = ''
        log.debug('rados read result of %s = %r', self.full_key, val)
        return val

    def write(self, content: str) -> None:
        """Write a RADOS object."""
        log.debug('rados write to %s', self.full_key)
        data = content.encode('utf-8')
        assert len(data) < _CHUNK_SIZE
        with self._shared_ioctx() as ioctx:
            ioctx.set_namespace(self._ns)
            ioctx.write_full(self._key, data)

    def get(self) -> Simplified:
        """Get the deserialized store entry value."""
        if not self.exists():
            raise KeyError(self.full_key)
        data = self.read()
        if not data:
            # empty data is equivalent to object not existing.
            # this may occur if a lock is taken.
            raise KeyError(self.full_key)
        return json.loads(data)

    def set(self, obj: Simplified) -> None:
        """Set the store entry value to that of the serialized value of obj."""
        self.write(json.dumps(obj))

    def remove(self) -> bool:
        """Remove the current entry from the store."""
        log.debug('rados remove of %s', self.full_key)
        with self._shared_ioctx() as ioctx:
            ioctx.set_namespace(self._ns)
            try:
                ioctx.remove_object(self._key)
                removed = True
            except rados.ObjectNotFound:
                removed = False
        log.debug('rados remove result of %s = %r', self.full_key, removed)
        return removed

    def exists(self) -> bool:
        """Returns true if the entry currently exists within the store."""
        log.debug('rados exists of %s', self.full_key)
        try:
            with self._shared_ioctx() as ioctx:
                ioctx.set_namespace(self._ns)
                ioctx.stat(self._key)
            found = True
        except rados.ObjectNotFound:
            found = False
        log.debug('rados exists result of %s = %r', self.full_key, found)
        return found

    @contextlib.contextmanager
    def locked(self, name: str) -> Iterator[None]:
        """Place a rados lock on the object for the duration of the context
        manager. Requires a lock name.
        """
        with self._shared_ioctx() as ioctx:
            ioctx.set_namespace(self._ns)
            cookie = self._acquire_lock(ioctx, name)
            try:
                yield None
            finally:
                self._release_lock(ioctx, name, cookie)

    @contextlib.contextmanager
    def _shared_ioctx(self) -> Iterator[rados.Ioctx]:
        """Helper for returning a ioctx for nested operations."""
        if self._ioctx is not None:
            yield self._ioctx
            return
        with self._rados.open_ioctx(self._pool) as ioctx:
            self._ioctx = ioctx
            try:
                yield ioctx
            finally:
                self._ioctx = None

    def _acquire_lock(
        self,
        ioctx: rados.Ioctx,
        name: str,
        desc: str = 'rados_store',
        *,
        wait_sec: float = 0.25,
        max_wait: int = 30,
    ) -> str:
        """Acquire a rados lock."""
        cookie = f'mgr:smb:{uuid.uuid4()}'
        for _ in range(int(max_wait / wait_sec)):
            try:
                ioctx.lock_exclusive(
                    self._key, name, cookie, desc=desc, duration=None
                )
                return cookie
            except rados.ObjectBusy as err:
                log.debug("object busy: %r, %r, %r", self._key, name, cookie)
                time.sleep(wait_sec)
                last_err = err
        log.warning('failed to acquire lock in %ssec: %r', max_wait, last_err)
        raise last_err

    def _release_lock(
        self, ioctx: rados.Ioctx, name: str, cookie: str
    ) -> None:
        """Release a rados lock."""
        ioctx.unlock(self._key, name, cookie)


class RADOSConfigStore:
    """A config store that saves entries in a RADOS pool.

    N.B. The RADOS config store exposes a subset of the RADOS functionality
    to implement a simple key-value store. As the namespaced keys map directly
    to RADOS namespaces and object names this store is suitable for sharing
    configuration items with external components.
    """

    def __init__(
        self,
        rados: rados.Rados,
        pool: str = SMB_POOL,
        init_cb: Optional[Callable] = None,
    ) -> None:
        self._rados = rados
        self._pool = pool
        # An optional initialization callback. If set, the callback will be
        # called once before any get, set, or other data-access call.  This is
        # to support lazily setting up the pool when we start acessing the
        # store contents.
        self._init_cb = init_cb

    def _lazy_init(self) -> None:
        if self._init_cb:
            self._init_cb
            self._init_cb = None

    def __getitem__(self, key: EntryKey) -> RADOSConfigEntry:
        """Return an entry object given a namespaced entry key. This entry does
        not have to exist in the store.
        """
        self._lazy_init()
        ns, okey = key
        return RADOSConfigEntry(self._rados, self._pool, ns, okey)

    def remove(self, ns: EntryKey) -> bool:
        """Remove an entry from the store. Returns true if an entry was
        present.
        """
        self._lazy_init()
        return self[ns].remove()

    def namespaces(self) -> Collection[str]:
        """Return all namespaces currently in the store."""
        self._lazy_init()
        return {item[0] for item in self}

    def contents(self, ns: str) -> Collection[str]:
        """Return all subkeys currently in the namespace."""
        self._lazy_init()
        return [item[1] for item in self if ns == item[0]]

    def __iter__(self) -> Iterator[EntryKey]:
        """Iterate over all namespaced keys currently in the store."""
        self._lazy_init()
        out = []
        with self._rados.open_ioctx(self._pool) as ioctx:
            ioctx.set_namespace(rados.LIBRADOS_ALL_NSPACES)
            for obj in ioctx.list_objects():
                out.append((obj.nspace, obj.key))
        return iter(out)

    @classmethod
    def init(cls, mgr: 'MgrModule', pool: str = SMB_POOL) -> Self:
        """Return a new RADOSConfigStore using the specified pool. The pool
        will be immediately created if it doesn't already exist.
        """
        _init_pool(mgr, pool)
        return cls(mgr.rados, pool=pool)

    @classmethod
    def lazy_init(cls, mgr: 'MgrModule', pool: str = SMB_POOL) -> Self:
        """Return a new RADOSConfigStore using the specified pool. The pool
        will be created when other RADOSConfigStore methods are called if it
        doesn't already exist.
        """
        cb = functools.partial(_init_pool, mgr, pool)
        return cls(mgr.rados, pool=pool, init_cb=cb)


def _init_pool(mgr: 'MgrModule', pool: str) -> None:
    """Use mgr apis to initialize a new pool if it doesn't exist."""
    pools = mgr.get_osdmap().dump().get('pools', [])
    pool_names = {p['pool_name'] for p in pools}
    if pool in pool_names:
        return
    log.debug('rados pool %r not found, creating it', pool)
    mgr.check_mon_command(
        {
            'prefix': 'osd pool create',
            'pool': pool,
            'yes_i_really_mean_it': True,
        }
    )
    mgr.check_mon_command(
        {
            'prefix': 'osd pool application enable',
            'pool': pool,
            'app': 'smb',
        }
    )


def parse_uri(uri: str) -> Tuple[str, str, str]:
    """Parse a rados-like uri into pool, namespace, and object values.
    Namespace may be an empty string.
    """
    if uri.startswith('rados://'):
        parts = uri.removeprefix('rados://').split('/')
        if len(parts) == 3:
            return tuple(parts)  # type: ignore
        if len(parts) == 2:
            return parts[0], '', parts[1]
        raise ValueError('invalid rados uri: {uri!r}')
    elif uri.startswith('rados:'):
        raise ValueError('not a supported rados uri: {uri!r}')
    raise ValueError('not a rados uri: {uri!r}')
