from typing import Dict, List, Optional, Tuple

import logging
import posixpath
import stat
import time

import cephfs
from mgr_util import CephfsClient, Module_T, open_filesystem

from .proto import MonCommandIssuer

log = logging.getLogger(__name__)


class AuthorizationGrantError(ValueError):
    pass


class FileSystemAuthorizer:
    """Using the rados apis provided by the ceph mgr, authorize cephx users for
    file system access.
    """

    def __init__(self, mc: MonCommandIssuer) -> None:
        self._mc = mc

    def authorize_entity(
        self, volume: str, entity: str, caps: Optional[List[str]] = None
    ) -> None:
        # TODO: the prototype is starting with wide open caps. we may want
        # to have more restricted defaults in the future
        assert entity.startswith('client.')
        if not caps:
            caps = ['/', 'rw']
        cmd = {
            'prefix': 'fs authorize',
            'filesystem': volume,
            'entity': entity,
            'caps': caps,
        }
        log.info('Requesting fs authorzation: %r', cmd)
        ret, _, status = self._mc.mon_command(cmd)
        if ret != 0:
            raise AuthorizationGrantError(status)
        log.info('Authorization request success: %r', status)


class CephFSSubvolumeResolutionError(KeyError):
    pass


class CephFSPathResolver:
    """Using the rados and cephfs apis, the CephFSPathResolver can be used to
    map to real paths in the cephfs volume and determine if those paths exist.
    """

    def __init__(
        self, mgr: Module_T, *, client: Optional[CephfsClient] = None
    ) -> None:
        self._mgr = mgr
        self._cephfs_client = client or CephfsClient(mgr)

    def resolve_subvolume_path(
        self, volume: str, subvolumegroup: str, subvolume: str
    ) -> str:
        """Given a volume, subvolumegroup, and subvolume, return the real path
        within the file system. subvolumegroup and subvolume may be empty strings
        when no subvolume is being used.
        """
        cmd = {
            'prefix': 'fs subvolume getpath',
            'vol_name': volume,
            'sub_name': subvolume,
        }
        if subvolumegroup:
            cmd['group_name'] = subvolumegroup
        log.debug('Mapping subvolume to path: %r', cmd)
        ret, data, status = self._mgr.mon_command(cmd)
        if ret != 0:
            raise CephFSSubvolumeResolutionError(status)
        log.info('Mapped subvolume to path: %r', data)
        return data.strip()

    def resolve(
        self, volume: str, subvolumegroup: str, subvolume: str, path: str
    ) -> str:
        """Given a volume, subvolumegroup, subvolume, and path, return the real
        path within the file system. subvolumegroup and subvolume may be empty
        strings when no subvolume is being used.
        """
        path = path.lstrip('/')
        if not (subvolumegroup or subvolume):
            return f'/{path}'
        subvolume_path = self.resolve_subvolume_path(
            volume, subvolumegroup, subvolume
        )
        return posixpath.join(subvolume_path, path)

    def resolve_exists(
        self, volume: str, subvolumegroup: str, subvolume: str, path: str
    ) -> str:
        """Executes the `resolve` method and verifies that it maps to a real
        sharable directory. May raise FileNotFoundError or NotADirectoryError
        when the path is not valid.
        """
        volpath = self.resolve(volume, subvolumegroup, subvolume, path)
        with open_filesystem(self._cephfs_client, volume) as fs:
            log.debug('checking if %r is a dir in %r', volpath, volume)
            try:
                stx = fs.statx(
                    volpath.encode('utf-8'),
                    cephfs.CEPH_STATX_MODE,
                    cephfs.AT_SYMLINK_NOFOLLOW,
                )
            except (cephfs.ObjectNotFound, OSError) as err:
                log.info('%r failed to stat: %s', volpath, err)
                raise FileNotFoundError(volpath)
            if not stat.S_ISDIR(stx.get('mode')):
                log.info('%r is not a directory', volpath)
                raise NotADirectoryError(volpath)
        log.debug('Verified that %r exists in %r', volpath, volume)
        return volpath


class _TTLCache:
    def __init__(self, maxsize: int = 512, ttl: float = 300.0) -> None:
        self.cache: Dict[Tuple[str, str, str], Tuple[str, float]] = {}
        self.maxsize: int = maxsize
        self.ttl: float = ttl

    def _evict(self) -> None:
        """Evicts items that have expired or if cache size exceeds maxsize."""
        current_time: float = time.monotonic()
        keys_to_evict: list[Tuple[str, str, str]] = [
            key
            for key, (_, timestamp) in self.cache.items()
            if current_time - timestamp > self.ttl
        ]
        for key in keys_to_evict:
            del self.cache[key]

        # Further evict if cache size exceeds maxsize
        if len(self.cache) > self.maxsize:
            for key in list(self.cache.keys())[
                : len(self.cache) - self.maxsize
            ]:
                del self.cache[key]

    def get(self, key: Tuple[str, str, str]) -> Optional[str]:
        """Retrieve item from cache if it exists and is not expired."""
        self._evict()  # Ensure expired items are removed
        if key in self.cache:
            value, _ = self.cache[key]
            return value
        return None

    def set(self, key: Tuple[str, str, str], value: str) -> None:
        """Set item in cache, evicting expired or excess items."""
        self._evict()  # Ensure expired items are removed
        self.cache[key] = (value, time.monotonic())

    def clear(self) -> None:
        """Clear all items in the cache."""
        self.cache.clear()

    def __len__(self) -> int:
        """Return the number of items currently in the cache."""
        return len(self.cache)


class CachingCephFSPathResolver(CephFSPathResolver):
    """
    A subclass of CephFSPathResolver that adds caching to the resolve method
    to improve performance by reducing redundant path resolutions.

    This implementation uses a TTL (Time-To-Live) cache rather than an LRU (Least
    Recently Used) cache. The TTL cache is preferred in this scenario because
    the validity of cached paths is time-sensitive, and we want to ensure that
    paths are refreshed after a certain period regardless of access frequency.
    Rlock can be used to synchronize access to the cache, but that is something
    not required for now & can be later tested.
    """

    def __init__(
        self, mgr: Module_T, *, client: Optional[CephfsClient] = None
    ) -> None:
        super().__init__(mgr, client=client)
        # Initialize a TTL cache.
        self._cache = _TTLCache(maxsize=512, ttl=5)

    def resolve_subvolume_path(
        self, volume: str, subvolumegroup: str, subvolume: str
    ) -> str:
        cache_key = (volume, subvolumegroup, subvolume)
        cached_path = self._cache.get(cache_key)
        if cached_path:
            log.debug("Cache hit for key: %r", cache_key)
            return cached_path

        log.debug("Cache miss for key: %r", cache_key)
        resolved_path = super().resolve_subvolume_path(
            volume, subvolumegroup, subvolume
        )
        self._cache.set(cache_key, resolved_path)
        return resolved_path
