from typing import List, Optional

import logging
import posixpath
import stat

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

    def __init__(self, mgr: Module_T) -> None:
        self._mgr = mgr
        self._cephfs_client = CephfsClient(mgr)

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
        log.debug('Mapped subvolume to path: %r', data)
        return posixpath.join(data.strip(), path)

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
