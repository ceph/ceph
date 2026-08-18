# -*- coding: utf-8 -*-

import datetime
import errno
import logging
import os
from contextlib import contextmanager, suppress
from typing import Any, Dict, Optional

import cephfs

from .. import mgr
from ..exceptions import DashboardException
from .ceph_service import CephService, SendCommandError

logger = logging.getLogger(__name__)


def is_unmanaged_volume_entry(error_code: int, err: str) -> bool:
    """
    Detect entries whose metadata xattrs are missing/unreadable.
    """
    return (
        error_code in (-errno.EINVAL, -errno.ENODATA)
        and 'getxattr' in (err or '').lower()
    )


def unmanaged_volume_info() -> Dict[str, Any]:
    return {'state': 'unmanaged'}


def get_subvolumegroup_path(vol_name: str, group_name: str) -> str:
    error_code, out, err = mgr.remote(
        'volumes', '_cmd_fs_subvolumegroup_getpath',
        None, {'vol_name': vol_name, 'group_name': group_name})
    if error_code != 0:
        raise DashboardException(
            f'Failed to get path for subvolume group {group_name}: {err}'
        )
    return out


def has_mirroring_mds_caps(mds_caps: Optional[str], fs_name: str) -> bool:
    """Return True if MDS caps allow CephFS snapshot mirroring for fs_name.

    Mirroring requires the equivalent of ``fs authorize <fs> <entity> / rwps``,
    stored as ``allow rwps fsname=<fs>`` (path ``/`` is omitted). ``allow *``
    and unrestricted ``allow rwps`` are also sufficient.
    """
    if not mds_caps or not fs_name:
        return False

    for grant in mds_caps.split(','):
        grant = grant.strip()
        if not grant.startswith('allow '):
            continue
        parts = grant[6:].split()
        if not parts:
            continue

        perms = parts[0]
        attrs = {}
        for part in parts[1:]:
            if '=' in part:
                key, value = part.split('=', 1)
                attrs[key] = value

        if perms != '*' and not all(flag in perms for flag in 'rwps'):
            continue

        grant_fs = attrs.get('fsname')
        if grant_fs and grant_fs not in (fs_name, '*', 'all'):
            continue

        grant_path = attrs.get('path')
        if grant_path and grant_path != '/':
            continue

        return True

    return False


def ensure_mirroring_client_caps(client_name: str, fs_name: str) -> None:
    """Reject existing CephX users that lack MDS caps required for mirroring.

    Missing users are left unchanged so bootstrap can create them. Existing
    users with sufficient MDS caps are also left unchanged.
    """
    try:
        user_data = CephService.send_command('mon', 'auth get', entity=client_name)
    except SendCommandError as ex:
        if ex.errno == -errno.ENOENT:
            return
        raise DashboardException(
            msg=f'Failed to lookup CephX user {client_name}: {ex}',
            component='cephfs.mirror') from ex

    auth = user_data[0] if isinstance(user_data, list) else user_data
    mds_caps = (auth.get('caps') or {}).get('mds', '')
    if not has_mirroring_mds_caps(mds_caps, fs_name):
        raise DashboardException(
            msg='Invalid capabilities on the MDS',
            code='invalid_mds_caps',
            component='cephfs.mirror')


class CephFS(object):
    @classmethod
    def list_filesystems(cls, all_info=False):
        fsmap = mgr.get("fs_map")

        if all_info:
            return fsmap['filesystems']
        return [{'id': fs['id'], 'name': fs['mdsmap']['fs_name']}
                for fs in fsmap['filesystems']]

    @classmethod
    def fs_name_from_id(cls, fs_id):
        """
        Get the filesystem name from ID.
        :param fs_id: The filesystem ID.
        :type fs_id: int | str
        :return: The filesystem name or None.
        :rtype: str | None
        """
        fs_map = mgr.get("fs_map")
        fs_info = list(filter(lambda x: str(x['id']) == str(fs_id),
                              fs_map['filesystems']))
        if not fs_info:
            return None
        return fs_info[0]['mdsmap']['fs_name']

    def __init__(self, fs_name=None):
        logger.debug("initializing cephfs connection")
        self.cfs = cephfs.LibCephFS(rados_inst=mgr.rados)
        logger.debug("mounting cephfs filesystem: %s", fs_name)
        if fs_name:
            self.cfs.mount(filesystem_name=fs_name)
        else:
            self.cfs.mount()
        logger.debug("mounted cephfs filesystem")

    @contextmanager
    def opendir(self, dirpath):
        d = None
        try:
            d = self.cfs.opendir(dirpath)
            yield d
        finally:
            if d:
                self.cfs.closedir(d)

    def ls_dir(self, path, depth):
        """
        List directories of specified path with additional information.
        :param path: The root directory path.
        :type path: str | bytes
        :param depth: The number of steps to go down the directory tree.
        :type depth: int | str
        :return: A list of directory dicts which consist of name, path,
            parent, snapshots and quotas.
        :rtype: list
        """
        paths = self._ls_dir(path, int(depth))
        # Convert (bytes => string), prettify paths (strip slashes)
        # and append additional information.
        return [self.get_directory(p) for p in paths if p != path.encode()]

    def _ls_dir(self, path, depth):
        """
        List directories of specified path.
        :param path: The root directory path.
        :type path: str | bytes
        :param depth: The number of steps to go down the directory tree.
        :type depth: int
        :return: A list of directory paths (bytes encoded).
            Example:
            ls_dir('/photos', 1) => [
                b'/photos/flowers', b'/photos/cars'
            ]
        :rtype: list
        """
        if isinstance(path, str):
            path = path.encode()
        logger.debug("get_dir_list dirpath=%s depth=%s", path,
                     depth)
        if depth == 0:
            return [path]
        logger.debug("opening dirpath=%s", path)
        with self.opendir(path) as d:
            dent = self.cfs.readdir(d)
            paths = [path]
            while dent:
                logger.debug("found entry=%s", dent.d_name)
                if dent.d_name in [b'.', b'..']:
                    dent = self.cfs.readdir(d)
                    continue
                if dent.is_dir():
                    logger.debug("found dir=%s", dent.d_name)
                    subdir_path = os.path.join(path, dent.d_name)
                    paths.extend(self._ls_dir(subdir_path, depth - 1))
                dent = self.cfs.readdir(d)
        return paths

    def get_directory(self, path):
        """
        Transforms path of directory into a meaningful dictionary.
        :param path: The root directory path.
        :type path: str | bytes
        :return: Dict consists of name, path, parent, snapshots and quotas.
        :rtype: dict
        """
        path = path.decode()
        not_root = path != os.sep
        return {
            'name': os.path.basename(path) if not_root else path,
            'path': path,
            'parent': os.path.dirname(path) if not_root else None,
            'snapshots': self.ls_snapshots(path),
            'quotas': self.get_quotas(path) if not_root else None
        }

    def dir_exists(self, path):
        try:
            with self.opendir(path):
                return True
        except cephfs.ObjectNotFound:
            return False

    def mk_dirs(self, path):
        """
        Create a directory.
        :param path: The path of the directory.
        """
        if path == os.sep:
            raise Exception('Cannot create root directory "/"')
        if self.dir_exists(path):
            return
        logger.info("Creating directory: %s", path)
        self.cfs.mkdirs(path, 0o755)

    def rm_dir(self, path):
        """
        Remove a directory.
        :param path: The path of the directory.
        """
        if path == os.sep:
            raise Exception('Cannot remove root directory "/"')
        if not self.dir_exists(path):
            return
        logger.info("Removing directory: %s", path)
        self.cfs.rmdir(path)

    def mk_snapshot(self, path, name=None, mode=0o755):
        """
        Create a snapshot.
        :param path: The path of the directory.
        :type path: str
        :param name: The name of the snapshot. If not specified,
            a name using the current time in RFC3339 UTC format
            will be generated.
        :type name: str | None
        :param mode: The permissions the directory should have
            once created.
        :type mode: int
        :return: Returns the name of the snapshot.
        :rtype: str
        """
        if name is None:
            now = datetime.datetime.now()
            tz = now.astimezone().tzinfo
            name = now.replace(tzinfo=tz).isoformat('T')
        client_snapdir = self.cfs.conf_get('client_snapdir')
        snapshot_path = os.path.join(path, client_snapdir, name)
        logger.info("Creating snapshot: %s", snapshot_path)
        self.cfs.mkdir(snapshot_path, mode)
        return name

    def ls_snapshots(self, path):
        """
        List snapshots for the specified path.
        :param path: The path of the directory.
        :type path: str
        :return: A list of dictionaries containing the name and the
          creation time of the snapshot.
        :rtype: list
        """
        result = []
        client_snapdir = self.cfs.conf_get('client_snapdir')
        path = os.path.join(path, client_snapdir).encode()
        with self.opendir(path) as d:
            dent = self.cfs.readdir(d)
            while dent:
                if dent.is_dir():
                    if dent.d_name not in [b'.', b'..'] and not dent.d_name.startswith(b'_'):
                        snapshot_path = os.path.join(path, dent.d_name)
                        stat = self.cfs.stat(snapshot_path)
                        result.append({
                            'name': dent.d_name.decode(),
                            'path': snapshot_path.decode(),
                            'created': '{}Z'.format(stat.st_ctime.isoformat('T'))
                        })
                dent = self.cfs.readdir(d)
        return result

    def rm_snapshot(self, path, name):
        """
        Remove a snapshot.
        :param path: The path of the directory.
        :type path: str
        :param name: The name of the snapshot.
        :type name: str
        """
        client_snapdir = self.cfs.conf_get('client_snapdir')
        snapshot_path = os.path.join(path, client_snapdir, name)
        logger.info("Removing snapshot: %s", snapshot_path)
        self.cfs.rmdir(snapshot_path)

    def get_quotas(self, path):
        """
        Get the quotas of the specified path.
        :param path: The path of the directory/file.
        :type path: str
        :return: Returns a dictionary containing 'max_bytes'
            and 'max_files'.
        :rtype: dict
        """
        try:
            max_bytes = int(self.cfs.getxattr(path, 'ceph.quota.max_bytes'))
        except cephfs.NoData:
            max_bytes = 0
        try:
            max_files = int(self.cfs.getxattr(path, 'ceph.quota.max_files'))
        except cephfs.NoData:
            max_files = 0
        return {'max_bytes': max_bytes, 'max_files': max_files}

    def set_quotas(self, path, max_bytes=None, max_files=None):
        """
        Set the quotas of the specified path.
        :param path: The path of the directory/file.
        :type path: str
        :param max_bytes: The byte limit.
        :type max_bytes: int | None
        :param max_files: The file limit.
        :type max_files: int | None
        """
        if max_bytes is not None:
            self.cfs.setxattr(path, 'ceph.quota.max_bytes',
                              str(max_bytes).encode(), 0)
        if max_files is not None:
            self.cfs.setxattr(path, 'ceph.quota.max_files',
                              str(max_files).encode(), 0)

    def write_to_file(self, path, buf) -> None:
        """
        Write some data to the specified path.
        :param path: The path of the file to write.
        :type path: str.
        :param buf: The str to write to the buf.
        :type buf: str.
        """
        try:
            fd = self.cfs.open(path, 'w', 644)
            self.cfs.write(fd, buf.encode('utf-8'), 0)
        except cephfs.Error as e:
            logger.debug("EIO: %s", str(e))
        finally:
            if fd is not None:
                self.cfs.close(fd)

    def unlink(self, path) -> None:
        """
        Removes a file, link, or symbolic link.
        :param path: The path of the file or link to unlink.
        """
        self.cfs.unlink(path)

    def statfs(self, path) -> dict:
        """
        Get the statfs of the specified path by xattr.
        :param path: The path of the directory/file.
        :type path: str
        :return: Returns a dictionary containing 'bytes',
            'files' and 'subdirs'.
        :rtype: dict
        """
        rbytes = 0
        rfiles = 0
        rsubdirs = 0
        with suppress(cephfs.NoData):
            rbytes = int(self.cfs.getxattr(path, 'ceph.dir.rbytes'))
            rfiles = int(self.cfs.getxattr(path, 'ceph.dir.rfiles'))
            rsubdirs = int(self.cfs.getxattr(path, 'ceph.dir.rsubdirs'))
        return {'bytes': rbytes, 'files': rfiles, 'subdirs': rsubdirs}

    def rename_path(self, src_path, dst_path) -> None:
        """
        Rename a file or directory.
        :param src: the path to the existing file or directory.
        :param dst: the new name of the file or directory.
        """
        logger.info("Renaming: from %s to %s", src_path, dst_path)
        self.cfs.rename(src_path, dst_path)
