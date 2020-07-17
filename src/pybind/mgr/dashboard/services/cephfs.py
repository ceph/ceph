# -*- coding: utf-8 -*-
from __future__ import absolute_import

from contextlib import contextmanager
import logging

import datetime
import os
import cephfs

from .. import mgr


logger = logging.getLogger('cephfs')


class CephFS(object):
    @classmethod
    def list_filesystems(cls):
        fsmap = mgr.get("fs_map")
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

    def __del__(self):
        logger.debug("shutting down cephfs filesystem")
        self.cfs.shutdown()

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
