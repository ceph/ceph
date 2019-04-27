# -*- coding: utf-8 -*-
from __future__ import absolute_import

from contextlib import contextmanager

import cephfs

from .. import mgr, logger


class CephFS(object):
    @classmethod
    def list_filesystems(cls):
        fsmap = mgr.get("fs_map")
        return [{'id': fs['id'], 'name': fs['mdsmap']['fs_name']}
                for fs in fsmap['filesystems']]

    def __init__(self, fs_name=None):
        logger.debug("[CephFS] initializing cephfs connection")
        self.cfs = cephfs.LibCephFS(rados_inst=mgr.rados)
        logger.debug("[CephFS] mounting cephfs filesystem: %s", fs_name)
        if fs_name:
            self.cfs.mount(filesystem_name=fs_name)
        else:
            self.cfs.mount()
        logger.debug("[CephFS] mounted cephfs filesystem")

    def __del__(self):
        logger.debug("[CephFS] shutting down cephfs filesystem")
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

    def get_dir_list(self, dirpath, level):
        logger.debug("[CephFS] get_dir_list dirpath=%s level=%s", dirpath,
                     level)
        if level == 0:
            return [dirpath]
        logger.debug("[CephFS] opening dirpath=%s", dirpath)
        with self.opendir(dirpath) as d:
            dent = self.cfs.readdir(d)
            paths = [dirpath]
            while dent:
                logger.debug("[CephFS] found entry=%s", dent.d_name)
                if dent.d_name in ['.', '..']:
                    dent = self.cfs.readdir(d)
                    continue
                if dent.is_dir():
                    logger.debug("[CephFS] found dir=%s", dent.d_name)
                    subdirpath = '{}{}/'.format(dirpath, dent.d_name)
                    paths.extend(self.get_dir_list(subdirpath, level-1))
                dent = self.cfs.readdir(d)
        return paths

    def dir_exists(self, dirpath):
        try:
            with self.opendir(dirpath):
                return True
        except cephfs.ObjectNotFound:
            return False

    def mkdirs(self, dirpath):
        if dirpath == '/':
            raise Exception('Cannot create root directory "/"')
        if self.dir_exists(dirpath):
            return

        logger.info("[CephFS] Creating directory: %s", dirpath)
        self.cfs.mkdirs("{}".format(dirpath).encode('utf-8'), 0o755)
