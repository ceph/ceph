from mgr_util import CephfsClient, open_filesystem
from .exception import MDSPartException
import cephfs
import logging
import os
import errno

log = logging.getLogger(__name__)

class Vxattr:
    def __init__(self, mgr, fsc):
        log.debug("Init vxattr module.")
        self.mgr = mgr
        self.fsc = fsc

    def get_vxattr(self, fs_name: str, dir_path: str, key: str):
        with open_filesystem(self.fsc, fs_name) as fsh:
            try:
                dir_path = Vxattr.norm_path(dir_path)
                return fsh.getxattr(dir_path, key).decode('utf-8')
            except cephfs.Error as e:
                raise MDSPartException(-e.errno, f'error fetching {key} vxattr')

    def set_vxattr(self, fs_name: str, dir_path: str, key: str, value: str):
        with open_filesystem(self.fsc, fs_name) as fsh:
            try:
                dir_path = Vxattr.norm_path(dir_path)
                fsh.setxattr(dir_path, key, str(value).encode('utf-8'), 0)
            except cephfs.Error as e:
                raise MDSPartException(-e.errno, f'error storing {key}:{value} vxattr')

    def get_vxattr_values(self, fs_name: str, subtree_list: list):
        log.debug('get_vxattr_values')
        subtree_xattr: dict = {} 
        key_list = ['ceph.dir.pin', 'ceph.dir.pin.random', 'ceph.dir.pin.distributed', 'ceph.dir.rentries']
        for subtree in subtree_list:
            subtree_xattr[subtree] = {}
            for key in key_list:
                value = self.get_vxattr(fs_name, subtree, key)
                log.debug(f'get_vxattr {subtree}:{key}:{value}')
                subtree_xattr[subtree][key] = value
        return subtree_xattr

    def scan_subdir(self, fs_name: str, dir_path: str):
        log.debug('scan subdirs')
        dir_list = []
        with open_filesystem(self.fsc, fs_name) as fsh:
            with fsh.opendir(dir_path) as d_handle:
                dir_ = fsh.readdir(d_handle)
                while dir_:
                    d_name = dir_.d_name.decode('utf-8')
                    d_type = dir_.d_type
                    if d_type == 4 and d_name not in ['.', '..']:
                        dir_list.append(os.path.join(dir_path, d_name))
                    dir_ = fsh.readdir(d_handle)
        return dir_list

    @staticmethod
    def norm_path(dir_path):
        if not os.path.isabs(dir_path):
            raise MDSPartException(-errno.EINVAL, f'{dir_path} should be an absolute path')
        return os.path.normpath(dir_path)
