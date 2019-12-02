import os
import stat
import uuid
import errno
import logging

import cephfs

from .subvolume_base import SubvolumeBase
from ..op_sm import OpSm
from ..template import SubvolumeTemplate
from ..snapshot_util import mksnap, rmsnap
from ...exception import OpSmException, VolumeException, MetadataMgrException
from ...fs_util import listdir

log = logging.getLogger(__name__)

class SubvolumeV1(SubvolumeBase, SubvolumeTemplate):
    VERSION = 1

    @staticmethod
    def version():
        return SubvolumeV1.VERSION

    @property
    def path(self):
        try:
            # no need to stat the path -- open() does that
            return self.metadata_mgr.get_global_option('path').encode('utf-8')
        except MetadataMgrException as me:
            raise VolumeException(-errno.EINVAL, "error fetching subvolume metadata")

    def create(self, size, isolate_nspace, pool, mode, uid, gid):
        subvolume_type = SubvolumeBase.SUBVOLUME_TYPE_NORMAL
        try:
            initial_state = OpSm.get_init_state(subvolume_type)
        except OpSmException as oe:
            raise VolumeException(-errno.EINVAL, "subvolume creation failed: internal error")

        subvol_path = os.path.join(self.base_path, str(uuid.uuid4()).encode('utf-8'))
        try:
            # create directory and set attributes
            self.fs.mkdirs(subvol_path, mode)
            self._set_attrs(subvol_path, size, isolate_nspace, pool, uid, gid)

            # persist subvolume metadata
            qpath = subvol_path.decode('utf-8')
            self.init_config(SubvolumeV1.VERSION, subvolume_type, qpath, initial_state)
        except (VolumeException, MetadataMgrException, cephfs.Error) as e:
            try:
                log.info("cleaning up subvolume with path: {0}".format(self.subvolname))
                self.remove()
            except VolumeException as ve:
                log.info("failed to cleanup subvolume '{0}' ({1})".format(self.subvolname, ve))

            if isinstance(e, MetadataMgrException):
                log.error("metadata manager exception: {0}".format(e))
                e = VolumeException(-errno.EINVAL, "exception in subvolume metadata")
            elif isinstance(e, cephfs.Error):
                e = VolumeException(-e.args[0], e.args[1])
            raise e

    def add_clone_source(self, volname, subvolume, snapname, flush=False):
        self.metadata_mgr.add_section("source")
        self.metadata_mgr.update_section("source", "volume", volname)
        if not subvolume.group.is_default_group():
            self.metadata_mgr.update_section("source", "group", subvolume.group_name)
        self.metadata_mgr.update_section("source", "subvolume", subvolume.subvol_name)
        self.metadata_mgr.update_section("source", "snapshot", snapname)
        if flush:
            self.metadata_mgr.flush()

    def remove_clone_source(self, flush=False):
        self.metadata_mgr.remove_section("source")
        if flush:
            self.metadata_mgr.flush()

    def create_clone(self, pool, source_volname, source_subvolume, snapname):
        subvolume_type = SubvolumeBase.SUBVOLUME_TYPE_CLONE
        try:
            initial_state = OpSm.get_init_state(subvolume_type)
        except OpSmException as oe:
            raise VolumeException(-errno.EINVAL, "clone failed: internal error")

        subvol_path = os.path.join(self.base_path, str(uuid.uuid4()).encode('utf-8'))
        try:
            # create directory and set attributes
            self.fs.mkdirs(subvol_path, source_subvolume.mode)
            self._set_attrs(subvol_path, None, None, pool, source_subvolume.uid, source_subvolume.gid)

            # persist subvolume metadata and clone source
            qpath = subvol_path.decode('utf-8')
            self.metadata_mgr.init(SubvolumeV1.VERSION, subvolume_type, qpath, initial_state)
            self.add_clone_source(source_volname, source_subvolume, snapname)
            self.metadata_mgr.flush()
        except (VolumeException, MetadataMgrException, cephfs.Error) as e:
            try:
                log.info("cleaning up subvolume with path: {0}".format(self.subvolname))
                self.remove()
            except VolumeException as ve:
                log.info("failed to cleanup subvolume '{0}' ({1})".format(self.subvolname, ve))

            if isinstance(e, MetadataMgrException):
                log.error("metadata manager exception: {0}".format(e))
                e = VolumeException(-errno.EINVAL, "exception in subvolume metadata")
            elif isinstance(e, cephfs.Error):
                e = VolumeException(-e.args[0], e.args[1])
            raise e

    def open(self):
        try:
            self.metadata_mgr.refresh()
            subvol_path = self.path
            log.debug("refreshed metadata, checking subvolume path '{0}'".format(subvol_path))
            st = self.fs.stat(subvol_path)
            self.uid = int(st.st_uid)
            self.gid = int(st.st_gid)
            self.mode = int(st.st_mode & ~stat.S_IFMT(st.st_mode))
        except MetadataMgrException as me:
            if me.errno == -errno.ENOENT:
                raise VolumeException(-errno.ENOENT, "subvolume '{0}' does not exist".format(self.subvolname))
            raise VolumeException(me.args[0], me.args[1])
        except cephfs.ObjectNotFound:
            log.debug("missing subvolume path '{0}' for subvolume '{1}'".format(subvol_path, self.subvolname))
            raise VolumeException(-errno.ENOENT, "mount path missing for subvolume '{0}'".format(self.subvolname))
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    def remove(self):
        self.trash_base_dir()

    def resize(self, newsize, noshrink):
        subvol_path = self.path
        return self._resize(subvol_path, newsize, noshrink)

    def create_snapshot(self, snapname):
        snappath = os.path.join(self.path,
                                self.vol_spec.snapshot_dir_prefix.encode('utf-8'),
                                snapname.encode('utf-8'))
        mksnap(self.fs, snappath)

    def remove_snapshot(self, snapname):
        snappath = os.path.join(self.path,
                                self.vol_spec.snapshot_dir_prefix.encode('utf-8'),
                                snapname.encode('utf-8'))
        rmsnap(self.fs, snappath)

    def list_snapshots(self):
        try:
            dirpath = os.path.join(self.path,
                                   self.vol_spec.snapshot_dir_prefix.encode('utf-8'))
            return listdir(self.fs, dirpath)
        except VolumeException as ve:
            if ve.errno == -errno.ENOENT:
                return []
            raise
