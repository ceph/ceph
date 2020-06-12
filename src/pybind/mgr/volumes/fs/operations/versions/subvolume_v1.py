import os
import stat
import uuid
import errno
import logging
from datetime import datetime

import cephfs

from .metadata_manager import MetadataManager
from .subvolume_base import SubvolumeBase
from ..op_sm import OpSm
from ..template import SubvolumeTemplate
from ..snapshot_util import mksnap, rmsnap
from ...exception import IndexException, OpSmException, VolumeException, MetadataMgrException
from ...fs_util import listdir

from ..clone_index import open_clone_index, create_clone_index

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
            self.set_attrs(subvol_path, size, isolate_nspace, pool, uid, gid)

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
            self.set_attrs(subvol_path, None, None, pool, source_subvolume.uid, source_subvolume.gid)

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

    def open(self, need_complete=True, expected_types=[]):
        try:
            self.metadata_mgr.refresh()
            subvol_path = self.path
            log.debug("refreshed metadata, checking subvolume path '{0}'".format(subvol_path))
            st = self.fs.stat(subvol_path)
            etype = self.metadata_mgr.get_global_option(MetadataManager.GLOBAL_META_KEY_TYPE)
            if len(expected_types) and not etype in expected_types:
                raise VolumeException(-errno.ENOTSUP, "subvolume '{0}' is not {1}".format(
                    self.subvolname, "a {0}".format(expected_types[0]) if len(expected_types) == 1 else \
                    "one of types ({0})".format(",".join(expected_types))))
            if need_complete:
                estate = self.metadata_mgr.get_global_option(MetadataManager.GLOBAL_META_KEY_STATE)
                if not OpSm.is_final_state(estate):
                    raise VolumeException(-errno.EAGAIN, "subvolume '{0}' is not ready for use".format(self.subvolname))
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

    def _get_clone_source(self):
        try:
            clone_source = {
                'volume'   : self.metadata_mgr.get_option("source", "volume"),
                'subvolume': self.metadata_mgr.get_option("source", "subvolume"),
                'snapshot' : self.metadata_mgr.get_option("source", "snapshot"),
            }

            try:
                clone_source["group"] = self.metadata_mgr.get_option("source", "group")
            except MetadataMgrException as me:
                if me.errno == -errno.ENOENT:
                    pass
                else:
                    raise
        except MetadataMgrException as me:
            raise VolumeException(-errno.EINVAL, "error fetching subvolume metadata")
        return clone_source

    @property
    def status(self):
        state = self.metadata_mgr.get_global_option(MetadataManager.GLOBAL_META_KEY_STATE)
        subvolume_type = self.metadata_mgr.get_global_option(MetadataManager.GLOBAL_META_KEY_TYPE)
        subvolume_status = {
            'state' : state
        }
        if not OpSm.is_final_state(state) and subvolume_type == SubvolumeBase.SUBVOLUME_TYPE_CLONE:
            subvolume_status["source"] = self._get_clone_source()
        return subvolume_status

    @property
    def state(self):
        return self.metadata_mgr.get_global_option(MetadataManager.GLOBAL_META_KEY_STATE)

    @state.setter
    def state(self, val):
        state = val[0]
        flush = val[1]
        self.metadata_mgr.update_global_section(MetadataManager.GLOBAL_META_KEY_STATE, state)
        if flush:
            self.metadata_mgr.flush()

    def remove(self):
        self.trash_base_dir()

    def resize(self, newsize, noshrink):
        subvol_path = self.path
        return self._resize(subvol_path, newsize, noshrink)

    def snapshot_path(self, snapname):
        return os.path.join(self.path,
                            self.vol_spec.snapshot_dir_prefix.encode('utf-8'),
                            snapname.encode('utf-8'))

    def create_snapshot(self, snapname):
        snappath = self.snapshot_path(snapname)
        mksnap(self.fs, snappath)

    def is_snapshot_protected(self, snapname):
        try:
            self.metadata_mgr.get_option('protected snaps', snapname)
        except MetadataMgrException as me:
            if me.errno == -errno.ENOENT:
                return False
            else:
                log.warning("error checking protected snap {0} ({1})".format(snapname, me))
                raise VolumeException(-errno.EINVAL, "snapshot protection check failed")
        else:
            return True

    def has_pending_clones(self, snapname):
        try:
            return self.metadata_mgr.section_has_item('clone snaps', snapname)
        except MetadataMgrException as me:
            if me.errno == -errno.ENOENT:
                return False
            raise

    def remove_snapshot(self, snapname):
        if self.is_snapshot_protected(snapname):
            raise VolumeException(-errno.EINVAL, "snapshot '{0}' is protected".format(snapname))
        snappath = self.snapshot_path(snapname)
        rmsnap(self.fs, snappath)

    def snapshot_info(self, snapname):
        snappath = self.snapshot_path(snapname)
        snap_info = {}
        try:
            snap_attrs = {'created_at':'ceph.snap.btime', 'size':'ceph.dir.rbytes',
                          'data_pool':'ceph.dir.layout.pool'}
            for key, val in snap_attrs.items():
                snap_info[key] = self.fs.getxattr(snappath, val)
            return {'size': int(snap_info['size']),
                    'created_at': str(datetime.fromtimestamp(float(snap_info['created_at']))),
                    'data_pool': snap_info['data_pool'].decode('utf-8'),
                    'protected': "yes" if self.is_snapshot_protected(snapname) else "no",
                    'has_pending_clones': "yes" if self.has_pending_clones(snapname) else "no"}
        except cephfs.Error as e:
            if e.errno == errno.ENOENT:
                raise VolumeException(-errno.ENOENT,
                                      "snapshot '{0}' doesnot exist".format(snapname))
            raise VolumeException(-e.args[0], e.args[1])

    def list_snapshots(self):
        try:
            dirpath = os.path.join(self.path,
                                   self.vol_spec.snapshot_dir_prefix.encode('utf-8'))
            return listdir(self.fs, dirpath)
        except VolumeException as ve:
            if ve.errno == -errno.ENOENT:
                return []
            raise

    def _protect_snapshot(self, snapname):
        try:
            self.metadata_mgr.add_section("protected snaps")
            self.metadata_mgr.update_section("protected snaps", snapname, "1")
            self.metadata_mgr.flush()
        except MetadataMgrException as me:
            log.warning("error updating protected snap list ({0})".format(me))
            raise VolumeException(-errno.EINVAL, "error protecting snapshot")

    def _unprotect_snapshot(self, snapname):
        try:
            self.metadata_mgr.remove_option("protected snaps", snapname)
            self.metadata_mgr.flush()
        except MetadataMgrException as me:
            log.warning("error updating protected snap list ({0})".format(me))
            raise VolumeException(-errno.EINVAL, "error unprotecting snapshot")

    def protect_snapshot(self, snapname):
        if not snapname.encode('utf-8') in self.list_snapshots():
            raise VolumeException(-errno.ENOENT, "snapshot '{0}' does not exist".format(snapname))
        if self.is_snapshot_protected(snapname):
            raise VolumeException(-errno.EEXIST, "snapshot '{0}' is already protected".format(snapname))
        self._protect_snapshot(snapname)

    def unprotect_snapshot(self, snapname):
        if not snapname.encode('utf-8') in self.list_snapshots():
            raise VolumeException(-errno.ENOENT, "snapshot '{0}' does not exist".format(snapname))
        if not self.is_snapshot_protected(snapname):
            raise VolumeException(-errno.EEXIST, "snapshot '{0}' is not protected".format(snapname))
        if self.has_pending_clones(snapname):
            raise VolumeException(-errno.EEXIST, "snapshot '{0}' has pending clones".format(snapname))
        self._unprotect_snapshot(snapname)

    def _add_snap_clone(self, track_id, snapname):
        self.metadata_mgr.add_section("clone snaps")
        self.metadata_mgr.update_section("clone snaps", track_id, snapname)
        self.metadata_mgr.flush()

    def _remove_snap_clone(self, track_id):
        self.metadata_mgr.remove_option("clone snaps", track_id)
        self.metadata_mgr.flush()

    def attach_snapshot(self, snapname, tgt_subvolume):
        if not snapname.encode('utf-8') in self.list_snapshots():
            raise VolumeException(-errno.ENOENT, "snapshot '{0}' does not exist".format(snapname))
        if not self.is_snapshot_protected(snapname):
            raise VolumeException(-errno.EINVAL, "snapshot '{0}' is not protected".format(snapname))
        try:
            create_clone_index(self.fs, self.vol_spec)
            with open_clone_index(self.fs, self.vol_spec) as index:
                track_idx = index.track(tgt_subvolume.base_path)
                self._add_snap_clone(track_idx, snapname)
        except (IndexException, MetadataMgrException) as e:
            log.warning("error creating clone index: {0}".format(e))
            raise VolumeException(-errno.EINVAL, "error cloning subvolume")

    def detach_snapshot(self, snapname, track_id):
        if not snapname.encode('utf-8') in self.list_snapshots():
            raise VolumeException(-errno.ENOENT, "snapshot '{0}' does not exist".format(snapname))
        try:
            with open_clone_index(self.fs, self.vol_spec) as index:
                index.untrack(track_id)
                self._remove_snap_clone(track_id)
        except (IndexException, MetadataMgrException) as e:
            log.warning("error delining snapshot from clone: {0}".format(e))
            raise VolumeException(-errno.EINVAL, "error delinking snapshot from clone")
