import os
import stat
import uuid
import errno
import logging

import cephfs

from .metadata_manager import MetadataManager
from .subvolume_attrs import SubvolumeTypes, SubvolumeStates, SubvolumeFeatures
from .op_sm import SubvolumeOpSm
from .subvolume_v1 import SubvolumeV1
from ..template import SubvolumeTemplate
from ...exception import OpSmException, VolumeException, MetadataMgrException
from ..template import SubvolumeOpType

log = logging.getLogger(__name__)

class SubvolumeV2(SubvolumeV1):
    """
    Version 2 subvolumes creates a subvolume with path as follows,
        volumes/<group-name>/<subvolume-name>/<uuid>/

    The distinguishing feature of V2 subvolume as compared to V1 subvolumes is its ability to retain snapshots
    of a subvolume on removal. This is done by creating snapshots under the <subvolume-name> directory,
    rather than under the <uuid> directory, as is the case of V1 subvolumes.

    - The directory under which user data resides is <uuid>
    - Snapshots of the subvolume are taken within the <subvolume-name> directory
    - A meta file is maintained under the <subvolume-name> directory as a metadata store, storing information similar
    to V1 subvolumes
    - On a request to remove subvolume but retain its snapshots, only the <uuid> directory is moved to trash, retaining
    the rest of the subvolume and its meta file.
        - The <uuid> directory, when present, is the current incarnation of the subvolume, which may have snapshots of
        older incarnations of the same subvolume.
    - V1 subvolumes that currently do not have any snapshots are upgraded to V2 subvolumes automatically, to support the
    snapshot retention feature
    """
    VERSION = 2

    @staticmethod
    def version():
        return SubvolumeV2.VERSION

    @property
    def features(self):
        return [SubvolumeFeatures.FEATURE_SNAPSHOT_CLONE.value,
                SubvolumeFeatures.FEATURE_SNAPSHOT_AUTOPROTECT.value,
                SubvolumeFeatures.FEATURE_SNAPSHOT_RETENTION.value]

    @staticmethod
    def is_valid_uuid(uuid_str):
        try:
            uuid.UUID(uuid_str)
            return True
        except ValueError:
            return False

    def snapshot_base_path(self):
        return os.path.join(self.base_path, self.vol_spec.snapshot_dir_prefix.encode('utf-8'))

    def snapshot_data_path(self, snapname):
        snap_base_path = self.snapshot_path(snapname)
        uuid_str = None
        try:
            with self.fs.opendir(snap_base_path) as dir_handle:
                d = self.fs.readdir(dir_handle)
                while d:
                    if d.d_name not in (b".", b".."):
                        d_full_path = os.path.join(snap_base_path, d.d_name)
                        stx = self.fs.statx(d_full_path, cephfs.CEPH_STATX_MODE, cephfs.AT_SYMLINK_NOFOLLOW)
                        if stat.S_ISDIR(stx.get('mode')):
                            if self.is_valid_uuid(d.d_name.decode('utf-8')):
                                uuid_str = d.d_name
                    d = self.fs.readdir(dir_handle)
        except cephfs.Error as e:
            if e.errno == errno.ENOENT:
                raise VolumeException(-errno.ENOENT, "snapshot '{0}' does not exist".format(snapname))
            raise VolumeException(-e.args[0], e.args[1])

        if not uuid_str:
            raise VolumeException(-errno.ENOENT, "snapshot '{0}' does not exist".format(snapname))

        return os.path.join(snap_base_path, uuid_str)

    def _set_incarnation_metadata(self, subvolume_type, qpath, initial_state):
        self.metadata_mgr.update_global_section(MetadataManager.GLOBAL_META_KEY_TYPE, subvolume_type.value)
        self.metadata_mgr.update_global_section(MetadataManager.GLOBAL_META_KEY_PATH, qpath)
        self.metadata_mgr.update_global_section(MetadataManager.GLOBAL_META_KEY_STATE, initial_state.value)

    def create(self, size, isolate_nspace, pool, mode, uid, gid):
        subvolume_type = SubvolumeTypes.TYPE_NORMAL
        try:
            initial_state = SubvolumeOpSm.get_init_state(subvolume_type)
        except OpSmException as oe:
            raise VolumeException(-errno.EINVAL, "subvolume creation failed: internal error")

        subvol_path = os.path.join(self.base_path, str(uuid.uuid4()).encode('utf-8'))
        try:
            self.fs.mkdirs(subvol_path, mode)
            self.set_attrs(subvol_path, size, isolate_nspace, pool, uid, gid)

            # persist subvolume metadata
            qpath = subvol_path.decode('utf-8')
            try:
                self.metadata_mgr.refresh()
                if self.state == SubvolumeStates.STATE_RETAINED:
                    self._set_incarnation_metadata(subvolume_type, qpath, initial_state)
                    self.metadata_mgr.flush()
                else:
                    raise VolumeException(-errno.EINVAL, "invalid state for subvolume '{0}' during create".format(self.subvolname))
            except MetadataMgrException as me:
                if me.errno != -errno.ENOENT:
                    raise
                self.init_config(SubvolumeV2.VERSION, subvolume_type, qpath, initial_state)
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

    def create_clone(self, pool, source_volname, source_subvolume, snapname):
        subvolume_type = SubvolumeTypes.TYPE_CLONE
        try:
            initial_state = SubvolumeOpSm.get_init_state(subvolume_type)
        except OpSmException as oe:
            raise VolumeException(-errno.EINVAL, "clone failed: internal error")

        subvol_path = os.path.join(self.base_path, str(uuid.uuid4()).encode('utf-8'))
        try:
            stx = self.fs.statx(source_subvolume.snapshot_data_path(snapname),
                                cephfs.CEPH_STATX_MODE | cephfs.CEPH_STATX_UID | cephfs.CEPH_STATX_GID,
                                cephfs.AT_SYMLINK_NOFOLLOW)
            uid = stx.get('uid')
            gid = stx.get('gid')
            stx_mode = stx.get('mode')
            if stx_mode is not None:
                mode = stx_mode & ~stat.S_IFMT(stx_mode)
            else:
                mode = None

            # create directory and set attributes
            self.fs.mkdirs(subvol_path, mode)
            self.set_attrs(subvol_path, None, None, pool, uid, gid)

            # persist subvolume metadata and clone source
            qpath = subvol_path.decode('utf-8')
            try:
                self.metadata_mgr.refresh()
                if self.state == SubvolumeStates.STATE_RETAINED:
                    self._set_incarnation_metadata(subvolume_type, qpath, initial_state)
                else:
                    raise VolumeException(-errno.EINVAL, "invalid state for subvolume '{0}' during clone".format(self.subvolname))
            except MetadataMgrException as me:
                if me.errno != -errno.ENOENT:
                    raise
                self.metadata_mgr.init(SubvolumeV2.VERSION, subvolume_type.value, qpath, initial_state.value)
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

    def allowed_ops_by_type(self, vol_type):
        if vol_type == SubvolumeTypes.TYPE_CLONE:
            return {op_type for op_type in SubvolumeOpType}

        if vol_type == SubvolumeTypes.TYPE_NORMAL:
            return {op_type for op_type in SubvolumeOpType} - {SubvolumeOpType.CLONE_STATUS,
                                                               SubvolumeOpType.CLONE_CANCEL,
                                                               SubvolumeOpType.CLONE_INTERNAL}

        return {}

    def allowed_ops_by_state(self, vol_state):
        if vol_state == SubvolumeStates.STATE_COMPLETE:
            return {op_type for op_type in SubvolumeOpType}

        if vol_state == SubvolumeStates.STATE_RETAINED:
            return {
                SubvolumeOpType.REMOVE,
                SubvolumeOpType.REMOVE_FORCE,
                SubvolumeOpType.LIST,
                SubvolumeOpType.INFO,
                SubvolumeOpType.SNAP_REMOVE,
                SubvolumeOpType.SNAP_LIST,
                SubvolumeOpType.SNAP_INFO,
                SubvolumeOpType.SNAP_PROTECT,
                SubvolumeOpType.SNAP_UNPROTECT,
                SubvolumeOpType.CLONE_SOURCE
            }

        return {SubvolumeOpType.REMOVE_FORCE,
                SubvolumeOpType.CLONE_CREATE,
                SubvolumeOpType.CLONE_STATUS,
                SubvolumeOpType.CLONE_CANCEL,
                SubvolumeOpType.CLONE_INTERNAL,
                SubvolumeOpType.CLONE_SOURCE}

    def open(self, op_type):
        if not isinstance(op_type, SubvolumeOpType):
            raise VolumeException(-errno.ENOTSUP, "operation {0} not supported on subvolume '{1}'".format(
                                  op_type.value, self.subvolname))
        try:
            self.metadata_mgr.refresh()

            etype = self.subvol_type
            if op_type not in self.allowed_ops_by_type(etype):
                raise VolumeException(-errno.ENOTSUP, "operation '{0}' is not allowed on subvolume '{1}' of type {2}".format(
                                      op_type.value, self.subvolname, etype.value))

            estate = self.state
            if op_type not in self.allowed_ops_by_state(estate) and estate == SubvolumeStates.STATE_RETAINED:
                raise VolumeException(-errno.ENOENT, "subvolume '{0}' is removed and has only snapshots retained".format(
                                      self.subvolname))

            if op_type not in self.allowed_ops_by_state(estate) and estate != SubvolumeStates.STATE_RETAINED:
                raise VolumeException(-errno.EAGAIN, "subvolume '{0}' is not ready for operation {1}".format(
                                      self.subvolname, op_type.value))

            if estate != SubvolumeStates.STATE_RETAINED:
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

    def trash_incarnation_dir(self):
        self._trash_dir(self.path)

    def remove(self, retainsnaps=False):
        if self.list_snapshots():
            if not retainsnaps:
                raise VolumeException(-errno.ENOTEMPTY, "subvolume '{0}' has snapshots".format(self.subvolname))
            if self.state != SubvolumeStates.STATE_RETAINED:
                self.trash_incarnation_dir()
                self.metadata_mgr.update_global_section(MetadataManager.GLOBAL_META_KEY_PATH, "")
                self.metadata_mgr.update_global_section(MetadataManager.GLOBAL_META_KEY_STATE, SubvolumeStates.STATE_RETAINED.value)
                self.metadata_mgr.flush()
        else:
            self.trash_base_dir()

    def info(self):
        if self.state != SubvolumeStates.STATE_RETAINED:
            return super(SubvolumeV2, self).info()

        return {'type': self.subvol_type.value, 'features': self.features, 'state': SubvolumeStates.STATE_RETAINED.value}

    def remove_snapshot(self, snapname):
        super(SubvolumeV2, self).remove_snapshot(snapname)
        if self.state == SubvolumeStates.STATE_RETAINED and not self.list_snapshots():
            self.trash_base_dir()
            # tickle the volume purge job to purge this entry, using ESTALE
            raise VolumeException(-errno.ESTALE, "subvolume '{0}' has been removed as the last retained snapshot is removed".format(self.subvolname))
