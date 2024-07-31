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
from ...exception import OpSmException, VolumeException, MetadataMgrException
from ...fs_util import listdir, create_base_dir
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

    @property
    def retained(self):
        try:
            self.metadata_mgr.refresh()
            if self.state == SubvolumeStates.STATE_RETAINED:
                return True
            return False
        except MetadataMgrException as me:
            if me.errno != -errno.ENOENT:
                raise VolumeException(me.errno, "internal error while processing subvolume '{0}'".format(self.subvolname))
        return False

    @property
    def purgeable(self):
        if not self.retained or self.list_snapshots() or self.has_pending_purges:
            return False
        return True

    @property
    def has_pending_purges(self):
        try:
            return not listdir(self.fs, self.trash_dir) == []
        except VolumeException as ve:
            if ve.errno == -errno.ENOENT:
                return False
            raise

    @property
    def trash_dir(self):
        return os.path.join(self.base_path, b".trash")

    def create_trashcan(self):
        """per subvolume trash directory"""
        try:
            self.fs.stat(self.trash_dir)
        except cephfs.Error as e:
            if e.args[0] == errno.ENOENT:
                try:
                    self.fs.mkdir(self.trash_dir, 0o700)
                except cephfs.Error as ce:
                    raise VolumeException(-ce.args[0], ce.args[1])
            else:
                raise VolumeException(-e.args[0], e.args[1])

    def mark_subvolume(self):
        # set subvolume attr, on subvolume root, marking it as a CephFS subvolume
        # subvolume root is where snapshots would be taken, and hence is the base_path for v2 subvolumes
        try:
            # MDS treats this as a noop for already marked subvolume
            self.fs.setxattr(self.base_path, 'ceph.dir.subvolume', b'1', 0)
        except cephfs.InvalidValue:
            raise VolumeException(-errno.EINVAL, "invalid value specified for ceph.dir.subvolume")
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

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

    def _remove_on_failure(self, subvol_path, retained):
        if retained:
            log.info("cleaning up subvolume incarnation with path: {0}".format(subvol_path))
            try:
                self.fs.rmdir(subvol_path)
            except cephfs.Error as e:
                raise VolumeException(-e.args[0], e.args[1])
        else:
            log.info("cleaning up subvolume with path: {0}".format(self.subvolname))
            self.remove(internal_cleanup=True)

    def _set_incarnation_metadata(self, subvolume_type, qpath, initial_state):
        self.metadata_mgr.update_global_section(MetadataManager.GLOBAL_META_KEY_TYPE, subvolume_type.value)
        self.metadata_mgr.update_global_section(MetadataManager.GLOBAL_META_KEY_PATH, qpath)
        self.metadata_mgr.update_global_section(MetadataManager.GLOBAL_META_KEY_STATE, initial_state.value)

    def create(self, size, isolate_nspace, pool, mode, uid, gid):
        subvolume_type = SubvolumeTypes.TYPE_NORMAL
        try:
            initial_state = SubvolumeOpSm.get_init_state(subvolume_type)
        except OpSmException:
            raise VolumeException(-errno.EINVAL, "subvolume creation failed: internal error")

        retained = self.retained
        if retained and self.has_pending_purges:
            raise VolumeException(-errno.EAGAIN, "asynchronous purge of subvolume in progress")
        subvol_path = os.path.join(self.base_path, str(uuid.uuid4()).encode('utf-8'))
        try:
            # create group directory with default mode(0o755) if it doesn't exist.
            create_base_dir(self.fs, self.group.path, self.vol_spec.DEFAULT_MODE)
            self.fs.mkdirs(subvol_path, mode)
            self.mark_subvolume()
            attrs = {
                'uid': uid,
                'gid': gid,
                'data_pool': pool,
                'pool_namespace': self.namespace if isolate_nspace else None,
                'quota': size
            }
            self.set_attrs(subvol_path, attrs)

            # persist subvolume metadata
            qpath = subvol_path.decode('utf-8')
            if retained:
                self._set_incarnation_metadata(subvolume_type, qpath, initial_state)
                self.metadata_mgr.flush()
            else:
                self.init_config(SubvolumeV2.VERSION, subvolume_type, qpath, initial_state)

            # Create the subvolume metadata file which manages auth-ids if it doesn't exist
            self.auth_mdata_mgr.create_subvolume_metadata_file(self.group.groupname, self.subvolname)
        except (VolumeException, MetadataMgrException, cephfs.Error) as e:
            try:
                self._remove_on_failure(subvol_path, retained)
            except VolumeException as ve:
                log.info("failed to cleanup subvolume '{0}' ({1})".format(self.subvolname, ve))

            if isinstance(e, MetadataMgrException):
                log.error("metadata manager exception: {0}".format(e))
                e = VolumeException(-errno.EINVAL, f"exception in subvolume metadata: {os.strerror(-e.args[0])}")
            elif isinstance(e, cephfs.Error):
                e = VolumeException(-e.args[0], e.args[1])
            raise e

    def create_clone(self, pool, source_volname, source_subvolume, snapname):
        subvolume_type = SubvolumeTypes.TYPE_CLONE
        try:
            initial_state = SubvolumeOpSm.get_init_state(subvolume_type)
        except OpSmException:
            raise VolumeException(-errno.EINVAL, "clone failed: internal error")

        retained = self.retained
        if retained and self.has_pending_purges:
            raise VolumeException(-errno.EAGAIN, "asynchronous purge of subvolume in progress")
        subvol_path = os.path.join(self.base_path, str(uuid.uuid4()).encode('utf-8'))
        try:
            # source snapshot attrs are used to create clone subvolume
            # attributes of subvolume's content though, are synced during the cloning process.
            attrs = source_subvolume.get_attrs(source_subvolume.snapshot_data_path(snapname))

            # The source of the clone may have exceeded its quota limit as
            # CephFS quotas are imprecise. Cloning such a source may fail if
            # the quota on the destination is set before starting the clone
            # copy. So always set the quota on destination after cloning is
            # successful.
            attrs["quota"] = None

            # override snapshot pool setting, if one is provided for the clone
            if pool is not None:
                attrs["data_pool"] = pool
                attrs["pool_namespace"] = None

            # create directory and set attributes
            self.fs.mkdirs(subvol_path, attrs.get("mode"))
            self.mark_subvolume()
            self.set_attrs(subvol_path, attrs)

            # persist subvolume metadata and clone source
            qpath = subvol_path.decode('utf-8')
            if retained:
                self._set_incarnation_metadata(subvolume_type, qpath, initial_state)
            else:
                self.metadata_mgr.init(SubvolumeV2.VERSION, subvolume_type.value, qpath, initial_state.value)
            self.add_clone_source(source_volname, source_subvolume, snapname)
            self.metadata_mgr.flush()
        except (VolumeException, MetadataMgrException, cephfs.Error) as e:
            try:
                self._remove_on_failure(subvol_path, retained)
            except VolumeException as ve:
                log.info("failed to cleanup subvolume '{0}' ({1})".format(self.subvolname, ve))

            if isinstance(e, MetadataMgrException):
                log.error("metadata manager exception: {0}".format(e))
                e = VolumeException(-errno.EINVAL, f"exception in subvolume metadata: {os.strerror(-e.args[0])}")
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
            # unconditionally mark as subvolume, to handle pre-existing subvolumes without the mark
            self.mark_subvolume()

            etype = self.subvol_type
            if op_type not in self.allowed_ops_by_type(etype):
                raise VolumeException(-errno.ENOTSUP, "operation '{0}' is not allowed on subvolume '{1}' of type {2}".format(
                                      op_type.value, self.subvolname, etype.value))

            estate = self.state
            if op_type not in self.allowed_ops_by_state(estate):
                if estate == SubvolumeStates.STATE_RETAINED:
                    raise VolumeException(
                        -errno.ENOENT,
                        f'subvolume "{self.subvolname}" is removed and has '
                        'only snapshots retained')
                else:
                    raise VolumeException(
                        -errno.EAGAIN,
                        f'subvolume "{self.subvolname}" is not ready for '
                        f'operation "{op_type.value}"')

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
        """rename subvolume (uuid component) to trash"""
        self.create_trashcan()
        try:
            bname = os.path.basename(self.path)
            tpath = os.path.join(self.trash_dir, bname)
            log.debug("trash: {0} -> {1}".format(self.path, tpath))
            self.fs.rename(self.path, tpath)
            self._link_dir(tpath, bname)
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], e.args[1])

    @staticmethod
    def safe_to_remove_subvolume_clone(subvol_state):
        # Both the STATE_FAILED and STATE_CANCELED are handled by 'handle_clone_failed' in the state
        # machine which removes the entry from the index. Hence, it's safe to removed clone with
        # force option for both.
        acceptable_rm_clone_states = [SubvolumeStates.STATE_COMPLETE, SubvolumeStates.STATE_CANCELED,
                                      SubvolumeStates.STATE_FAILED, SubvolumeStates.STATE_RETAINED]
        if subvol_state not in acceptable_rm_clone_states:
            return False
        return True

    def remove(self, retainsnaps=False, internal_cleanup=False):
        if self.list_snapshots():
            if not retainsnaps:
                raise VolumeException(-errno.ENOTEMPTY, "subvolume '{0}' has snapshots".format(self.subvolname))
        else:
            if not internal_cleanup and not self.safe_to_remove_subvolume_clone(self.state):
                raise VolumeException(-errno.EAGAIN,
                                      "{0} clone in-progress -- please cancel the clone and retry".format(self.subvolname))
            if not self.has_pending_purges:
                self.trash_base_dir()
                # Delete the volume meta file, if it's not already deleted
                self.auth_mdata_mgr.delete_subvolume_metadata_file(self.group.groupname, self.subvolname)
                return
        if self.state != SubvolumeStates.STATE_RETAINED:
            self.trash_incarnation_dir()
            self.metadata_mgr.remove_section(MetadataManager.USER_METADATA_SECTION)
            self.metadata_mgr.update_global_section(MetadataManager.GLOBAL_META_KEY_PATH, "")
            self.metadata_mgr.update_global_section(MetadataManager.GLOBAL_META_KEY_STATE, SubvolumeStates.STATE_RETAINED.value)
            self.metadata_mgr.flush()
            # Delete the volume meta file, if it's not already deleted
            self.auth_mdata_mgr.delete_subvolume_metadata_file(self.group.groupname, self.subvolname)

    def info(self):
        if self.state != SubvolumeStates.STATE_RETAINED:
            return super(SubvolumeV2, self).info()

        return {'type': self.subvol_type.value, 'features': self.features, 'state': SubvolumeStates.STATE_RETAINED.value}

    def remove_snapshot(self, snapname, force=False):
        super(SubvolumeV2, self).remove_snapshot(snapname, force)
        if self.purgeable:
            self.trash_base_dir()
            # tickle the volume purge job to purge this entry, using ESTALE
            raise VolumeException(-errno.ESTALE, "subvolume '{0}' has been removed as the last retained snapshot is removed".format(self.subvolname))
        # if not purgeable, subvol is not retained, or has snapshots, or already has purge jobs that will garbage collect this subvol
