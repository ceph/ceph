import errno
import logging
import importlib

import cephfs

from .subvolume_base import SubvolumeBase
from .subvolume_attrs import SubvolumeTypes
from .subvolume_v1 import SubvolumeV1
from .subvolume_v2 import SubvolumeV2
from .subvolume_v3 import SubvolumeV3
from .metadata_manager import MetadataManager
from .op_sm import SubvolumeOpSm
from ..template import SubvolumeOpType
from ...exception import MetadataMgrException, OpSmException, VolumeException

log = logging.getLogger(__name__)

class SubvolumeLoader(object):
    INVALID_VERSION = -1

    SUPPORTED_MODULES = ['subvolume_v1.SubvolumeV1', 'subvolume_v2.SubvolumeV2']

    def __init__(self):
        self.max_version = SubvolumeLoader.INVALID_VERSION
        self.versions = {}

    def _load_module(self, mod_cls):
        mod_name, cls_name = mod_cls.split('.')
        mod = importlib.import_module('.versions.{0}'.format(mod_name), package='volumes.fs.operations')
        return getattr(mod, cls_name)

    def _load_supported_versions(self):
        for mod_cls in SubvolumeLoader.SUPPORTED_MODULES:
            cls = self._load_module(mod_cls)
            log.info("loaded v{0} subvolume".format(cls.VERSION))
            if self.max_version is not None or cls.VERSION > self.max_version:
                self.max_version = cls.VERSION
                self.versions[cls.VERSION] = cls
        if self.max_version == SubvolumeLoader.INVALID_VERSION:
            raise VolumeException(-errno.EINVAL, "no subvolume version available")
        log.info("max subvolume version is v{0}".format(self.max_version))

    def _get_subvolume_class(self, version):
        try:
            return self.versions[version]
        except KeyError:
            raise VolumeException(-errno.EINVAL, "subvolume class v{0} does not exist".format(version))

    def get_subvolume_object_max(self, mgr, fs, vol_spec, group, subvolname):
        return self._get_subvolume_class(self.max_version)(mgr, fs, vol_spec,
                                                           group, subvolname)

    def allow_subvolume_upgrade(self, subvolume):
        asu = True
        try:
            opt = subvolume.metadata_mgr.get_global_option(MetadataManager.GLOBAL_META_KEY_ALLOW_SUBVOLUME_UPGRADE)
            asu = False if opt == "0" else True
        except MetadataMgrException:
            # this key is injected for QA testing and will not be available in
            # production
            pass

        return asu

    def upgrade_v1_to_v2(self, subvolume):
        # legacy mode subvolumes cannot be upgraded to v2
        if subvolume.legacy_mode:
            return

        version = int(subvolume.metadata_mgr.get_global_option('version'))
        if version >= SubvolumeV2.VERSION:
            return

        if not self.allow_subvolume_upgrade(subvolume):
            return

        v1_subvolume = self._get_subvolume_class(version)(
                subvolume.mgr, subvolume.fs, subvolume.vol_spec,
                subvolume.group, subvolume.subvolname)
        try:
            v1_subvolume.open(SubvolumeOpType.SNAP_LIST)
        except VolumeException as ve:
            # if volume is not ready for snapshot listing, do not upgrade at present
            if ve.errno == -errno.EAGAIN:
                return
            raise

        # v1 subvolumes with snapshots cannot be upgraded to v2
        if v1_subvolume.list_snapshots():
            return

        subvolume.metadata_mgr.update_global_section(
                MetadataManager.GLOBAL_META_KEY_VERSION, SubvolumeV2.VERSION)
        subvolume.metadata_mgr.flush()

    # v0 = legacy subvol
    def upgrade_v0_to_v1(self, fs, subvolume):
        assert subvolume.legacy_mode
        try:
            fs.mkdirs(subvolume.legacy_dir, 0o700)
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], "error accessing subvolume")
        subvolume_type = SubvolumeTypes.TYPE_NORMAL
        try:
            initial_state = SubvolumeOpSm.get_init_state(subvolume_type)
        except OpSmException as e:
            raise VolumeException(-errno.EINVAL,
                                  "subvolume creation failed due to internal "
                                  f"error, exception: {e}")
        qpath = subvolume.base_path.decode('utf-8')
        # legacy is only upgradable to v1
        subvolume.init_config(SubvolumeV1.VERSION, subvolume_type, qpath,
                              initial_state)

    def get_subvolume_object(self, mgr, fs, vol_spec, group, subvolname):
        base_subvol = SubvolumeBase(mgr, fs, vol_spec, group, subvolname)

        try:
            # disc = discovered
            disc_version, disc_uuid = base_subvol.discover()

            if disc_version is False:
                raise VolumeException(-errno.ENOENT,
                                      f'subvolume "{self.name}" does not exist')
            # legacy
            elif disc_version is None:
                self.upgrade_v0_to_v1(fs, base_subvol)
                disc_version, disc_uuid = SubvolumeBase(mgr, fs, vol_spec, group,
                                                        subvolname).discover()
                assert disc_version == 1, \
                        f'disc_version = {disc_version}'

                subvol_class = self._get_subvolume_class(disc_version)
                subvol_obj = subvol_class(mgr, fs, vol_spec, group, subvolname,
                                          legacy=True)
            if disc_version <= 2:
                self.upgrade_v1_to_v2(base_subvol)
                subvol_class = self._get_subvolume_class(disc_version)
                subvol_obj = subvol_class(mgr, fs, vol_spec, group, subvolname,
                                          legacy=base_subvol.legacy_mode)
            elif disc_version == 3:
                subvol_obj = SubvolumeV3(mgr, fs, vol_spec, group, subvolname,
                                         disc_uuid, disc_version)
            else:
                assert False, \
                        f'invalid version. disc_version = {disc_version}'

            subvol_obj.metadata_mgr.refresh()
            subvol_obj.clean_stale_snapshot_metadata()
            return subvol_obj
        except MetadataMgrException as me:
            msg = f'error accessing metadata for subvol "{subvolname}": {me}'
            log.error(msg)
            raise VolumeException(-errno.EINVAL, msg)

loaded_subvolumes = SubvolumeLoader()
loaded_subvolumes._load_supported_versions()
