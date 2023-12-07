import errno
import logging
import importlib

import cephfs

from .subvolume_base import SubvolumeBase
from .subvolume_attrs import SubvolumeTypes
from .subvolume_v1 import SubvolumeV1
from .subvolume_v2 import SubvolumeV2
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
            log.info("loaded v{0} subvolume".format(cls.version()))
            if self.max_version is not None or cls.version() > self.max_version:
                self.max_version = cls.version()
                self.versions[cls.version()] = cls
        if self.max_version == SubvolumeLoader.INVALID_VERSION:
            raise VolumeException(-errno.EINVAL, "no subvolume version available")
        log.info("max subvolume version is v{0}".format(self.max_version))

    def _get_subvolume_version(self, version):
        try:
            return self.versions[version]
        except KeyError:
            raise VolumeException(-errno.EINVAL, "subvolume class v{0} does not exist".format(version))

    def get_subvolume_object_max(self, mgr, fs, vol_spec, group, subvolname):
        return self._get_subvolume_version(self.max_version)(mgr, fs, vol_spec, group, subvolname)

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

    def upgrade_to_v2_subvolume(self, subvolume):
        # legacy mode subvolumes cannot be upgraded to v2
        if subvolume.legacy_mode:
            return

        version = int(subvolume.metadata_mgr.get_global_option('version'))
        if version >= SubvolumeV2.version():
            return

        if not self.allow_subvolume_upgrade(subvolume):
            return

        v1_subvolume = self._get_subvolume_version(version)(subvolume.mgr, subvolume.fs, subvolume.vol_spec, subvolume.group, subvolume.subvolname)
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

        subvolume.metadata_mgr.update_global_section(MetadataManager.GLOBAL_META_KEY_VERSION, SubvolumeV2.version())
        subvolume.metadata_mgr.flush()

    def upgrade_legacy_subvolume(self, fs, subvolume):
        assert subvolume.legacy_mode
        try:
            fs.mkdirs(subvolume.legacy_dir, 0o700)
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], "error accessing subvolume")
        subvolume_type = SubvolumeTypes.TYPE_NORMAL
        try:
            initial_state = SubvolumeOpSm.get_init_state(subvolume_type)
        except OpSmException:
            raise VolumeException(-errno.EINVAL, "subvolume creation failed: internal error")
        qpath = subvolume.base_path.decode('utf-8')
        # legacy is only upgradable to v1
        subvolume.init_config(SubvolumeV1.version(), subvolume_type, qpath, initial_state)

    def get_subvolume_object(self, mgr, fs, vol_spec, group, subvolname, upgrade=True):
        subvolume = SubvolumeBase(mgr, fs, vol_spec, group, subvolname)
        try:
            subvolume.discover()
            self.upgrade_to_v2_subvolume(subvolume)
            version = int(subvolume.metadata_mgr.get_global_option('version'))
            subvolume_version_object = self._get_subvolume_version(version)(mgr, fs, vol_spec, group, subvolname, legacy=subvolume.legacy_mode)
            subvolume_version_object.metadata_mgr.refresh()
            subvolume_version_object.clean_stale_snapshot_metadata()
            return subvolume_version_object
        except MetadataMgrException as me:
            if me.errno == -errno.ENOENT and upgrade:
                self.upgrade_legacy_subvolume(fs, subvolume)
                return self.get_subvolume_object(mgr, fs, vol_spec, group, subvolname, upgrade=False)
            else:
                # log the actual error and generalize error string returned to user
                log.error("error accessing subvolume metadata for '{0}' ({1})".format(subvolname, me))
                raise VolumeException(-errno.EINVAL, "error accessing subvolume metadata")

loaded_subvolumes = SubvolumeLoader()
loaded_subvolumes._load_supported_versions()
