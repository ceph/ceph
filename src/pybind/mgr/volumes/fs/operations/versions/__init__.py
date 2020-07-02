import errno
import logging
import importlib

import cephfs

from .subvolume_base import SubvolumeBase
from ..op_sm import SubvolumeOpSm
from ..op_sm import SubvolumeTypes
from ...exception import MetadataMgrException, OpSmException, VolumeException

log = logging.getLogger(__name__)

class SubvolumeLoader(object):
    INVALID_VERSION = -1

    SUPPORTED_MODULES = ['subvolume_v1.SubvolumeV1']

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

    def get_subvolume_object_max(self, fs, vol_spec, group, subvolname):
        return self._get_subvolume_version(self.max_version)(fs, vol_spec, group, subvolname)

    def upgrade_legacy_subvolume(self, fs, subvolume):
        assert subvolume.legacy_mode
        try:
            fs.mkdirs(subvolume.legacy_dir, 0o700)
        except cephfs.Error as e:
            raise VolumeException(-e.args[0], "error accessing subvolume")
        subvolume_type = SubvolumeTypes.TYPE_NORMAL
        try:
            initial_state = SubvolumeOpSm.get_init_state(subvolume_type)
        except OpSmException as oe:
            raise VolumeException(-errno.EINVAL, "subvolume creation failed: internal error")
        qpath = subvolume.base_path.decode('utf-8')
        subvolume.init_config(self.max_version, subvolume_type, qpath, initial_state)

    def get_subvolume_object(self, fs, vol_spec, group, subvolname, upgrade=True):
        subvolume = SubvolumeBase(fs, vol_spec, group, subvolname)
        try:
            subvolume.discover()
            version = int(subvolume.metadata_mgr.get_global_option('version'))
            return self._get_subvolume_version(version)(fs, vol_spec, group, subvolname, legacy=subvolume.legacy_mode)
        except MetadataMgrException as me:
            if me.errno == -errno.ENOENT and upgrade:
                self.upgrade_legacy_subvolume(fs, subvolume)
                return self.get_subvolume_object(fs, vol_spec, group, subvolname, upgrade=False)
            else:
                # log the actual error and generalize error string returned to user
                log.error("error accessing subvolume metadata for '{0}' ({1})".format(subvolname, me))
                raise VolumeException(-errno.EINVAL, "error accessing subvolume metadata")

loaded_subvolumes = SubvolumeLoader()
loaded_subvolumes._load_supported_versions()
