import errno
import logging
import importlib

from .subvolume_base import SubvolumeBase
from .subvolume_v1 import SubvolumeV1
from .subvolume_v3 import SubvolumeV3
from .metadata_manager import MetadataManager
from ...exception import MetadataMgrException, VolumeException

log = logging.getLogger(__name__)

class SubvolumeLoader(object):
    INVALID_VERSION = -1

    SUPPORTED_MODULES = ['subvolume_v1.SubvolumeV1', 'subvolume_v2.SubvolumeV2',
                         'subvolume_v3.SubvolumeV3']

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

    def _get_subvolume_version(self, version):
        try:
            return self.versions[version]
        except KeyError:
            raise VolumeException(-errno.EINVAL, "subvolume class v{0} does not exist".format(version))

    def get_subvolume_object_max(self, mgr, fs, vol_spec, group, subvolname):
        return self._get_subvolume_version(self.max_version)(mgr, fs, vol_spec,
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

    def get_subvolume_object(self, mgr, fs, vol_spec, group, subvolname):
        base_subvol = SubvolumeBase(mgr, fs, vol_spec, group, subvolname)

        try:
            # disc = discovered
            disc_version = base_subvol.discover_version()

            if disc_version is False:
                raise VolumeException(-errno.ENOENT,
                                      f'subvolume "{base_subvol.name}" does '
                                      'not exist')
            elif disc_version in ('legacy', 'pre-legacy'):
                subvol_obj = SubvolumeV1(mgr, fs, vol_spec, group, subvolname,
                                         legacy=True)
            elif disc_version in (3, 2, 1):
                disc_uuid = base_subvol.discover_subvol_uuid(disc_version)
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
