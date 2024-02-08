import logging
import os

from typing import Dict, List, Optional, Tuple, Union

from ..container_daemon_form import ContainerDaemonForm, daemon_to_container
from ..container_types import CephContainer
from ..context_getters import fetch_configs, get_config_and_keyring
from ..daemon_form import register as register_daemon_form
from ..daemon_identity import DaemonIdentity
from ..constants import DEFAULT_NVMEOF_IMAGE
from ..context import CephadmContext
from ..data_utils import dict_get, is_fsid
from ..deployment_utils import to_deployment_container
from ..exceptions import Error
from ..file_utils import makedirs, populate_files
from ..call_wrappers import call


logger = logging.getLogger()


@register_daemon_form
class CephNvmeof(ContainerDaemonForm):
    """Defines a Ceph-Nvmeof container"""

    daemon_type = 'nvmeof'
    required_files = ['ceph-nvmeof.conf']
    default_image = DEFAULT_NVMEOF_IMAGE

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        return cls.daemon_type == daemon_type

    def __init__(
        self, ctx, fsid, daemon_id, config_json, image=DEFAULT_NVMEOF_IMAGE
    ):
        # type: (CephadmContext, str, Union[int, str], Dict, str) -> None
        self.ctx = ctx
        self.fsid = fsid
        self.daemon_id = daemon_id
        self.image = image

        # config-json options
        self.files = dict_get(config_json, 'files', {})

        # validate the supplied args
        self.validate()

    @classmethod
    def init(cls, ctx, fsid, daemon_id):
        # type: (CephadmContext, str, Union[int, str]) -> CephNvmeof
        return cls(ctx, fsid, daemon_id, fetch_configs(ctx), ctx.image)

    @classmethod
    def create(
        cls, ctx: CephadmContext, ident: DaemonIdentity
    ) -> 'CephNvmeof':
        return cls.init(ctx, ident.fsid, ident.daemon_id)

    @property
    def identity(self) -> DaemonIdentity:
        return DaemonIdentity(self.fsid, self.daemon_type, self.daemon_id)

    @staticmethod
    def _get_container_mounts(data_dir: str, log_dir: str) -> Dict[str, str]:
        mounts = dict()
        mounts[os.path.join(data_dir, 'config')] = '/etc/ceph/ceph.conf:z'
        mounts[os.path.join(data_dir, 'keyring')] = '/etc/ceph/keyring:z'
        mounts[
            os.path.join(data_dir, 'ceph-nvmeof.conf')
        ] = '/src/ceph-nvmeof.conf:z'
        mounts[os.path.join(data_dir, 'configfs')] = '/sys/kernel/config'
        mounts['/dev/hugepages'] = '/dev/hugepages'
        mounts['/dev/vfio/vfio'] = '/dev/vfio/vfio'
        mounts[log_dir] = '/var/log/ceph:z'
        return mounts

    def customize_container_mounts(
        self, ctx: CephadmContext, mounts: Dict[str, str]
    ) -> None:
        data_dir = self.identity.data_dir(ctx.data_dir)
        log_dir = os.path.join(ctx.log_dir, self.identity.fsid)
        mounts.update(self._get_container_mounts(data_dir, log_dir))

    def customize_container_binds(
        self, ctx: CephadmContext, binds: List[List[str]]
    ) -> None:
        lib_modules = [
            'type=bind',
            'source=/lib/modules',
            'destination=/lib/modules',
            'ro=true',
        ]
        binds.append(lib_modules)

    @staticmethod
    def get_version(ctx: CephadmContext, container_id: str) -> Optional[str]:
        out, err, ret = call(
            ctx,
            [
                ctx.container_engine.path,
                'inspect',
                '--format',
                '{{index .Config.Labels "io.ceph.version"}}',
                ctx.image,
            ],
        )
        version = None
        if ret == 0:
            version = out.strip()
        return version

    def validate(self):
        # type: () -> None
        if not is_fsid(self.fsid):
            raise Error('not an fsid: %s' % self.fsid)
        if not self.daemon_id:
            raise Error('invalid daemon_id: %s' % self.daemon_id)
        if not self.image:
            raise Error('invalid image: %s' % self.image)

        # check for the required files
        if self.required_files:
            for fname in self.required_files:
                if fname not in self.files:
                    raise Error(
                        'required file missing from config-json: %s' % fname
                    )

    def get_daemon_name(self):
        # type: () -> str
        return '%s.%s' % (self.daemon_type, self.daemon_id)

    def get_container_name(self, desc=None):
        # type: (Optional[str]) -> str
        cname = '%s-%s' % (self.fsid, self.get_daemon_name())
        if desc:
            cname = '%s-%s' % (cname, desc)
        return cname

    def create_daemon_dirs(self, data_dir, uid, gid):
        # type: (str, int, int) -> None
        """Create files under the container data dir"""
        if not os.path.isdir(data_dir):
            raise OSError('data_dir is not a directory: %s' % (data_dir))

        logger.info('Creating ceph-nvmeof config...')
        configfs_dir = os.path.join(data_dir, 'configfs')
        makedirs(configfs_dir, uid, gid, 0o755)

        # populate files from the config-json
        populate_files(data_dir, self.files, uid, gid)

    @staticmethod
    def configfs_mount_umount(data_dir, mount=True):
        # type: (str, bool) -> List[str]
        mount_path = os.path.join(data_dir, 'configfs')
        if mount:
            cmd = (
                'if ! grep -qs {0} /proc/mounts; then '
                'mount -t configfs none {0}; fi'.format(mount_path)
            )
        else:
            cmd = (
                'if grep -qs {0} /proc/mounts; then '
                'umount {0}; fi'.format(mount_path)
            )
        return cmd.split()

    @staticmethod
    def get_sysctl_settings() -> List[str]:
        return [
            'vm.nr_hugepages = 4096',
        ]

    def container(self, ctx: CephadmContext) -> CephContainer:
        ctr = daemon_to_container(ctx, self)
        return to_deployment_container(ctx, ctr)

    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        return 167, 167  # TODO: need to get properly the uid/gid

    def config_and_keyring(
        self, ctx: CephadmContext
    ) -> Tuple[Optional[str], Optional[str]]:
        return get_config_and_keyring(ctx)

    def customize_container_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.append(ctx.container_engine.unlimited_pids_option)
        args.extend(['--ulimit', 'memlock=-1:-1'])
        args.extend(['--ulimit', 'nofile=10240'])
        args.extend(['--cap-add=SYS_ADMIN', '--cap-add=CAP_SYS_NICE'])
