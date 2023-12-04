import logging
import os
import re

from typing import Dict, List, Optional, Tuple, Union

from ..call_wrappers import call, CallVerbosity
from ..constants import DEFAULT_IMAGE, CEPH_DEFAULT_CONF
from ..container_daemon_form import ContainerDaemonForm, daemon_to_container
from ..container_types import CephContainer, extract_uid_gid
from ..context import CephadmContext
from ..context_getters import fetch_configs, get_config_and_keyring
from ..daemon_form import register as register_daemon_form
from ..daemon_identity import DaemonIdentity
from ..data_utils import dict_get, is_fsid
from ..deploy import DeploymentType
from ..deployment_utils import to_deployment_container
from ..exceptions import Error
from ..file_utils import makedirs, populate_files, write_new
from ..net_utils import EndPoint


logger = logging.getLogger()


@register_daemon_form
class NFSGanesha(ContainerDaemonForm):
    """Defines a NFS-Ganesha container"""

    daemon_type = 'nfs'
    entrypoint = '/usr/bin/ganesha.nfsd'
    daemon_args = ['-F', '-L', 'STDERR']

    required_files = ['ganesha.conf']

    port_map = {
        'nfs': 2049,
    }

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        return cls.daemon_type == daemon_type

    def __init__(
        self, ctx, fsid, daemon_id, config_json, image=DEFAULT_IMAGE
    ):
        # type: (CephadmContext, str, Union[int, str], Dict, str) -> None
        self.ctx = ctx
        self.fsid = fsid
        self.daemon_id = daemon_id
        self.image = image

        # config-json options
        self.pool = dict_get(config_json, 'pool', require=True)
        self.namespace = dict_get(config_json, 'namespace')
        self.userid = dict_get(config_json, 'userid')
        self.extra_args = dict_get(config_json, 'extra_args', [])
        self.files = dict_get(config_json, 'files', {})
        self.rgw = dict_get(config_json, 'rgw', {})

        # validate the supplied args
        self.validate()

    @classmethod
    def init(cls, ctx, fsid, daemon_id):
        # type: (CephadmContext, str, Union[int, str]) -> NFSGanesha
        return cls(ctx, fsid, daemon_id, fetch_configs(ctx), ctx.image)

    @classmethod
    def create(
        cls, ctx: CephadmContext, ident: DaemonIdentity
    ) -> 'NFSGanesha':
        return cls.init(ctx, ident.fsid, ident.daemon_id)

    @property
    def identity(self) -> DaemonIdentity:
        return DaemonIdentity(self.fsid, self.daemon_type, self.daemon_id)

    def _get_container_mounts(self, data_dir):
        # type: (str) -> Dict[str, str]
        mounts = dict()
        mounts[os.path.join(data_dir, 'config')] = '/etc/ceph/ceph.conf:z'
        mounts[os.path.join(data_dir, 'keyring')] = '/etc/ceph/keyring:z'
        mounts[os.path.join(data_dir, 'etc/ganesha')] = '/etc/ganesha:z'
        if self.rgw:
            cluster = self.rgw.get('cluster', 'ceph')
            rgw_user = self.rgw.get('user', 'admin')
            mounts[
                os.path.join(data_dir, 'keyring.rgw')
            ] = '/var/lib/ceph/radosgw/%s-%s/keyring:z' % (cluster, rgw_user)
        return mounts

    def customize_container_mounts(
        self, ctx: CephadmContext, mounts: Dict[str, str]
    ) -> None:
        data_dir = self.identity.data_dir(ctx.data_dir)
        mounts.update(self._get_container_mounts(data_dir))

    @staticmethod
    def get_container_envs():
        # type: () -> List[str]
        envs = ['CEPH_CONF=%s' % (CEPH_DEFAULT_CONF)]
        return envs

    @staticmethod
    def get_version(ctx, container_id):
        # type: (CephadmContext, str) -> Optional[str]
        version = None
        out, err, code = call(
            ctx,
            [
                ctx.container_engine.path,
                'exec',
                container_id,
                NFSGanesha.entrypoint,
                '-v',
            ],
            verbosity=CallVerbosity.QUIET,
        )
        if code == 0:
            match = re.search(r'NFS-Ganesha Release\s*=\s*[V]*([\d.]+)', out)
            if match:
                version = match.group(1)
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

        # check for an RGW config
        if self.rgw:
            if not self.rgw.get('keyring'):
                raise Error('RGW keyring is missing')
            if not self.rgw.get('user'):
                raise Error('RGW user is missing')

    def get_daemon_name(self):
        # type: () -> str
        return '%s.%s' % (self.daemon_type, self.daemon_id)

    def get_container_name(self, desc=None):
        # type: (Optional[str]) -> str
        cname = 'ceph-%s-%s' % (self.fsid, self.get_daemon_name())
        if desc:
            cname = '%s-%s' % (cname, desc)
        return cname

    def get_daemon_args(self):
        # type: () -> List[str]
        return self.daemon_args + self.extra_args

    def create_daemon_dirs(self, data_dir, uid, gid):
        # type: (str, int, int) -> None
        """Create files under the container data dir"""
        if not os.path.isdir(data_dir):
            raise OSError('data_dir is not a directory: %s' % (data_dir))

        logger.info('Creating ganesha config...')

        # create the ganesha conf dir
        config_dir = os.path.join(data_dir, 'etc/ganesha')
        makedirs(config_dir, uid, gid, 0o755)

        # populate files from the config-json
        populate_files(config_dir, self.files, uid, gid)

        # write the RGW keyring
        if self.rgw:
            keyring_path = os.path.join(data_dir, 'keyring.rgw')
            with write_new(keyring_path, owner=(uid, gid)) as f:
                f.write(self.rgw.get('keyring', ''))

    def firewall_service_name(self) -> str:
        return 'nfs'

    def container(self, ctx: CephadmContext) -> CephContainer:
        ctr = daemon_to_container(ctx, self)
        return to_deployment_container(ctx, ctr)

    def customize_container_endpoints(
        self, endpoints: List[EndPoint], deployment_type: DeploymentType
    ) -> None:
        if deployment_type == DeploymentType.DEFAULT and not endpoints:
            nfs_ports = list(NFSGanesha.port_map.values())
            endpoints.extend([EndPoint('0.0.0.0', p) for p in nfs_ports])

    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        # TODO: extract ganesha uid/gid (997, 994) ?
        return extract_uid_gid(ctx)

    def config_and_keyring(
        self, ctx: CephadmContext
    ) -> Tuple[Optional[str], Optional[str]]:
        return get_config_and_keyring(ctx)

    def customize_container_envs(
        self, ctx: CephadmContext, envs: List[str]
    ) -> None:
        envs.extend(self.get_container_envs())

    def customize_process_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.extend(self.get_daemon_args())

    def customize_container_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.append(ctx.container_engine.unlimited_pids_option)

    def default_entrypoint(self) -> str:
        return self.entrypoint
