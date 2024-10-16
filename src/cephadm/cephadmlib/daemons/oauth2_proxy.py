import logging
import os
from typing import Dict, List, Tuple, Optional
import re

from ..call_wrappers import call, CallVerbosity
from ..container_daemon_form import ContainerDaemonForm, daemon_to_container
from ..container_types import CephContainer
from ..context import CephadmContext
from ..context_getters import fetch_configs
from ..daemon_form import register as register_daemon_form
from ..daemon_identity import DaemonIdentity
from ..deployment_utils import to_deployment_container
from ceph.cephadm.images import DEFAULT_OAUTH2_PROXY_IMAGE
from ..constants import UID_NOBODY, GID_NOGROUP
from ..data_utils import dict_get, is_fsid
from ..file_utils import populate_files, makedirs, recursive_chown
from ..exceptions import Error


logger = logging.getLogger()


@register_daemon_form
class OAuth2Proxy(ContainerDaemonForm):
    """Define the configs for the jaeger tracing containers"""

    default_image = DEFAULT_OAUTH2_PROXY_IMAGE
    daemon_type = 'oauth2-proxy'
    required_files = [
        'oauth2-proxy.conf',
        'oauth2-proxy.crt',
        'oauth2-proxy.key',
    ]

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        return cls.daemon_type == daemon_type

    def __init__(
        self,
        ctx: CephadmContext,
        fsid: str,
        daemon_id: str,
        config_json: Dict,
        image: str = DEFAULT_OAUTH2_PROXY_IMAGE,
    ):
        self.ctx = ctx
        self.fsid = fsid
        self.daemon_id = daemon_id
        self.image = image
        self.files = dict_get(config_json, 'files', {})
        self.validate()

    @classmethod
    def init(
        cls, ctx: CephadmContext, fsid: str, daemon_id: str
    ) -> 'OAuth2Proxy':
        return cls(ctx, fsid, daemon_id, fetch_configs(ctx), ctx.image)

    @classmethod
    def create(
        cls, ctx: CephadmContext, ident: DaemonIdentity
    ) -> 'OAuth2Proxy':
        return cls.init(ctx, ident.fsid, ident.daemon_id)

    @property
    def identity(self) -> DaemonIdentity:
        return DaemonIdentity(self.fsid, self.daemon_type, self.daemon_id)

    def container(self, ctx: CephadmContext) -> CephContainer:
        ctr = daemon_to_container(ctx, self)
        return to_deployment_container(ctx, ctr)

    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        return UID_NOBODY, GID_NOGROUP

    def get_daemon_args(self) -> List[str]:
        return [
            '--config=/etc/oauth2-proxy.conf',
            '--tls-cert-file=/etc/oauth2-proxy.crt',
            '--tls-key-file=/etc/oauth2-proxy.key',
        ]

    def default_entrypoint(self) -> str:
        return ''

    def create_daemon_dirs(self, data_dir: str, uid: int, gid: int) -> None:
        """Create files under the container data dir"""
        if not os.path.isdir(data_dir):
            raise OSError('data_dir is not a directory: %s' % (data_dir))
        logger.info('Writing oauth2-proxy config...')
        config_dir = os.path.join(data_dir, 'etc/')
        makedirs(config_dir, uid, gid, 0o755)
        recursive_chown(config_dir, uid, gid)
        populate_files(config_dir, self.files, uid, gid)

    def validate(self) -> None:
        if not is_fsid(self.fsid):
            raise Error(f'not an fsid: {self.fsid}')
        if not self.daemon_id:
            raise Error(f'invalid daemon_id: {self.daemon_id}')
        if not self.image:
            raise Error(f'invalid image: {self.image}')

        # check for the required files
        if self.required_files:
            for fname in self.required_files:
                if fname not in self.files:
                    raise Error(
                        'required file missing from config-json: %s' % fname
                    )

    @staticmethod
    def get_version(ctx: CephadmContext, container_id: str) -> Optional[str]:
        """Return the version of the oauth2-proxy container"""
        version = None
        out, err, code = call(
            ctx,
            [
                ctx.container_engine.path,
                'exec',
                container_id,
                'oauth2-proxy',
                '--version',
            ],
            verbosity=CallVerbosity.QUIET,
        )
        if code == 0:
            match = re.search(r'oauth2-proxy (v\d+\.\d+\.\d+)', out)
            if match:
                version = match.group(1)
        return version

    def customize_container_mounts(
        self, ctx: CephadmContext, mounts: Dict[str, str]
    ) -> None:
        data_dir = self.identity.data_dir(ctx.data_dir)
        mounts.update(
            {
                os.path.join(
                    data_dir, 'etc/oauth2-proxy.conf'
                ): '/etc/oauth2-proxy.conf:Z',
                os.path.join(
                    data_dir, 'etc/oauth2-proxy.crt'
                ): '/etc/oauth2-proxy.crt:Z',
                os.path.join(
                    data_dir, 'etc/oauth2-proxy.key'
                ): '/etc/oauth2-proxy.key:Z',
            }
        )

    def customize_container_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        uid, _ = self.uid_gid(ctx)
        other_args = [
            '--user',
            str(uid),
        ]
        args.extend(other_args)

    def customize_process_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.extend(self.get_daemon_args())
