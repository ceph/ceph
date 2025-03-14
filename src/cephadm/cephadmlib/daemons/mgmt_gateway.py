import logging
import os
from typing import Dict, List, Tuple, Optional
import re

from ..call_wrappers import call, CallVerbosity
from ..container_daemon_form import ContainerDaemonForm, daemon_to_container
from ..container_types import CephContainer, extract_uid_gid
from ..context import CephadmContext
from ..context_getters import fetch_configs
from ..daemon_form import register as register_daemon_form
from ..daemon_identity import DaemonIdentity
from ..deployment_utils import to_deployment_container
from ceph.cephadm.images import DefaultImages
from ..data_utils import dict_get, is_fsid
from ..file_utils import populate_files, makedirs, recursive_chown
from ..exceptions import Error

logger = logging.getLogger()


@register_daemon_form
class MgmtGateway(ContainerDaemonForm):
    """Defines an MgmtGateway container"""

    daemon_type = 'mgmt-gateway'
    required_files = [
        'nginx.conf',
        'nginx_external_server.conf',
        'nginx_internal_server.conf',
        'nginx_internal.crt',
        'nginx_internal.key',
    ]

    default_image = DefaultImages.NGINX.image_ref

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        return cls.daemon_type == daemon_type

    def __init__(
        self,
        ctx: CephadmContext,
        fsid: str,
        daemon_id: str,
        config_json: Dict,
        image: str = DefaultImages.NGINX.image_ref,
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
    ) -> 'MgmtGateway':
        return cls(ctx, fsid, daemon_id, fetch_configs(ctx), ctx.image)

    @classmethod
    def create(
        cls, ctx: CephadmContext, ident: DaemonIdentity
    ) -> 'MgmtGateway':
        return cls.init(ctx, ident.fsid, ident.daemon_id)

    @property
    def identity(self) -> DaemonIdentity:
        return DaemonIdentity(self.fsid, self.daemon_type, self.daemon_id)

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

    def container(self, ctx: CephadmContext) -> CephContainer:
        ctr = daemon_to_container(ctx, self)
        return to_deployment_container(ctx, ctr)

    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        return extract_uid_gid(ctx, file_path='/etc/nginx/')

    def get_daemon_args(self) -> List[str]:
        return []

    def default_entrypoint(self) -> str:
        return ''

    def create_daemon_dirs(self, data_dir: str, uid: int, gid: int) -> None:
        """Create files under the container data dir"""
        if not os.path.isdir(data_dir):
            raise OSError('data_dir is not a directory: %s' % (data_dir))
        logger.info('Writing mgmt-gateway config...')
        config_dir = os.path.join(data_dir, 'etc/')
        ssl_dir = os.path.join(data_dir, 'etc/ssl')
        for ddir in [config_dir, ssl_dir]:
            makedirs(ddir, uid, gid, 0o755)
            recursive_chown(ddir, uid, gid)
        conf_files = {
            fname: content
            for fname, content in self.files.items()
            if fname.endswith('.conf')
        }
        cert_files = {
            fname: content
            for fname, content in self.files.items()
            if fname.endswith('.crt') or fname.endswith('.key')
        }
        populate_files(config_dir, conf_files, uid, gid)
        populate_files(ssl_dir, cert_files, uid, gid)

    def _get_container_mounts(self, data_dir: str) -> Dict[str, str]:
        mounts: Dict[str, str] = {}
        mounts[
            os.path.join(data_dir, 'nginx.conf')
        ] = '/etc/nginx/nginx.conf:Z'
        return mounts

    @staticmethod
    def get_version(ctx: CephadmContext, container_id: str) -> Optional[str]:
        """Return the version of the Nginx container"""
        version = None
        out, err, code = call(
            ctx,
            [
                ctx.container_engine.path,
                'exec',
                container_id,
                'nginx',
                '-v',
            ],
            verbosity=CallVerbosity.QUIET,
        )
        if code == 0:
            # nginx is using stderr to print the version!!
            match = re.search(r'nginx version:\s*nginx\/(.+)', err)
            if match:
                version = match.group(1)
        return version

    def customize_container_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        uid, _ = self.uid_gid(ctx)
        extra_args = [
            '--user',
            str(uid),
        ]
        args.extend(extra_args)

    def customize_process_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        # The following noqa comment is intentional to suppress warnings about using double quotes
        # instead of single quotes. We use double quotes here to ensure that single quotes are
        # used in the final parsed output: nginx -g 'daemon off;'
        args.extend(['nginx', '-g', "daemon off;"])  # noqa

    def customize_container_mounts(
        self, ctx: CephadmContext, mounts: Dict[str, str]
    ) -> None:
        data_dir = self.identity.data_dir(ctx.data_dir)
        mounts.update(
            {
                os.path.join(
                    data_dir, 'etc/nginx.conf'
                ): '/etc/nginx/nginx.conf:Z',
                os.path.join(
                    data_dir, 'etc/nginx_internal_server.conf'
                ): '/etc/nginx_internal_server.conf:Z',
                os.path.join(
                    data_dir, 'etc/nginx_external_server.conf'
                ): '/etc/nginx_external_server.conf:Z',
                os.path.join(data_dir, 'etc/ssl'): '/etc/nginx/ssl/',
            }
        )
