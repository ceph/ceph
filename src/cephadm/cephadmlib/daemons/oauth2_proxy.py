import logging
import os
from typing import Dict, List, Tuple, Optional

from ..container_daemon_form import ContainerDaemonForm, daemon_to_container
from ..container_types import CephContainer, extract_uid_gid
from ..context import CephadmContext
from ..context_getters import fetch_configs
from ..daemon_form import register as register_daemon_form
from ..daemon_identity import DaemonIdentity
from ..deployment_utils import to_deployment_container
from ..constants import DEFAULT_OAUTH2_PROXY_IMAGE
from ..data_utils import dict_get
from ..file_utils import populate_files, makedirs, recursive_chown


logger = logging.getLogger()


@register_daemon_form
class OAuth2Proxy(ContainerDaemonForm):
    """Define the configs for the jaeger tracing containers"""

    daemon_type = 'oauth2-proxy'
    required_files = ['oauth2-proxy.conf']

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        return cls.daemon_type == daemon_type

    def __init__(
        self,
        ctx: CephadmContext,
        daemon_id: str,
        config_json: Dict,
        image: str = DEFAULT_OAUTH2_PROXY_IMAGE,
    ):
        self.ctx = ctx
        self.fsid = ctx.fsid
        self.daemon_id = daemon_id
        self.image = image
        self.files = dict_get(config_json, 'files', {})

    @classmethod
    def init(
        cls, ctx: CephadmContext, fsid: str, daemon_id: str
    ) -> 'OAuth2Proxy':
        return cls(ctx, daemon_id, fetch_configs(ctx), ctx.image)

    @classmethod
    def create(
        cls, ctx: CephadmContext, ident: DaemonIdentity
    ) -> 'OAuth2Proxy':
        return cls.init(ctx, ctx.fsid, ident.daemon_id)

    @property
    def identity(self) -> DaemonIdentity:
        return DaemonIdentity(self.fsid, self.daemon_type, self.daemon_id)

    def container(self, ctx: CephadmContext) -> CephContainer:
        ctr = daemon_to_container(ctx, self)
        return to_deployment_container(ctx, ctr)

    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        return extract_uid_gid(ctx, file_path='/bin/oauth2-proxy')

    def get_daemon_args(self) -> List[str]:
        return ['--config=/etc/oauth2-proxy.conf']

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

    @staticmethod
    def get_version(
        ctx: CephadmContext, fsid: str, daemon_id: str
    ) -> Optional[str]:
        """Return the version of the notifier from it's http endpoint"""
        # Redo(TODO): fix version
        return 'TODO'

    def customize_container_mounts(
        self, ctx: CephadmContext, mounts: Dict[str, str]
    ) -> None:
        data_dir = self.identity.data_dir(ctx.data_dir)
        mounts.update(
            {
                os.path.join(
                    data_dir, 'etc/oauth2-proxy.conf'
                ): '/etc/oauth2-proxy.conf:Z',
            }
        )

    def customize_container_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        uid, _ = self.uid_gid(ctx)
        other_args = [
            '--user',
            str(uid),
            # FIXME: disable cpu/memory limits for the time being (not supported
            # by ubuntu 18.04 kernel!)
        ]
        args.extend(other_args)

    def customize_process_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.extend(self.get_daemon_args())
