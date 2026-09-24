import logging
import os

from typing import Any, Dict, List, Tuple

from ceph.cephadm.images import DefaultImages
from ..container_daemon_form import ContainerDaemonForm, daemon_to_container
from ..container_types import CephContainer, extract_uid_gid
from ..context import CephadmContext
from ..context_getters import fetch_configs
from ..daemon_form import register as register_daemon_form
from ..daemon_identity import DaemonIdentity
from ..deployment_utils import to_deployment_container
from ..file_utils import makedirs, populate_files


logger = logging.getLogger()


@register_daemon_form
class Tracing(ContainerDaemonForm):
    """Define the configs for the jaeger tracing containers"""

    components: Dict[str, Dict[str, Any]] = {
        'elasticsearch': {
            'image': DefaultImages.ELASTICSEARCH.image_ref,
            'envs': ['discovery.type=single-node'],
        },
        'jaeger': {
            'image': DefaultImages.JAEGER.image_ref,
        },
    }  # type: ignore

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        return daemon_type in cls.components

    def __init__(self, ctx: CephadmContext, ident: DaemonIdentity) -> None:
        self._ctx = ctx
        self._identity = ident

    @classmethod
    def create(cls, ctx: CephadmContext, ident: DaemonIdentity) -> 'Tracing':
        return cls(ctx, ident)

    @property
    def identity(self) -> DaemonIdentity:
        return self._identity

    def container(self, ctx: CephadmContext) -> CephContainer:
        ctr = daemon_to_container(ctx, self)
        return to_deployment_container(ctx, ctr)

    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        # jaeger image (quay.io/jaegertracing/jaeger) sets USER 10001 in its
        # Dockerfile but owns all filesystem paths as root, so there is no path
        # to probe with extract_uid_gid — use the known value directly.
        return [10001,10001]

    def get_daemon_args(self) -> List[str]:
        if self.identity.daemon_type == 'jaeger':
            return ['--config', '/etc/jaeger/config.yaml']
        return self.components[self.identity.daemon_type].get(
            'daemon_args', []
        )

    def customize_container_mounts(
        self, ctx: CephadmContext, mounts: Dict[str, str]
    ) -> None:
        if self.identity.daemon_type == 'jaeger':
            data_dir = self.identity.data_dir(ctx.data_dir)
            mounts[
                os.path.join(data_dir, 'etc/jaeger/config.yaml')
            ] = '/etc/jaeger/config.yaml:Z'

    def customize_process_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.extend(self.get_daemon_args())

    def customize_container_envs(
        self, ctx: CephadmContext, envs: List[str]
    ) -> None:
        envs.extend(
            self.components[self.identity.daemon_type].get('envs', [])
        )

    def default_entrypoint(self) -> str:
        return ''

    def create_daemon_dirs(self, data_dir: str, uid: int, gid: int) -> None:
        """Write config.yaml for the jaeger daemon into its data directory."""
        config: Dict[str, Any] = fetch_configs(self._ctx)
        files: Dict[str, Any] = config.get('files', {})
        if not files:
            return
        config_dir = os.path.join(data_dir, 'etc/jaeger')
        makedirs(config_dir, uid, gid, 0o755)
        populate_files(config_dir, files, uid, gid)
