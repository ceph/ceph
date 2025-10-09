import logging

from typing import Any, Dict, List, Tuple

from ceph.cephadm.images import DefaultImages
from ..container_daemon_form import ContainerDaemonForm, daemon_to_container
from ..container_types import CephContainer
from ..context import CephadmContext
from ..context_getters import fetch_configs
from ..daemon_form import register as register_daemon_form
from ..daemon_identity import DaemonIdentity
from ..deployment_utils import to_deployment_container
from ..constants import UID_NOBODY, GID_NOGROUP


logger = logging.getLogger()


@register_daemon_form
class Tracing(ContainerDaemonForm):
    """Define the configs for the jaeger tracing containers"""

    components: Dict[str, Dict[str, Any]] = {
        'elasticsearch': {
            'image': DefaultImages.ELASTICSEARCH.image_ref,
            'envs': ['discovery.type=single-node'],
        },
        'jaeger-agent': {
            'image': DefaultImages.JAEGER_AGENT.image_ref,
        },
        'jaeger-collector': {
            'image': DefaultImages.JAEGER_COLLECTOR.image_ref,
        },
        'jaeger-query': {
            'image': DefaultImages.JAEGER_QUERY.image_ref,
        },
    }  # type: ignore

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        return daemon_type in cls.components

    @staticmethod
    def set_configuration(config: Dict[str, str], daemon_type: str) -> None:
        if daemon_type in ['jaeger-collector', 'jaeger-query']:
            assert 'elasticsearch_nodes' in config
            Tracing.components[daemon_type]['envs'] = [
                'SPAN_STORAGE_TYPE=elasticsearch',
                f'ES_SERVER_URLS={config["elasticsearch_nodes"]}',
            ]
        if daemon_type == 'jaeger-agent':
            assert 'collector_nodes' in config
            Tracing.components[daemon_type]['daemon_args'] = [
                f'--reporter.grpc.host-port={config["collector_nodes"]}',
                '--processor.jaeger-compact.server-host-port=6799',
            ]

    def __init__(self, ident: DaemonIdentity) -> None:
        self._identity = ident
        self._configured = False

    def _configure(self, ctx: CephadmContext) -> None:
        if self._configured:
            return
        config = fetch_configs(ctx)
        # Currently, this method side-effects the class attribute, and that
        # is unpleasant. In the future it would be nice to move all of
        # set_configuration into _confiure and only modify each classes data
        # independently
        self.set_configuration(config, self.identity.daemon_type)
        self._configured = True

    @classmethod
    def create(cls, ctx: CephadmContext, ident: DaemonIdentity) -> 'Tracing':
        return cls(ident)

    @property
    def identity(self) -> DaemonIdentity:
        return self._identity

    def container(self, ctx: CephadmContext) -> CephContainer:
        ctr = daemon_to_container(ctx, self)
        return to_deployment_container(ctx, ctr)

    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        return UID_NOBODY, GID_NOGROUP

    def get_daemon_args(self) -> List[str]:
        return self.components[self.identity.daemon_type].get(
            'daemon_args', []
        )

    def customize_process_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        self._configure(ctx)
        # earlier code did an explicit check if the daemon type was jaeger-agent
        # and would only call get_daemon_args if that was true. However, since
        # the function only returns a non-empty list in the case of jaeger-agent
        # that check is unnecessary and is not brought over.
        args.extend(self.get_daemon_args())

    def customize_container_envs(
        self, ctx: CephadmContext, envs: List[str]
    ) -> None:
        self._configure(ctx)
        envs.extend(
            self.components[self.identity.daemon_type].get('envs', [])
        )

    def default_entrypoint(self) -> str:
        return ''
