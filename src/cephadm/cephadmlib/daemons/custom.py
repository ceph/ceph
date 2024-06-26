import logging
import os
import re

from typing import Any, Dict, List, Optional, Tuple, Union

from ..container_daemon_form import ContainerDaemonForm, daemon_to_container
from ..container_types import CephContainer, InitContainer
from ..context import CephadmContext
from ..context_getters import fetch_configs
from ..daemon_form import register as register_daemon_form
from ..daemon_identity import DaemonIdentity
from ..data_utils import dict_get, dict_get_join
from ..deploy import DeploymentType
from ..deployment_utils import to_deployment_container
from ..file_utils import write_new, makedirs
from ..net_utils import EndPoint


logger = logging.getLogger()


@register_daemon_form
class CustomContainer(ContainerDaemonForm):
    """Defines a custom container"""

    daemon_type = 'container'

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        return cls.daemon_type == daemon_type

    def __init__(
        self,
        fsid: str,
        daemon_id: Union[int, str],
        config_json: Dict,
        image: str,
    ) -> None:
        self.fsid = fsid
        self.daemon_id = daemon_id
        self.image = image

        # config-json options
        self.entrypoint = dict_get(config_json, 'entrypoint')
        self.uid = dict_get(config_json, 'uid', 65534)  # nobody
        self.gid = dict_get(config_json, 'gid', 65534)  # nobody
        self.volume_mounts = dict_get(config_json, 'volume_mounts', {})
        self.args = dict_get(config_json, 'args', [])
        self.envs = dict_get(config_json, 'envs', [])
        self.privileged = dict_get(config_json, 'privileged', False)
        self.bind_mounts = dict_get(config_json, 'bind_mounts', [])
        self.ports = dict_get(config_json, 'ports', [])
        self.dirs = dict_get(config_json, 'dirs', [])
        self.files = dict_get(config_json, 'files', {})

    @classmethod
    def init(
        cls, ctx: CephadmContext, fsid: str, daemon_id: Union[int, str]
    ) -> 'CustomContainer':
        return cls(fsid, daemon_id, fetch_configs(ctx), ctx.image)

    @classmethod
    def create(
        cls, ctx: CephadmContext, ident: DaemonIdentity
    ) -> 'CustomContainer':
        return cls.init(ctx, ident.fsid, ident.daemon_id)

    @property
    def identity(self) -> DaemonIdentity:
        return DaemonIdentity(self.fsid, self.daemon_type, self.daemon_id)

    def create_daemon_dirs(self, data_dir: str, uid: int, gid: int) -> None:
        """
        Create dirs/files below the container data directory.
        """
        logger.info(
            'Creating custom container configuration '
            'dirs/files in {} ...'.format(data_dir)
        )

        if not os.path.isdir(data_dir):
            raise OSError('data_dir is not a directory: %s' % data_dir)

        for dir_path in self.dirs:
            logger.info('Creating directory: {}'.format(dir_path))
            dir_path = os.path.join(data_dir, dir_path.strip('/'))
            makedirs(dir_path, uid, gid, 0o755)

        for file_path in self.files:
            logger.info('Creating file: {}'.format(file_path))
            content = dict_get_join(self.files, file_path)
            file_path = os.path.join(data_dir, file_path.strip('/'))
            with write_new(
                file_path, owner=(uid, gid), encoding='utf-8'
            ) as f:
                f.write(content)

    def get_daemon_args(self) -> List[str]:
        return []

    def get_container_args(self) -> List[str]:
        return self.args

    def get_container_envs(self) -> List[str]:
        return self.envs

    def _get_container_mounts(self, data_dir: str) -> Dict[str, str]:
        """
        Get the volume mounts. Relative source paths will be located below
        `/var/lib/ceph/<cluster-fsid>/<daemon-name>`.

        Example:
        {
            /foo/conf: /conf
            foo/conf: /conf
        }
        becomes
        {
            /foo/conf: /conf
            /var/lib/ceph/<cluster-fsid>/<daemon-name>/foo/conf: /conf
        }
        """
        mounts = {}
        for source, destination in self.volume_mounts.items():
            source = os.path.join(data_dir, source)
            mounts[source] = destination
        return mounts

    def customize_container_mounts(
        self, ctx: CephadmContext, mounts: Dict[str, str]
    ) -> None:
        data_dir = self.identity.data_dir(ctx.data_dir)
        mounts.update(self._get_container_mounts(data_dir))

    def _get_container_binds(self, data_dir: str) -> List[List[str]]:
        """
        Get the bind mounts. Relative `source=...` paths will be located below
        `/var/lib/ceph/<cluster-fsid>/<daemon-name>`.

        Example:
        [
            'type=bind',
            'source=lib/modules',
            'destination=/lib/modules',
            'ro=true'
        ]
        becomes
        [
            ...
            'source=/var/lib/ceph/<cluster-fsid>/<daemon-name>/lib/modules',
            ...
        ]
        """
        binds = self.bind_mounts.copy()
        for bind in binds:
            for index, value in enumerate(bind):
                match = re.match(r'^source=(.+)$', value)
                if match:
                    bind[index] = 'source={}'.format(
                        os.path.join(data_dir, match.group(1))
                    )
        return binds

    def customize_container_binds(
        self, ctx: CephadmContext, binds: List[List[str]]
    ) -> None:
        data_dir = self.identity.data_dir(ctx.data_dir)
        binds.extend(self._get_container_binds(data_dir))

    # Cache the container so we don't need to rebuild it again when calling
    # into init_containers
    _container: Optional[CephContainer] = None

    def container(self, ctx: CephadmContext) -> CephContainer:
        if self._container is None:
            ctr = daemon_to_container(
                ctx,
                self,
                host_network=False,
                privileged=self.privileged,
                ptrace=ctx.allow_ptrace,
            )
            self._container = to_deployment_container(ctx, ctr)
        return self._container

    def init_containers(self, ctx: CephadmContext) -> List[InitContainer]:
        primary = self.container(ctx)
        init_containers: List[Dict[str, Any]] = getattr(
            ctx, 'init_containers', []
        )
        return [
            InitContainer.from_primary_and_opts(ctx, primary, ic_opts)
            for ic_opts in init_containers
        ]

    def customize_container_endpoints(
        self, endpoints: List[EndPoint], deployment_type: DeploymentType
    ) -> None:
        if deployment_type == DeploymentType.DEFAULT:
            endpoints.extend([EndPoint('0.0.0.0', p) for p in self.ports])

    def customize_container_envs(
        self, ctx: CephadmContext, envs: List[str]
    ) -> None:
        envs.extend(self.get_container_envs())

    def customize_container_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.extend(self.get_container_args())

    def customize_process_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.extend(self.get_daemon_args())

    def default_entrypoint(self) -> str:
        return self.entrypoint or ''

    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        return self.uid, self.gid
