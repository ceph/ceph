import logging
import os

from typing import Dict, List, Optional, Tuple

from ..constants import DEFAULT_IMAGE
from ..container_daemon_form import ContainerDaemonForm, daemon_to_container
from ..container_types import CephContainer, extract_uid_gid
from ..context import CephadmContext
from ..context_getters import fetch_configs, get_config_and_keyring
from ..daemon_form import register as register_daemon_form
from ..daemon_identity import DaemonIdentity
from ..data_utils import dict_get, is_fsid
from ..deployment_utils import to_deployment_container
from ..exceptions import Error
from ..file_utils import populate_files

logger = logging.getLogger()


@register_daemon_form
class NodeProxy(ContainerDaemonForm):
    """Defines a node-proxy container"""

    daemon_type = 'node-proxy'
    # TODO: update this if we make node-proxy an executable
    entrypoint = '/usr/sbin/ceph-node-proxy'
    required_files = ['node-proxy.json']

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        return cls.daemon_type == daemon_type

    def __init__(
        self,
        ctx: CephadmContext,
        ident: DaemonIdentity,
        config_json: Dict,
        image: str = DEFAULT_IMAGE,
    ):
        self.ctx = ctx
        self._identity = ident
        self.image = image

        # config-json options
        config = dict_get(config_json, 'node-proxy.json', {})
        self.files = {'node-proxy.json': config}

        # validate the supplied args
        self.validate()

    @classmethod
    def init(
        cls, ctx: CephadmContext, fsid: str, daemon_id: str
    ) -> 'NodeProxy':
        return cls.create(
            ctx, DaemonIdentity(fsid, cls.daemon_type, daemon_id)
        )

    @classmethod
    def create(
        cls, ctx: CephadmContext, ident: DaemonIdentity
    ) -> 'NodeProxy':
        return cls(ctx, ident, fetch_configs(ctx), ctx.image)

    @property
    def identity(self) -> DaemonIdentity:
        return self._identity

    @property
    def fsid(self) -> str:
        return self._identity.fsid

    @property
    def daemon_id(self) -> str:
        return self._identity.daemon_id

    def customize_container_mounts(
        self, ctx: CephadmContext, mounts: Dict[str, str]
    ) -> None:
        data_dir = self.identity.data_dir(ctx.data_dir)
        # TODO: update this when we have the actual location
        # in the ceph container we are going to keep node-proxy
        mounts.update(
            {
                os.path.join(
                    data_dir, 'node-proxy.json'
                ): '/usr/share/ceph/node-proxy.json:z'
            }
        )

    def customize_process_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        # TODO: this corresponds with the mount location of
        # the config in _get_container_mounts above. They
        # will both need to be updated when we have a proper
        # location in the container for node-proxy
        args.extend(['--config', '/usr/share/ceph/node-proxy.json'])

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
        cname = 'ceph-%s-%s' % (self.fsid, self.get_daemon_name())
        if desc:
            cname = '%s-%s' % (cname, desc)
        return cname

    def create_daemon_dirs(self, data_dir, uid, gid):
        # type: (str, int, int) -> None
        """Create files under the container data dir"""
        if not os.path.isdir(data_dir):
            raise OSError('data_dir is not a directory: %s' % (data_dir))

        logger.info('Writing node-proxy config...')
        # populate files from the config-json
        populate_files(data_dir, self.files, uid, gid)

    def container(self, ctx: CephadmContext) -> CephContainer:
        # So the container can modprobe iscsi_target_mod and have write perms
        # to configfs we need to make this a privileged container.
        ctr = daemon_to_container(ctx, self, privileged=True)
        return to_deployment_container(ctx, ctr)

    def config_and_keyring(
        self, ctx: CephadmContext
    ) -> Tuple[Optional[str], Optional[str]]:
        return get_config_and_keyring(ctx)

    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        return extract_uid_gid(ctx)

    def default_entrypoint(self) -> str:
        return self.entrypoint
