import os

from typing import Dict, List, Optional, Tuple, Union

from ..constants import (
    DEFAULT_HAPROXY_IMAGE,
    DEFAULT_KEEPALIVED_IMAGE,
    DATA_DIR_MODE,
)
from ..container_daemon_form import ContainerDaemonForm, daemon_to_container
from ..container_types import CephContainer, extract_uid_gid
from ..context import CephadmContext
from ..context_getters import fetch_configs
from ..daemon_form import register as register_daemon_form
from ..daemon_identity import DaemonIdentity
from ..data_utils import dict_get, is_fsid
from ..deployment_utils import to_deployment_container
from ..exceptions import Error
from ..file_utils import makedirs, populate_files


@register_daemon_form
class HAproxy(ContainerDaemonForm):
    """Defines an HAproxy container"""

    daemon_type = 'haproxy'
    required_files = ['haproxy.cfg']
    default_image = DEFAULT_HAPROXY_IMAGE

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        return cls.daemon_type == daemon_type

    def __init__(
        self,
        ctx: CephadmContext,
        fsid: str,
        daemon_id: Union[int, str],
        config_json: Dict,
        image: str,
    ) -> None:
        self.ctx = ctx
        self.fsid = fsid
        self.daemon_id = daemon_id
        self.image = image

        # config-json options
        self.files = dict_get(config_json, 'files', {})

        self.validate()

    @classmethod
    def init(
        cls, ctx: CephadmContext, fsid: str, daemon_id: Union[int, str]
    ) -> 'HAproxy':
        return cls(ctx, fsid, daemon_id, fetch_configs(ctx), ctx.image)

    @classmethod
    def create(cls, ctx: CephadmContext, ident: DaemonIdentity) -> 'HAproxy':
        return cls.init(ctx, ident.fsid, ident.daemon_id)

    @property
    def identity(self) -> DaemonIdentity:
        return DaemonIdentity(self.fsid, self.daemon_type, self.daemon_id)

    def create_daemon_dirs(self, data_dir: str, uid: int, gid: int) -> None:
        """Create files under the container data dir"""
        if not os.path.isdir(data_dir):
            raise OSError('data_dir is not a directory: %s' % (data_dir))

        # create additional directories in data dir for HAproxy to use
        if not os.path.isdir(os.path.join(data_dir, 'haproxy')):
            makedirs(
                os.path.join(data_dir, 'haproxy'), uid, gid, DATA_DIR_MODE
            )

        data_dir = os.path.join(data_dir, 'haproxy')
        populate_files(data_dir, self.files, uid, gid)

    def get_daemon_args(self) -> List[str]:
        return ['haproxy', '-f', '/var/lib/haproxy/haproxy.cfg']

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

    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        # better directory for this?
        print('UUUUU', extract_uid_gid)
        return extract_uid_gid(self.ctx, file_path='/var/lib')

    @staticmethod
    def _get_container_mounts(data_dir: str) -> Dict[str, str]:
        mounts = dict()
        mounts[os.path.join(data_dir, 'haproxy')] = '/var/lib/haproxy'
        return mounts

    def customize_container_mounts(
        self, ctx: CephadmContext, mounts: Dict[str, str]
    ) -> None:
        data_dir = self.identity.data_dir(ctx.data_dir)
        mounts.update(self._get_container_mounts(data_dir))

    @staticmethod
    def get_sysctl_settings() -> List[str]:
        return [
            '# IP forwarding and non-local bind',
            'net.ipv4.ip_forward = 1',
            'net.ipv4.ip_nonlocal_bind = 1',
        ]

    def container(self, ctx: CephadmContext) -> CephContainer:
        ctr = daemon_to_container(ctx, self)
        return to_deployment_container(ctx, ctr)

    def customize_container_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.extend(
            ['--user=root']
        )  # haproxy 2.4 defaults to a different user

    def customize_process_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.extend(self.get_daemon_args())


@register_daemon_form
class Keepalived(ContainerDaemonForm):
    """Defines an Keepalived container"""

    daemon_type = 'keepalived'
    required_files = ['keepalived.conf']
    default_image = DEFAULT_KEEPALIVED_IMAGE

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        return cls.daemon_type == daemon_type

    def __init__(
        self,
        ctx: CephadmContext,
        fsid: str,
        daemon_id: Union[int, str],
        config_json: Dict,
        image: str,
    ) -> None:
        self.ctx = ctx
        self.fsid = fsid
        self.daemon_id = daemon_id
        self.image = image

        # config-json options
        self.files = dict_get(config_json, 'files', {})

        self.validate()

    @classmethod
    def init(
        cls, ctx: CephadmContext, fsid: str, daemon_id: Union[int, str]
    ) -> 'Keepalived':
        return cls(ctx, fsid, daemon_id, fetch_configs(ctx), ctx.image)

    @classmethod
    def create(
        cls, ctx: CephadmContext, ident: DaemonIdentity
    ) -> 'Keepalived':
        return cls.init(ctx, ident.fsid, ident.daemon_id)

    @property
    def identity(self) -> DaemonIdentity:
        return DaemonIdentity(self.fsid, self.daemon_type, self.daemon_id)

    def create_daemon_dirs(self, data_dir: str, uid: int, gid: int) -> None:
        """Create files under the container data dir"""
        if not os.path.isdir(data_dir):
            raise OSError('data_dir is not a directory: %s' % (data_dir))

        # create additional directories in data dir for keepalived to use
        if not os.path.isdir(os.path.join(data_dir, 'keepalived')):
            makedirs(
                os.path.join(data_dir, 'keepalived'), uid, gid, DATA_DIR_MODE
            )

        # populate files from the config-json
        populate_files(data_dir, self.files, uid, gid)

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

    @staticmethod
    def get_container_envs():
        # type: () -> List[str]
        envs = [
            'KEEPALIVED_AUTOCONF=false',
            'KEEPALIVED_CONF=/etc/keepalived/keepalived.conf',
            'KEEPALIVED_CMD=/usr/sbin/keepalived -n -l -f /etc/keepalived/keepalived.conf',
            'KEEPALIVED_DEBUG=false',
        ]
        return envs

    @staticmethod
    def get_sysctl_settings() -> List[str]:
        return [
            '# IP forwarding and non-local bind',
            'net.ipv4.ip_forward = 1',
            'net.ipv4.ip_nonlocal_bind = 1',
        ]

    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        # better directory for this?
        return extract_uid_gid(self.ctx, file_path='/var/lib')

    @staticmethod
    def _get_container_mounts(data_dir: str) -> Dict[str, str]:
        mounts = dict()
        mounts[
            os.path.join(data_dir, 'keepalived.conf')
        ] = '/etc/keepalived/keepalived.conf'
        return mounts

    def customize_container_mounts(
        self, ctx: CephadmContext, mounts: Dict[str, str]
    ) -> None:
        data_dir = self.identity.data_dir(ctx.data_dir)
        mounts.update(self._get_container_mounts(data_dir))

    def container(self, ctx: CephadmContext) -> CephContainer:
        ctr = daemon_to_container(ctx, self)
        return to_deployment_container(ctx, ctr)

    def customize_container_envs(
        self, ctx: CephadmContext, envs: List[str]
    ) -> None:
        envs.extend(self.get_container_envs())

    def customize_container_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.extend(['--cap-add=NET_ADMIN', '--cap-add=NET_RAW'])
