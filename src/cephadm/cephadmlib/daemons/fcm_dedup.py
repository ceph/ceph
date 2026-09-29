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
from ..deployment_utils import to_deployment_container

logger = logging.getLogger()


@register_daemon_form
class FcmDedup(ContainerDaemonForm):
    """One fcm-dedup container per OSD on FCM-capable hosts.

    Uses the same Ceph base image as the OSD — a different entrypoint
    argument, not a different image.  /dev is mounted so the process can
    issue NVMe IO passthrough commands via libnvme.
    """

    daemon_type = 'fcm-dedup'
    entrypoint = '/usr/bin/fcm-dedup'

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        return cls.daemon_type == daemon_type

    def __init__(
        self,
        ctx: CephadmContext,
        ident: DaemonIdentity,
        config_json: Dict,
        image: str = DEFAULT_IMAGE,
    ) -> None:
        self.ctx = ctx
        self._identity = ident
        self.image = image

    @classmethod
    def init(
        cls, ctx: CephadmContext, fsid: str, daemon_id: str
    ) -> 'FcmDedup':
        return cls.create(
            ctx, DaemonIdentity(fsid, cls.daemon_type, daemon_id)
        )

    @classmethod
    def create(cls, ctx: CephadmContext, ident: DaemonIdentity) -> 'FcmDedup':
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

    def default_entrypoint(self) -> str:
        return self.entrypoint

    def customize_process_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.extend(['--id', str(self.daemon_id), '--fsid', self.fsid])

    def customize_container_mounts(
        self, ctx: CephadmContext, mounts: Dict[str, str]
    ) -> None:
        # Required for NVMe IO passthrough via libnvme ioctl.
        mounts['/dev'] = '/dev:rw'
        data_dir = self.identity.data_dir(ctx.data_dir)
        mounts[os.path.join(data_dir, 'config')] = '/etc/ceph/ceph.conf:z'
        mounts[
            os.path.join(data_dir, 'keyring')
        ] = f'/etc/ceph/ceph.client.fcm-dedup.{self.daemon_id}.keyring:z'
        run_path = os.path.join('/var/run/ceph', self.fsid)
        if os.path.exists(run_path):
            mounts[run_path] = '/var/run/ceph:z'

    def customize_container_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        # --privileged is required for the ioctl path used by libnvme.
        args.append('--privileged')
        args.append(ctx.container_engine.unlimited_pids_option)

    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        return extract_uid_gid(ctx)

    def config_and_keyring(
        self, ctx: CephadmContext
    ) -> Tuple[Optional[str], Optional[str]]:
        return get_config_and_keyring(ctx)

    def create_daemon_dirs(self, data_dir: str, uid: int, gid: int) -> None:
        pass

    def container(self, ctx: CephadmContext) -> CephContainer:
        ctr = daemon_to_container(ctx, self, privileged=True)
        return to_deployment_container(ctx, ctr)
