import logging
import os  # TODO: remove in actual PR (test-only payload shim)

from typing import Dict, List, Optional, Tuple, Union

from ..container_daemon_form import ContainerDaemonForm, daemon_to_container
from ..container_types import CephContainer, extract_uid_gid
from ..context import CephadmContext
from ..context_getters import get_config_and_keyring
from ..daemon_form import register as register_daemon_form
from ..daemon_identity import DaemonIdentity
from ..deployment_utils import to_deployment_container
from ..file_utils import make_run_dir


logger = logging.getLogger()

# TODO: remove in actual PR (test-only payload shim)
# /usr/bin/fcm-dedup is not in the image yet. Until it is, a host-side
# payload dir supplies it (bind-mounted over /usr/bin/fcm-dedup).
PAYLOAD_DIR = '/etc/ceph/fcm_ad_payload'
PAYLOAD_ENTRYPOINT = PAYLOAD_DIR + '/fcm_dedup_entrypoint.sh'


@register_daemon_form
class FcmDedup(ContainerDaemonForm):
    """Defines an fcm-dedup container (one per OSD, daemon id is the OSD id)"""

    daemon_type = 'fcm-dedup'
    entrypoint = '/usr/bin/fcm-dedup'

    @classmethod
    def for_daemon_type(cls, daemon_type: str) -> bool:
        return cls.daemon_type == daemon_type

    def __init__(
        self,
        ctx: CephadmContext,
        fsid: str,
        daemon_id: Union[int, str],
        image: str,
    ) -> None:
        self.ctx = ctx
        self.fsid = fsid
        self.daemon_id = daemon_id
        self.image = image

    @classmethod
    def init(
        cls, ctx: CephadmContext, fsid: str, daemon_id: Union[int, str]
    ) -> 'FcmDedup':
        return cls(ctx, fsid, daemon_id, ctx.image)

    @classmethod
    def create(cls, ctx: CephadmContext, ident: DaemonIdentity) -> 'FcmDedup':
        return cls.init(ctx, ident.fsid, ident.daemon_id)

    @property
    def identity(self) -> DaemonIdentity:
        return DaemonIdentity(self.fsid, self.daemon_type, self.daemon_id)

    def container(self, ctx: CephadmContext) -> CephContainer:
        uid, gid = self.uid_gid(ctx)
        make_run_dir(ctx.fsid, uid, gid)
        ctr = daemon_to_container(ctx, self, privileged=True)
        return to_deployment_container(ctx, ctr)

    def uid_gid(self, ctx: CephadmContext) -> Tuple[int, int]:
        return extract_uid_gid(ctx)

    def config_and_keyring(
        self, ctx: CephadmContext
    ) -> Tuple[Optional[str], Optional[str]]:
        return get_config_and_keyring(ctx)

    def customize_container_mounts(
        self, ctx: CephadmContext, mounts: Dict[str, str]
    ) -> None:
        data_dir = self.identity.data_dir(ctx.data_dir)
        mounts[data_dir + '/config'] = '/etc/ceph/ceph.conf:z'
        mounts[
            data_dir + '/keyring'
        ] = '/etc/ceph/ceph.client.%s.%s.keyring:z' % (
            self.daemon_type,
            self.daemon_id,
        )
        mounts['/dev'] = '/dev'

    # TODO: remove in actual PR (test-only payload shim)
    def customize_container_binds(
        self, ctx: CephadmContext, binds: List[List[str]]
    ) -> None:
        if os.path.exists(PAYLOAD_ENTRYPOINT):
            binds.append(
                [
                    'type=bind',
                    f'source={PAYLOAD_DIR}',
                    f'destination={PAYLOAD_DIR}',
                ]
            )
            binds.append(
                [
                    'type=bind',
                    f'source={PAYLOAD_ENTRYPOINT}',
                    f'destination={self.entrypoint}',
                ]
            )

    def customize_process_args(
        self, ctx: CephadmContext, args: List[str]
    ) -> None:
        args.extend(['--id', str(self.daemon_id), '--fsid', self.fsid])

    def default_entrypoint(self) -> str:
        return self.entrypoint
