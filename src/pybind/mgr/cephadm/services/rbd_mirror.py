import errno
from typing import List, Optional
from mgr_module import HandleCommandResult
from .service_registry import register_cephadm_service
from .cephadmservice import CephService, CephadmDaemonDeploySpec, DaemonDeployContext


@register_cephadm_service
class RbdMirrorService(CephService):
    TYPE = 'rbd-mirror'

    def allow_colo(self) -> bool:
        return True

    def prepare_create(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> CephadmDaemonDeploySpec:
        daemon_spec = deploy_ctx.daemon_spec
        assert self.TYPE == daemon_spec.daemon_type
        daemon_id, _ = daemon_spec.daemon_id, daemon_spec.host

        keyring = self.get_keyring_with_caps(self.get_auth_entity(daemon_id),
                                             ['mon', 'profile rbd-mirror',
                                              'osd', 'profile rbd'])

        daemon_spec.keyring = keyring

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(deploy_ctx)

        return daemon_spec

    def ok_to_stop(
            self,
            daemon_ids: List[str],
            force: bool = False,
            known: Optional[List[str]] = None  # output argument
    ) -> HandleCommandResult:
        # if only 1 rbd-mirror, alert user (this is not passable with --force)
        warn, warn_message = self._enough_daemons_to_stop(
            self.TYPE, daemon_ids, 'Rbdmirror', 1, True)
        if warn:
            return HandleCommandResult(-errno.EBUSY, '', warn_message)
        return HandleCommandResult(0, warn_message, '')
