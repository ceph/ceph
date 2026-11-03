from .service_registry import register_cephadm_service
from .cephadmservice import CephService, CephadmDaemonDeploySpec, DaemonDeployContext


@register_cephadm_service
class CrashService(CephService):
    TYPE = 'crash'

    def prepare_create(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> CephadmDaemonDeploySpec:
        daemon_spec = deploy_ctx.daemon_spec
        assert self.TYPE == daemon_spec.daemon_type
        daemon_id, host = daemon_spec.daemon_id, daemon_spec.host

        keyring = self.get_keyring_with_caps(self.get_auth_entity(daemon_id, host=host),
                                             ['mon', 'profile crash',
                                              'mgr', 'profile crash'])

        daemon_spec.keyring = keyring

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(deploy_ctx)

        return daemon_spec
