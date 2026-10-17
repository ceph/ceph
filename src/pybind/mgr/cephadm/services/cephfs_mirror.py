from ceph.deployment.service_spec import ServiceSpec
from .service_registry import register_cephadm_service
from .cephadmservice import CephService, CephadmDaemonDeploySpec, DaemonDeployContext


@register_cephadm_service
class CephfsMirrorService(CephService):
    TYPE = 'cephfs-mirror'

    def config(self, spec: ServiceSpec) -> None:
        # make sure mirroring module is enabled
        mgr_map = self.mgr.get('mgr_map')
        mod_name = 'mirroring'
        if mod_name not in mgr_map.get('services', {}):
            self.mgr.check_mon_command({
                'prefix': 'mgr module enable',
                'module': mod_name
            })
            # we shouldn't get here (mon will tell the mgr to respawn), but no
            # harm done if we do.

    def prepare_create(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> CephadmDaemonDeploySpec:
        daemon_spec = deploy_ctx.daemon_spec
        assert self.TYPE == daemon_spec.daemon_type

        ret, keyring, err = self.mgr.check_mon_command({
            'prefix': 'auth get-or-create',
            'entity': daemon_spec.entity_name(),
            'caps': ['mon', 'profile cephfs-mirror',
                     'mds', 'allow r',
                     'osd', 'allow rw tag cephfs metadata=*, allow r tag cephfs data=*',
                     'mgr', 'allow r'],
        })

        daemon_spec.keyring = keyring
        daemon_spec.final_config, daemon_spec.deps = self.generate_config(deploy_ctx)
        return daemon_spec
