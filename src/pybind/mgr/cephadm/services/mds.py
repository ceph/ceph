from typing import List
from ceph.deployment.service_spec import ServiceSpec
from orchestrator import DaemonDescription
from .service_registry import register_cephadm_service
from .cephadmservice import CephService, CephadmDaemonDeploySpec, DaemonDeployContext


@register_cephadm_service
class MdsService(CephService):
    TYPE = 'mds'

    def allow_colo(self) -> bool:
        return True

    def config(self, spec: ServiceSpec) -> None:
        assert self.TYPE == spec.service_type
        assert spec.service_id

        # ensure mds_join_fs is set for these daemons
        ret, out, err = self.mgr.check_mon_command({
            'prefix': 'config set',
            'who': 'mds.' + spec.service_id,
            'name': 'mds_join_fs',
            'value': spec.service_id,
        })

    def prepare_create(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> CephadmDaemonDeploySpec:
        daemon_spec = deploy_ctx.daemon_spec
        assert self.TYPE == daemon_spec.daemon_type
        mds_id, _ = daemon_spec.daemon_id, daemon_spec.host

        # get mds. key
        keyring = self.get_keyring_with_caps(self.get_auth_entity(mds_id),
                                             ['mon', 'profile mds',
                                              'osd', 'allow rw tag cephfs *=*',
                                              'mds', 'allow'])
        daemon_spec.keyring = keyring

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(deploy_ctx)

        return daemon_spec

    def get_active_daemon(self, daemon_descrs: List[DaemonDescription]) -> DaemonDescription:
        active_mds_strs = list()
        for fs in self.mgr.get('fs_map')['filesystems']:
            mds_map = fs['mdsmap']
            if mds_map is not None:
                for mds_id, mds_status in mds_map['info'].items():
                    if mds_status['state'] == 'up:active':
                        active_mds_strs.append(mds_status['name'])
        if len(active_mds_strs) != 0:
            for daemon in daemon_descrs:
                if daemon.daemon_id in active_mds_strs:
                    return daemon
        # if no mds found, return empty Daemon Desc
        return DaemonDescription()

    def purge(self, service_name: str) -> None:
        self.mgr.check_mon_command({
            'prefix': 'config rm',
            'who': service_name,
            'name': 'mds_join_fs',
        })
