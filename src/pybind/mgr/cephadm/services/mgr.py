from typing import TYPE_CHECKING
import errno
import re
import time
from typing import List, Optional, Dict, Any, Tuple
from mgr_module import HandleCommandResult
from ceph.deployment.service_spec import ServiceSpec
from orchestrator import OrchestratorError, DaemonDescription
from .service_registry import register_cephadm_service
from .cephadmservice import CephService, CephadmDaemonDeploySpec, DaemonDeployContext
import logging

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


@register_cephadm_service
class MgrService(CephService):
    TYPE = 'mgr'

    def allow_colo(self) -> bool:
        if self.mgr.get_ceph_option('mgr_standby_modules'):
            # traditional mgr mode: standby daemons' modules listen on
            # ports and redirect to the primary.  we must not schedule
            # multiple mgrs on the same host or else ports will
            # conflict.
            return False
        else:
            # standby daemons do nothing, and therefore port conflicts
            # are not a concern.
            return True

    @staticmethod
    def _get_mgr_service_ports(mgr: "CephadmOrchestrator") -> List[int]:
        """Return the list of ports from the mgr map services."""
        ports: List[int] = []
        mgr_map = mgr.get('mgr_map')
        for end_point in mgr_map.get('services', {}).values():
            port = re.search(r'\:(\d+)\/', end_point)
            if port:
                ports.append(int(port.group(1)))
        return ports

    @classmethod
    def _get_service_dependencies(
        cls,
        mgr: "CephadmOrchestrator",
        spec: Optional[ServiceSpec] = None,
        daemon_type: Optional[str] = None,
    ) -> List[str]:
        return sorted(
            [f'port:{p}' for p in cls._get_mgr_service_ports(mgr)]
            + [f'sd_port:{mgr.service_discovery_port}']
        )

    def generate_config(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> Tuple[Dict[str, Any], List[str]]:
        return super().generate_config(deploy_ctx)

    def prepare_create(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> CephadmDaemonDeploySpec:
        """
        Create a new manager instance on a host.
        """
        daemon_spec = deploy_ctx.daemon_spec
        assert self.TYPE == daemon_spec.daemon_type
        mgr_id, _ = daemon_spec.daemon_id, daemon_spec.host

        # get mgr. key
        keyring = self.get_keyring_with_caps(self.get_auth_entity(mgr_id),
                                             ['mon', 'profile mgr',
                                              'osd', 'allow *',
                                              'mds', 'allow *'])

        # Retrieve ports used by manager modules
        # In the case of the dashboard port and with several manager daemons
        # running in different hosts, it exists the possibility that the
        # user has decided to use different dashboard ports in each server
        # If this is the case then the dashboard port opened will be only the used
        # as default.
        ports = self._get_mgr_service_ports(self.mgr)
        # Always replace ports (do not append onto a list rehydrated from the
        # persisted host cache). When ``mgr services`` is empty, ``ports`` is
        # empty and we must not retain old entries + append service discovery
        # again
        daemon_spec.ports = ports + [self.mgr.service_discovery_port]
        daemon_spec.keyring = keyring

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(deploy_ctx)

        return daemon_spec

    def get_active_daemon(self, daemon_descrs: List[DaemonDescription]) -> DaemonDescription:
        for daemon in daemon_descrs:
            assert daemon.daemon_type is not None
            assert daemon.daemon_id is not None
            if self.mgr.daemon_is_self(daemon.daemon_type, daemon.daemon_id):
                return daemon
        # if no active mgr found, return empty Daemon Desc
        return DaemonDescription()

    def fail_over(self) -> None:
        # this has been seen to sometimes transiently fail even when there are multiple
        # mgr daemons. As long as there are multiple known mgr daemons, we should retry.
        class NoStandbyError(OrchestratorError):
            pass
        no_standby_exc = NoStandbyError('Need standby mgr daemon', event_kind_subject=(
            'daemon', 'mgr' + self.mgr.get_mgr_id()))
        for sleep_secs in [2, 8, 15]:
            try:
                if not self.mgr_map_has_standby():
                    raise no_standby_exc
                self.mgr.events.for_daemon('mgr' + self.mgr.get_mgr_id(),
                                           'INFO', 'Failing over to other MGR')
                logger.info('Failing over to other MGR')

                # fail over
                ret, out, err = self.mgr.check_mon_command({
                    'prefix': 'mgr fail',
                    'who': self.mgr.get_mgr_id(),
                })
                return
            except NoStandbyError:
                logger.info(
                    f'Failed to find standby mgr for failover. Retrying in {sleep_secs} seconds')
                time.sleep(sleep_secs)
        raise no_standby_exc

    def mgr_map_has_standby(self) -> bool:
        """
        This is a bit safer than asking our inventory. If the mgr joined the mgr map,
        we know it joined the cluster
        """
        mgr_map = self.mgr.get('mgr_map')
        num = len(mgr_map.get('standbys'))
        return bool(num)

    def ok_to_stop(
            self,
            daemon_ids: List[str],
            force: bool = False,
            known: Optional[List[str]] = None  # output argument
    ) -> HandleCommandResult:
        # ok to stop if there is more than 1 mgr and not trying to stop the active mgr

        warn, warn_message = self._enough_daemons_to_stop(self.TYPE, daemon_ids, 'Mgr', 1, True)
        if warn:
            return HandleCommandResult(-errno.EBUSY, '', warn_message)

        mgr_daemons = self.mgr.cache.get_daemons_by_type(self.TYPE)
        active = self.get_active_daemon(mgr_daemons).daemon_id
        if active in daemon_ids:
            warn_message = 'ALERT: Cannot stop active Mgr daemon, Please switch active Mgrs with \'ceph mgr fail %s\'' % active
            return HandleCommandResult(-errno.EBUSY, '', warn_message)

        return HandleCommandResult(0, warn_message, '')
