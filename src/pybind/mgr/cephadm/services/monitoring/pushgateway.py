from typing import TYPE_CHECKING
import errno
from typing import List, Any, Tuple, Dict, Optional
from mgr_module import HandleCommandResult
from ..service_registry import register_cephadm_service
from orchestrator import DaemonDescription
from ceph.deployment.service_spec import ServiceSpec
from cephadm.services.cephadmservice import CephadmService, DaemonDeployContext, next_action_for_mgmt_stack_service
from ... import utils


if TYPE_CHECKING:
    from ...module import CephadmOrchestrator


@register_cephadm_service
class PushgatewayService(CephadmService):
    """Manages the Prometheus Pushgateway service.

    Pushgateway acts as a metric cache for short-lived batch jobs
    (e.g. RGW per-bucket/per-user stats collection) that can't be
    scraped directly by Prometheus.
    """
    TYPE = 'pushgateway'
    DEFAULT_SERVICE_PORT = 9091

    @property
    def needs_monitoring(self) -> bool:
        # pushgateway must be scraped by prometheus
        return True

    @classmethod
    def _get_service_dependencies(
        cls,
        mgr: "CephadmOrchestrator",
        spec: Optional[ServiceSpec] = None,
        daemon_type: Optional[str] = None,
    ) -> List[str]:
        deps = []
        deps.append(f'secure_monitoring_stack:{mgr.secure_monitoring_stack}')
        deps += mgr.cache.get_daemons_by_types(['mgmt-gateway'])
        return sorted(deps)

    def generate_config(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> Tuple[Dict[str, Any], List[str]]:
        daemon_spec = deploy_ctx.daemon_spec
        spec = deploy_ctx.service_spec
        assert self.TYPE == daemon_spec.daemon_type
        security_enabled, mgmt_gw_enabled, _ = self.mgr._get_security_config()
        if security_enabled:
            # security is on, so generate TLS certs and web config.
            # if mgmt-gateway is also present, enable mTLS (mutual TLS).
            tls_pair = self.get_certificates(daemon_spec)
            r = {
                'files': {
                    'web.yml': self.mgr.template.render('services/pushgateway/web.yml.j2',
                                                        {'enable_mtls': mgmt_gw_enabled}),
                    'root_cert.pem': self.mgr.cert_mgr.get_root_ca(),
                    'pushgateway.crt': tls_pair.cert,
                    'pushgateway.key': tls_pair.key,
                },
                'web_config': '/etc/pushgateway/web.yml'
            }
        else:
            # security is off, no TLS config needed, run plain HTTP
            r = {}

        return r, self.get_dependencies(
            self.mgr, spec, daemon_spec.daemon_type)

    def ok_to_stop(self,
                   daemon_ids: List[str],
                   force: bool = False,
                   known: Optional[List[str]] = None) -> HandleCommandResult:
        # make sure at least 1 pushgateway daemon stays running
        warn, warn_message = self._enough_daemons_to_stop(self.TYPE, daemon_ids, 'Pushgateway', 1)
        if warn and not force:
            return HandleCommandResult(-errno.EBUSY, '', warn_message)
        return HandleCommandResult(0, warn_message, '')

    def choose_next_action(
        self,
        scheduled_action: utils.Action,
        daemon_type: Optional[str],
        spec: Optional[ServiceSpec],
        curr_deps: List[str],
        last_deps: List[str],
        daemon: Optional[DaemonDescription] = None,
    ) -> utils.NextDaemonStep:
        # decide whether to redeploy or reconfig based on what changed.
        return next_action_for_mgmt_stack_service(
            scheduled_action, daemon_type, spec, curr_deps, last_deps
        )
