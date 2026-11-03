from typing import List, Any, Tuple, Dict
from ..service_registry import register_cephadm_service
from orchestrator import DaemonDescription
from cephadm.services.cephadmservice import CephadmService, DaemonDeployContext
from mgr_util import build_url
import logging

logger = logging.getLogger(__name__)


@register_cephadm_service
class LokiService(CephadmService):
    TYPE = 'loki'
    DEFAULT_SERVICE_PORT = 3100

    def generate_config(
            self,
            deploy_ctx: DaemonDeployContext,
    ) -> Tuple[Dict[str, Any], List[str]]:
        daemon_spec = deploy_ctx.daemon_spec
        spec = deploy_ctx.service_spec
        assert self.TYPE == daemon_spec.daemon_type
        yml = self.mgr.template.render('services/loki.yml.j2')
        return {
            "files": {
                "loki.yml": yml
            }
        }, self.get_dependencies(self.mgr, spec, daemon_spec.daemon_type)

    def get_active_daemon(self, daemon_descrs: List[DaemonDescription]) -> DaemonDescription:
        if daemon_descrs:
            return daemon_descrs[0]
        return DaemonDescription()

    def config_dashboard(self, daemon_descrs: List[DaemonDescription]) -> None:
        dd = self.get_active_daemon(daemon_descrs)
        assert dd.hostname is not None
        addr = dd.ip if dd.ip else self.mgr.get_fqdn(dd.hostname)
        port = dd.ports[0] if dd.ports else self.DEFAULT_SERVICE_PORT
        service_url = build_url(scheme='http', host=addr, port=port)
        self._set_value_on_dashboard(
            'Loki',
            'dashboard get-loki-api-host',
            'dashboard set-loki-api-host',
            service_url
        )

    def pre_remove(self, daemon: DaemonDescription) -> None:
        if daemon.hostname is None:
            return
        try:
            current_api_host = self.mgr.check_mon_command(
                {"prefix": "dashboard get-loki-api-host"}).stdout.strip()
            daemon_addr = daemon.ip if daemon.ip else self.mgr.get_fqdn(daemon.hostname)
            daemon_port = daemon.ports[0] if daemon.ports else self.DEFAULT_SERVICE_PORT
            service_url = build_url(scheme='http', host=daemon_addr, port=daemon_port)

            if current_api_host == service_url:
                remaining_daemons = [
                    d for d in self.mgr.cache.get_daemons_by_service(self.TYPE)
                    if d.name() != daemon.name()
                ]
                if remaining_daemons:
                    self.config_dashboard(remaining_daemons)
                    logger.info("Updated dashboard API settings to point to a remaining Loki daemon")
                else:
                    self.mgr.check_mon_command({"prefix": "dashboard reset-loki-api-host"})
                    self.mgr.check_mon_command({"prefix": "dashboard reset-loki-api-ssl-verify"})
                    logger.info("Reset dashboard API settings as no Loki daemons are remaining")
            else:
                logger.info(f"Loki {daemon.name()} removed; no changes to dashboard API settings")
        except Exception as e:
            logger.error(f"Error in Loki pre_remove: {str(e)}")
