import json
import logging
from typing import TYPE_CHECKING, List, Dict, Any, Tuple

from orchestrator import OrchestratorError
from mgr_util import ServerConfigException, verify_tls

from .cephadmservice import CephadmService, CephadmDaemonDeploySpec

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)


class CephadmExporterConfig:
    required_keys = ['crt', 'key', 'token', 'port']
    DEFAULT_PORT = '9443'

    def __init__(self, mgr, crt="", key="", token="", port=""):
        # type: (CephadmOrchestrator, str, str, str, str) -> None
        self.mgr = mgr
        self.crt = crt
        self.key = key
        self.token = token
        self.port = port

    @property
    def ready(self) -> bool:
        return all([self.crt, self.key, self.token, self.port])

    def load_from_store(self) -> None:
        cfg = self.mgr._get_exporter_config()

        assert isinstance(cfg, dict)
        self.crt = cfg.get('crt', "")
        self.key = cfg.get('key', "")
        self.token = cfg.get('token', "")
        self.port = cfg.get('port', "")

    def load_from_json(self, json_str: str) -> Tuple[int, str]:
        try:
            cfg = json.loads(json_str)
        except ValueError:
            return 1, "Invalid JSON provided - unable to load"

        if not all([k in cfg for k in CephadmExporterConfig.required_keys]):
            return 1, "JSON file must contain crt, key, token and port"

        self.crt = cfg.get('crt')
        self.key = cfg.get('key')
        self.token = cfg.get('token')
        self.port = cfg.get('port')

        return 0, ""

    def validate_config(self) -> Tuple[int, str]:
        if not self.ready:
            return 1, "Incomplete configuration. cephadm-exporter needs crt, key, token and port to be set"

        for check in [self._validate_tls, self._validate_token, self._validate_port]:
            rc, reason = check()
            if rc:
                return 1, reason

        return 0, ""

    def _validate_tls(self) -> Tuple[int, str]:

        try:
            verify_tls(self.crt, self.key)
        except ServerConfigException as e:
            return 1, str(e)

        return 0, ""

    def _validate_token(self) -> Tuple[int, str]:
        if not isinstance(self.token, str):
            return 1, "token must be a string"
        if len(self.token) < 8:
            return 1, "Token must be a string of at least 8 chars in length"

        return 0, ""

    def _validate_port(self) -> Tuple[int, str]:
        try:
            p = int(str(self.port))
            if p <= 1024:
                raise ValueError
        except ValueError:
            return 1, "Port must be a integer (>1024)"

        return 0, ""


class CephadmExporter(CephadmService):
    TYPE = 'cephadm-exporter'

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type

        cfg = CephadmExporterConfig(self.mgr)
        cfg.load_from_store()

        if cfg.ready:
            rc, reason = cfg.validate_config()
            if rc:
                raise OrchestratorError(reason)
        else:
            logger.info(
                "Incomplete/Missing configuration, applying defaults")
            self.mgr._set_exporter_defaults()
            cfg.load_from_store()

        if not daemon_spec.ports:
            daemon_spec.ports = [int(cfg.port)]

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)

        return daemon_spec

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        assert self.TYPE == daemon_spec.daemon_type
        deps: List[str] = []

        cfg = CephadmExporterConfig(self.mgr)
        cfg.load_from_store()

        if cfg.ready:
            rc, reason = cfg.validate_config()
            if rc:
                raise OrchestratorError(reason)
        else:
            logger.info("Using default configuration for cephadm-exporter")
            self.mgr._set_exporter_defaults()
            cfg.load_from_store()

        config = {
            "crt": cfg.crt,
            "key": cfg.key,
            "token": cfg.token
        }
        return config, deps

    def purge(self, service_name: str) -> None:
        logger.info("Purging cephadm-exporter settings from mon K/V store")
        self.mgr._clear_exporter_config_settings()
