import json
import ssl
import base64

from urllib.error import HTTPError, URLError
from typing import List, Any, Dict, Tuple, Optional, MutableMapping

from .cephadmservice import CephadmDaemonDeploySpec, CephService
from ceph.deployment.service_spec import ServiceSpec, PlacementSpec
from ceph.utils import http_req
from orchestrator import OrchestratorError


class NodeProxy(CephService):
    TYPE = 'node-proxy'

    def prepare_create(self, daemon_spec: CephadmDaemonDeploySpec) -> CephadmDaemonDeploySpec:
        assert self.TYPE == daemon_spec.daemon_type
        daemon_id, host = daemon_spec.daemon_id, daemon_spec.host

        if not self.mgr.http_server.agent:
            raise OrchestratorError('Cannot deploy node-proxy before creating cephadm endpoint')

        keyring = self.get_keyring_with_caps(self.get_auth_entity(daemon_id, host=host), [])
        daemon_spec.keyring = keyring
        self.mgr.node_proxy_cache.update_keyring(host, keyring)

        daemon_spec.final_config, daemon_spec.deps = self.generate_config(daemon_spec)

        return daemon_spec

    def generate_config(self, daemon_spec: CephadmDaemonDeploySpec) -> Tuple[Dict[str, Any], List[str]]:
        # node-proxy is re-using the agent endpoint and therefore
        # needs similar checks to see if the endpoint is ready.
        self.agent_endpoint = self.mgr.http_server.agent
        try:
            assert self.agent_endpoint
            assert self.agent_endpoint.ssl_certs.get_root_cert()
            assert self.agent_endpoint.server_port
        except Exception:
            raise OrchestratorError(
                'Cannot deploy node-proxy daemons until cephadm endpoint has finished generating certs')

        listener_cert, listener_key = self.agent_endpoint.ssl_certs.generate_cert(daemon_spec.host, self.mgr.inventory.get_addr(daemon_spec.host))
        cfg = {
            'target_ip': self.mgr.get_mgr_ip(),
            'target_port': self.agent_endpoint.server_port,
            'name': f'node-proxy.{daemon_spec.host}',
            'keyring': daemon_spec.keyring,
            'root_cert.pem': self.agent_endpoint.ssl_certs.get_root_cert(),
            'listener.crt': listener_cert,
            'listener.key': listener_key,
        }
        config = {'node-proxy.json': json.dumps(cfg)}

        return config, sorted([str(self.mgr.get_mgr_ip()), str(self.agent_endpoint.server_port),
                               self.agent_endpoint.ssl_certs.get_root_cert()])

    def handle_hw_monitoring_setting(self) -> bool:
        # function to apply or remove node-proxy service spec depending
        # on whether the hw_mointoring config option is set or not.
        # It should return True when it either creates or deletes a spec
        # and False otherwise.
        if self.mgr.hw_monitoring:
            if 'node-proxy' not in self.mgr.spec_store:
                spec = ServiceSpec(
                    service_type='node-proxy',
                    placement=PlacementSpec(host_pattern='*')
                )
                self.mgr.spec_store.save(spec)
                return True
            return False
        else:
            if 'node-proxy' in self.mgr.spec_store:
                self.mgr.spec_store.rm('node-proxy')
                return True
            return False

    def get_ssl_ctx(self) -> ssl.SSLContext:
        ssl_root_crt = self.mgr.http_server.agent.ssl_certs.get_root_cert()
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = True
        ssl_ctx.verify_mode = ssl.CERT_REQUIRED
        ssl_ctx.load_verify_locations(cadata=ssl_root_crt)
        return ssl_ctx

    def led(self, light_type: str, action: str, hostname: str, device: Optional[str] = None) -> Dict[str, Any]:
        ssl_ctx: ssl.SSLContext = self.get_ssl_ctx()
        header: MutableMapping[str, str] = {}
        method: str = 'PATCH' if action in ['on', 'off'] else 'GET'
        payload: Optional[Dict[str, str]] = None
        addr: str = self.mgr.inventory.get_addr(hostname)
        endpoint: List[str] = ['led', light_type]
        _device: str = device if device else ''

        if light_type == 'drive':
            endpoint.append(_device)

        if method == 'PATCH':
            payload = dict(state=action)

        header = self.generate_auth_header(hostname)

        endpoint = f'/{"/".join(endpoint)}'

        try:
            headers, result, status = http_req(hostname=addr,
                                               port='8080',
                                               headers=header,
                                               method=method,
                                               data=json.dumps(payload),
                                               endpoint=endpoint,
                                               ssl_ctx=ssl_ctx)
            result_json = json.loads(result)
        except HTTPError as e:
            self.mgr.log.error(e)
            raise
        except URLError as e:
            raise RuntimeError(e)

        return result_json

    def generate_auth_header(self, hostname: str) -> Dict[str, str]:
        try:
            username = self.mgr.node_proxy_cache.oob[hostname]['username']
            password = self.mgr.node_proxy_cache.oob[hostname]['password']
            auth: bytes = f'{username}:{password}'.encode('utf-8')
            auth_str: str = base64.b64encode(auth).decode('utf-8')
            header = {'Authorization': f'Basic {auth_str}'}
        except KeyError as e:
            self.mgr.log.error(f'Check oob information is provided for {hostname}.')
            raise RuntimeError(e)
        return header

    def shutdown(self, hostname: str, force: Optional[bool] = False) -> Dict[str, Any]:
        ssl_ctx: ssl.SSLContext = self.get_ssl_ctx()
        header: Dict[str, str] = self.generate_auth_header(hostname)
        addr: str = self.mgr.inventory.get_addr(hostname)

        endpoint = '/shutdown'
        payload: Dict[str, Optional[bool]] = dict(force=force)

        try:
            headers, result, status = http_req(hostname=addr,
                                               port='8080',
                                               headers=header,
                                               data=json.dumps(payload),
                                               endpoint=endpoint,
                                               ssl_ctx=ssl_ctx)
            result_json = json.loads(result)
        except HTTPError as e:
            self.mgr.log.error(e)
            raise
        except URLError as e:
            raise RuntimeError(e)

        return result_json

    def powercycle(self, hostname: str) -> Dict[str, Any]:
        ssl_ctx: ssl.SSLContext = self.get_ssl_ctx()
        header: Dict[str, str] = self.generate_auth_header(hostname)
        addr: str = self.mgr.inventory.get_addr(hostname)

        endpoint = '/powercycle'

        try:
            headers, result, status = http_req(hostname=addr,
                                               port='8080',
                                               headers=header,
                                               data="{}",
                                               endpoint=endpoint,
                                               ssl_ctx=ssl_ctx)
            result_json = json.loads(result)
        except HTTPError as e:
            self.mgr.log.error(e)
            raise
        except URLError as e:
            raise RuntimeError(e)

        return result_json
