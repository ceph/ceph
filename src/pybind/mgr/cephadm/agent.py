try:
    import cherrypy
    from cherrypy._cpserver import Server
except ImportError:
    # to avoid sphinx build crash
    class Server:  # type: ignore
        pass

import json
import logging
import socket
import ssl
import tempfile
import threading
import time
import base64

from orchestrator import DaemonDescriptionStatus
from orchestrator._interface import daemon_type_to_service
from ceph.utils import datetime_now
from ceph.deployment.inventory import Devices
from ceph.deployment.service_spec import ServiceSpec, PlacementSpec
from cephadm.services.cephadmservice import CephadmDaemonDeploySpec
from cephadm.ssl_cert_utils import SSLCerts
from mgr_util import test_port_allocation, PortAlreadyInUse
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

from typing import Any, Dict, List, Set, Tuple, TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


def cherrypy_filter(record: logging.LogRecord) -> int:
    blocked = [
        'TLSV1_ALERT_DECRYPT_ERROR'
    ]
    msg = record.getMessage()
    return not any([m for m in blocked if m in msg])


logging.getLogger('cherrypy.error').addFilter(cherrypy_filter)
cherrypy.log.access_log.propagate = False


class AgentEndpoint:

    KV_STORE_AGENT_ROOT_CERT = 'cephadm_agent/root/cert'
    KV_STORE_AGENT_ROOT_KEY = 'cephadm_agent/root/key'

    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr
        self.ssl_certs = SSLCerts()
        self.server_port = 7150
        self.server_addr = self.mgr.get_mgr_ip()

    def configure_routes(self) -> None:
        conf = {'/': {'tools.trailing_slash.on': False}}

        cherrypy.tree.mount(self.host_data, '/data', config=conf)
        cherrypy.tree.mount(self.node_proxy, '/node-proxy', config=conf)

    def configure_tls(self, server: Server) -> None:
        old_cert = self.mgr.get_store(self.KV_STORE_AGENT_ROOT_CERT)
        old_key = self.mgr.get_store(self.KV_STORE_AGENT_ROOT_KEY)
        if old_cert and old_key:
            self.ssl_certs.load_root_credentials(old_cert, old_key)
        else:
            self.ssl_certs.generate_root_cert(self.mgr.get_mgr_ip())
            self.mgr.set_store(self.KV_STORE_AGENT_ROOT_CERT, self.ssl_certs.get_root_cert())
            self.mgr.set_store(self.KV_STORE_AGENT_ROOT_KEY, self.ssl_certs.get_root_key())

        host = self.mgr.get_hostname()
        addr = self.mgr.get_mgr_ip()
        server.ssl_certificate, server.ssl_private_key = self.ssl_certs.generate_cert_files(host, addr)

    def find_free_port(self) -> None:
        max_port = self.server_port + 150
        while self.server_port <= max_port:
            try:
                test_port_allocation(self.server_addr, self.server_port)
                self.host_data.socket_port = self.server_port
                self.mgr.log.debug(f'Cephadm agent endpoint using {self.server_port}')
                return
            except PortAlreadyInUse:
                self.server_port += 1
        self.mgr.log.error(f'Cephadm agent could not find free port in range {max_port - 150}-{max_port} and failed to start')

    def configure(self) -> None:
        self.host_data = HostData(self.mgr, self.server_port, self.server_addr)
        self.configure_tls(self.host_data)
        self.node_proxy = NodeProxy(self.mgr)
        self.configure_routes()
        self.find_free_port()


class NodeProxy:
    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr = mgr
        self.ssl_root_crt = self.mgr.http_server.agent.ssl_certs.get_root_cert()
        self.ssl_ctx = ssl.create_default_context()
        self.ssl_ctx.check_hostname = True
        self.ssl_ctx.verify_mode = ssl.CERT_REQUIRED
        self.ssl_ctx.load_verify_locations(cadata=self.ssl_root_crt)

    def _cp_dispatch(self, vpath: List[str]) -> "NodeProxy":
        if len(vpath) == 2:
            hostname = vpath.pop(0)
            cherrypy.request.params['hostname'] = hostname
        return self

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['POST'])
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def idrac(self) -> Dict[str, Any]:
        data: Dict[str, Any] = cherrypy.request.json
        results: Dict[str, Any] = {}

        if self.validate_node_proxy_data(data):
            host = data["cephx"]["name"]
            results['result'] = self.mgr.node_proxy.idrac.get(host)
        else:
            results['result'] = self.validate_msg

        return results

    def validate_node_proxy_data(self, data: Dict[str, Any]) -> bool:
        self.validate_msg = 'valid node-proxy data received.'
        cherrypy.response.status = 200
        try:
            if 'cephx' not in data.keys():
                cherrypy.response.status = 400
                self.validate_msg = 'The field \'cephx\' must be provided.'
            elif 'name' not in data['cephx'].keys():
                cherrypy.response.status = 400
                self.validate_msg = 'The field \'host\' must be provided.'
            elif 'secret' not in data['cephx'].keys():
                cherrypy.response.status = 400
                self.validate_msg = 'The agent keyring must be provided.'
            elif not self.mgr.agent_cache.agent_keys.get(data['cephx']['name']):
                cherrypy.response.status = 400
                self.validate_msg = f'Make sure the agent is running on {data["cephx"]["name"]}'
            elif data['cephx']['secret'] != self.mgr.agent_cache.agent_keys[data['cephx']['name']]:
                cherrypy.response.status = 403
                self.validate_msg = f'Got wrong keyring from agent on host {data["cephx"]["name"]}.'
        except AttributeError:
            cherrypy.response.status = 400
            self.validate_msg = 'Malformed data received.'

        return cherrypy.response.status == 200

    def get_nok_members(self,
                        component: str,
                        data: Dict[str, Any]) -> List[Dict[str, str]]:
        nok_members: List[Dict[str, str]] = []

        for member in data.keys():
            # Force a fake error for testing purpose
            if component == 'storage':
                _status = 'critical'
                state = "[Fake error] device is faulty."
            elif component == 'power':
                _status = 'critical'
                state = "[Fake error] power supply unplugged."
            else:
                _status = data[member]['status']['health'].lower()
            if _status.lower() != 'ok':
                # state = data[member]['status']['state']
                _member = dict(
                    member=member,
                    status=_status,
                    state=state
                )
                nok_members.append(_member)

        return nok_members

    def raise_alert(self, data: Dict[str, Any]) -> None:
        mapping: Dict[str, str] = {
            'storage': 'HARDWARE_STORAGE',
            'memory': 'HARDWARE_MEMORY',
            'processors': 'HARDWARE_PROCESSORS',
            'network': 'HARDWARE_NETWORK',
            'power': 'HARDWARE_POWER',
            'fans': 'HARDWARE_FANS'
        }

        for component in data['patch']['status'].keys():
            self.mgr.remove_health_warning(mapping[component])
            nok_members = self.get_nok_members(component, data['patch']['status'][component])

            if nok_members:
                count = len(nok_members)
                self.mgr.set_health_warning(
                    mapping[component],
                    summary=f'{count} {component} member{"s" if count > 1 else ""} {"are" if count > 1 else "is"} not ok',
                    count=count,
                    detail=[f"{member['member']} is {member['status']}: {member['state']}" for member in nok_members],
                )

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['POST'])
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def data(self) -> Dict[str, Any]:
        results: Dict[str, Any] = {}

        data: Dict[str, Any] = cherrypy.request.json
        if self.validate_node_proxy_data(data):
            host = data['cephx']['name']
            self.mgr.node_proxy.save(host, data['patch'])
            self.raise_alert(data)

        results["result"] = self.validate_msg

        return results

    def query_endpoint(self,
                       addr: str = '',
                       port: str = '',
                       method: Optional[str] = None,
                       headers: Optional[Dict[str, str]] = {},
                       data: Optional[bytes] = None,
                       endpoint: str = '',
                       ssl_ctx: Optional[Any] = None) -> Tuple[int, Dict[str, Any]]:
        url = f'https://{addr}:{port}{endpoint}'
        _headers = headers
        response_json = {}
        if not _headers.get('Content-Type'):
            # default to application/json if nothing provided
            _headers['Content-Type'] = 'application/json'
        _data = bytes(data, 'ascii') if data else None
        try:
            req = Request(url, _data, _headers, method=method)
            with urlopen(req, context=ssl_ctx) as response:
                response_str = response.read()
                response_json = json.loads(response_str)
                response_status = response.status
        except HTTPError as e:
            self.mgr.log.debug(f"{e.code} {e.reason}")
            response_status = e.code
        except URLError as e:
            self.mgr.log.debug(f"{e.reason}")
            raise
        except Exception as e:
            self.mgr.log.error(f"{e}")
            raise
        return (response_status, response_json)

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET', 'PATCH'])
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    def led(self, **kw: Any) -> Dict[str, Any]:
        method: str = cherrypy.request.method
        hostname: Optional[str] = kw.get('hostname')
        headers: Dict[str, str] = {}

        if not hostname:
            msg: str = "listing enclosure LED status for all nodes is not implemented."
            self.mgr.log.debug(msg)
            raise cherrypy.HTTPError(501, msg)

        addr = self.mgr.inventory.get_addr(hostname)

        if method == 'PATCH':
            # TODO(guits): need to check the request is authorized
            # allowing a specific keyring only ? (client.admin or client.agent.. ?)
            data: str = json.dumps(cherrypy.request.json)
            username = self.mgr.node_proxy.idrac[hostname]['username']
            password = self.mgr.node_proxy.idrac[hostname]['password']
            auth = f"{username}:{password}".encode("utf-8")
            auth = base64.b64encode(auth).decode("utf-8")
            headers = {"Authorization": f"Basic {auth}"}

        try:
            status, result = self.query_endpoint(data=data,
                                                headers=headers,
                                                addr=addr,
                                                method=method,
                                                port=8080,
                                                endpoint='/led',
                                                ssl_ctx=self.ssl_ctx)
        except URLError as e:
            raise cherrypy.HTTPError(502, f"{e}")
        cherrypy.response.status = status
        return result

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def fullreport(self, **kw: Any) -> Dict[str, Any]:
        return self.mgr.node_proxy.fullreport(**kw)

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def criticals(self, **kw: Any) -> Dict[str, Any]:
        return self.mgr.node_proxy.criticals()

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def summary(self, **kw: Any) -> Dict[str, Any]:
        return self.mgr.node_proxy.summary(**kw)

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def memory(self, **kw: Any) -> Dict[str, Any]:
        return self.mgr.node_proxy.common('memory', **kw)

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def network(self, **kw: Any) -> Dict[str, Any]:
        return self.mgr.node_proxy.common('network', **kw)

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def processors(self, **kw: Any) -> Dict[str, Any]:
        return self.mgr.node_proxy.common('processors', **kw)

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def storage(self, **kw: Any) -> Dict[str, Any]:
        return self.mgr.node_proxy.common('storage', **kw)

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def power(self, **kw: Any) -> Dict[str, Any]:
        return self.mgr.node_proxy.common('power', **kw)

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def fans(self, **kw: Any) -> Dict[str, Any]:
        return self.mgr.node_proxy.common('fans', **kw)

    @cherrypy.expose
    @cherrypy.tools.allow(methods=['GET'])
    @cherrypy.tools.json_out()
    def firmwares(self, **kw: Any) -> Dict[str, Any]:
        return self.mgr.node_proxy.firmwares(**kw)


class HostData(Server):
    exposed = True

    def __init__(self, mgr: "CephadmOrchestrator", port: int, host: str):
        self.mgr = mgr
        super().__init__()
        self.socket_port = port
        self.socket_host = host
        self.subscribe()

    def stop(self) -> None:
        # we must call unsubscribe before stopping the server,
        # otherwise the port is not released and we will get
        # an exception when trying to restart it
        self.unsubscribe()
        super().stop()

    @cherrypy.tools.allow(methods=['POST'])
    @cherrypy.tools.json_in()
    @cherrypy.tools.json_out()
    @cherrypy.expose
    def index(self) -> Dict[str, Any]:
        data: Dict[str, Any] = cherrypy.request.json
        results: Dict[str, Any] = {}
        try:
            self.check_request_fields(data)
        except Exception as e:
            results['result'] = f'Bad metadata: {e}'
            self.mgr.log.warning(f'Received bad metadata from an agent: {e}')
        else:
            # if we got here, we've already verified the keyring of the agent. If
            # host agent is reporting on is marked offline, it shouldn't be any more
            self.mgr.offline_hosts_remove(data['host'])
            results['result'] = self.handle_metadata(data)
        return results

    def check_request_fields(self, data: Dict[str, Any]) -> None:
        fields = '{' + ', '.join([key for key in data.keys()]) + '}'
        if 'host' not in data:
            raise Exception(
                f'No host in metadata from agent ("host" field). Only received fields {fields}')
        host = data['host']
        if host not in self.mgr.cache.get_hosts():
            raise Exception(f'Received metadata from agent on unknown hostname {host}')
        if 'keyring' not in data:
            raise Exception(
                f'Agent on host {host} not reporting its keyring for validation ("keyring" field). Only received fields {fields}')
        if host not in self.mgr.agent_cache.agent_keys:
            raise Exception(f'No agent keyring stored for host {host}. Cannot verify agent')
        if data['keyring'] != self.mgr.agent_cache.agent_keys[host]:
            raise Exception(f'Got wrong keyring from agent on host {host}.')
        if 'port' not in data:
            raise Exception(
                f'Agent on host {host} not reporting its listener port ("port" fields). Only received fields {fields}')
        if 'ack' not in data:
            raise Exception(
                f'Agent on host {host} not reporting its counter value ("ack" field). Only received fields {fields}')
        try:
            int(data['ack'])
        except Exception as e:
            raise Exception(
                f'Counter value from agent on host {host} could not be converted to an integer: {e}')
        metadata_types = ['ls', 'networks', 'facts', 'volume']
        metadata_types_str = '{' + ', '.join(metadata_types) + '}'
        if not all(item in data.keys() for item in metadata_types):
            self.mgr.log.warning(
                f'Agent on host {host} reported incomplete metadata. Not all of {metadata_types_str} were present. Received fields {fields}')

    def handle_metadata(self, data: Dict[str, Any]) -> str:
        try:
            host = data['host']
            self.mgr.agent_cache.agent_ports[host] = int(data['port'])
            if host not in self.mgr.agent_cache.agent_counter:
                self.mgr.agent_cache.agent_counter[host] = 1
                self.mgr.agent_helpers._request_agent_acks({host})
                res = f'Got metadata from agent on host {host} with no known counter entry. Starting counter at 1 and requesting new metadata'
                self.mgr.log.debug(res)
                return res

            # update timestamp of most recent agent update
            self.mgr.agent_cache.agent_timestamp[host] = datetime_now()

            error_daemons_old = set([dd.name() for dd in self.mgr.cache.get_error_daemons()])
            daemon_count_old = len(self.mgr.cache.get_daemons_by_host(host))

            up_to_date = False

            int_ack = int(data['ack'])
            if int_ack == self.mgr.agent_cache.agent_counter[host]:
                up_to_date = True
            else:
                # we got old counter value with message, inform agent of new timestamp
                if not self.mgr.agent_cache.messaging_agent(host):
                    self.mgr.agent_helpers._request_agent_acks({host})
                self.mgr.log.debug(
                    f'Received old metadata from agent on host {host}. Requested up-to-date metadata.')

            if 'ls' in data and data['ls']:
                self.mgr._process_ls_output(host, data['ls'])
                self.mgr.update_failed_daemon_health_check()
            if 'networks' in data and data['networks']:
                self.mgr.cache.update_host_networks(host, data['networks'])
            if 'facts' in data and data['facts']:
                self.mgr.cache.update_host_facts(host, json.loads(data['facts']))
            if 'volume' in data and data['volume']:
                ret = Devices.from_json(json.loads(data['volume']))
                self.mgr.cache.update_host_devices(host, ret.devices)

            if (
                error_daemons_old != set([dd.name() for dd in self.mgr.cache.get_error_daemons()])
                or daemon_count_old != len(self.mgr.cache.get_daemons_by_host(host))
            ):
                self.mgr.log.debug(
                    f'Change detected in state of daemons from {host} agent metadata. Kicking serve loop')
                self.mgr._kick_serve_loop()

            if up_to_date and ('ls' in data and data['ls']):
                was_out_of_date = not self.mgr.cache.all_host_metadata_up_to_date()
                self.mgr.cache.metadata_up_to_date[host] = True
                if was_out_of_date and self.mgr.cache.all_host_metadata_up_to_date():
                    self.mgr.log.debug(
                        'New metadata from agent has made all hosts up to date. Kicking serve loop')
                    self.mgr._kick_serve_loop()
                self.mgr.log.debug(
                    f'Received up-to-date metadata from agent on host {host}.')

            self.mgr.agent_cache.save_agent(host)
            return 'Successfully processed metadata.'

        except Exception as e:
            err_str = f'Failed to update metadata with metadata from agent on host {host}: {e}'
            self.mgr.log.warning(err_str)
            return err_str


class AgentMessageThread(threading.Thread):
    def __init__(self, host: str, port: int, data: Dict[Any, Any], mgr: "CephadmOrchestrator", daemon_spec: Optional[CephadmDaemonDeploySpec] = None) -> None:
        self.mgr = mgr
        self.agent = mgr.http_server.agent
        self.host = host
        self.addr = self.mgr.inventory.get_addr(host) if host in self.mgr.inventory else host
        self.port = port
        self.data: str = json.dumps(data)
        self.daemon_spec: Optional[CephadmDaemonDeploySpec] = daemon_spec
        super().__init__(target=self.run)

    def run(self) -> None:
        self.mgr.log.debug(f'Sending message to agent on host {self.host}')
        self.mgr.agent_cache.sending_agent_message[self.host] = True
        try:
            assert self.agent
            root_cert = self.agent.ssl_certs.get_root_cert()
            root_cert_tmp = tempfile.NamedTemporaryFile()
            root_cert_tmp.write(root_cert.encode('utf-8'))
            root_cert_tmp.flush()
            root_cert_fname = root_cert_tmp.name

            cert, key = self.agent.ssl_certs.generate_cert(
                self.mgr.get_hostname(), self.mgr.get_mgr_ip())

            cert_tmp = tempfile.NamedTemporaryFile()
            cert_tmp.write(cert.encode('utf-8'))
            cert_tmp.flush()
            cert_fname = cert_tmp.name

            key_tmp = tempfile.NamedTemporaryFile()
            key_tmp.write(key.encode('utf-8'))
            key_tmp.flush()
            key_fname = key_tmp.name

            ssl_ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=root_cert_fname)
            ssl_ctx.verify_mode = ssl.CERT_REQUIRED
            ssl_ctx.check_hostname = True
            ssl_ctx.load_cert_chain(cert_fname, key_fname)
        except Exception as e:
            self.mgr.log.error(f'Failed to get certs for connecting to agent: {e}')
            self.mgr.agent_cache.sending_agent_message[self.host] = False
            return
        try:
            bytes_len: str = str(len(self.data.encode('utf-8')))
            if len(bytes_len.encode('utf-8')) > 10:
                raise Exception(
                    f'Message is too big to send to agent. Message size is {bytes_len} bytes!')
            while len(bytes_len.encode('utf-8')) < 10:
                bytes_len = '0' + bytes_len
        except Exception as e:
            self.mgr.log.error(f'Failed to get length of json payload: {e}')
            self.mgr.agent_cache.sending_agent_message[self.host] = False
            return
        for retry_wait in [3, 5]:
            try:
                agent_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                secure_agent_socket = ssl_ctx.wrap_socket(agent_socket, server_hostname=self.addr)
                secure_agent_socket.connect((self.addr, self.port))
                msg = (bytes_len + self.data)
                secure_agent_socket.sendall(msg.encode('utf-8'))
                agent_response = secure_agent_socket.recv(1024).decode()
                self.mgr.log.debug(f'Received "{agent_response}" from agent on host {self.host}')
                if self.daemon_spec:
                    self.mgr.agent_cache.agent_config_successfully_delivered(self.daemon_spec)
                self.mgr.agent_cache.sending_agent_message[self.host] = False
                return
            except ConnectionError as e:
                # if it's a connection error, possibly try to connect again.
                # We could have just deployed agent and it might not be ready
                self.mgr.log.debug(
                    f'Retrying connection to agent on {self.host} in {str(retry_wait)} seconds. Connection failed with: {e}')
                time.sleep(retry_wait)
            except Exception as e:
                # if it's not a connection error, something has gone wrong. Give up.
                self.mgr.log.error(f'Failed to contact agent on host {self.host}: {e}')
                self.mgr.agent_cache.sending_agent_message[self.host] = False
                return
        self.mgr.log.error(f'Could not connect to agent on host {self.host}')
        self.mgr.agent_cache.sending_agent_message[self.host] = False
        return


class CephadmAgentHelpers:
    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr: "CephadmOrchestrator" = mgr
        self.agent = mgr.http_server.agent

    def _request_agent_acks(self, hosts: Set[str], increment: bool = False, daemon_spec: Optional[CephadmDaemonDeploySpec] = None) -> None:
        for host in hosts:
            if increment:
                self.mgr.cache.metadata_up_to_date[host] = False
            if host not in self.mgr.agent_cache.agent_counter:
                self.mgr.agent_cache.agent_counter[host] = 1
            elif increment:
                self.mgr.agent_cache.agent_counter[host] = self.mgr.agent_cache.agent_counter[host] + 1
            payload: Dict[str, Any] = {'counter': self.mgr.agent_cache.agent_counter[host]}
            if daemon_spec:
                payload['config'] = daemon_spec.final_config
            message_thread = AgentMessageThread(
                host, self.mgr.agent_cache.agent_ports[host], payload, self.mgr, daemon_spec)
            message_thread.start()

    def _request_ack_all_not_up_to_date(self) -> None:
        self.mgr.agent_helpers._request_agent_acks(
            set([h for h in self.mgr.cache.get_hosts() if
                 (not self.mgr.cache.host_metadata_up_to_date(h)
                 and h in self.mgr.agent_cache.agent_ports and not self.mgr.agent_cache.messaging_agent(h))]))

    def _agent_down(self, host: str) -> bool:
        # if host is draining or drained (has _no_schedule label) there should not
        # be an agent deployed there and therefore we should return False
        if self.mgr.cache.is_host_draining(host):
            return False
        # if we haven't deployed an agent on the host yet, don't say an agent is down
        if not self.mgr.cache.get_daemons_by_type('agent', host=host):
            return False
        # if we don't have a timestamp, it's likely because of a mgr fail over.
        # just set the timestamp to now. However, if host was offline before, we
        # should not allow creating a new timestamp to cause it to be marked online
        if host not in self.mgr.agent_cache.agent_timestamp:
            if host in self.mgr.offline_hosts:
                return False
            self.mgr.agent_cache.agent_timestamp[host] = datetime_now()
        # agent hasn't reported in down multiplier * it's refresh rate. Something is likely wrong with it.
        down_mult: float = max(self.mgr.agent_down_multiplier, 1.5)
        time_diff = datetime_now() - self.mgr.agent_cache.agent_timestamp[host]
        if time_diff.total_seconds() > down_mult * float(self.mgr.agent_refresh_rate):
            return True
        return False

    def _update_agent_down_healthcheck(self, down_agent_hosts: List[str]) -> None:
        self.mgr.remove_health_warning('CEPHADM_AGENT_DOWN')
        if down_agent_hosts:
            detail: List[str] = []
            down_mult: float = max(self.mgr.agent_down_multiplier, 1.5)
            for agent in down_agent_hosts:
                detail.append((f'Cephadm agent on host {agent} has not reported in '
                              f'{down_mult * self.mgr.agent_refresh_rate} seconds. Agent is assumed '
                               'down and host may be offline.'))
            for dd in [d for d in self.mgr.cache.get_daemons_by_type('agent') if d.hostname in down_agent_hosts]:
                dd.status = DaemonDescriptionStatus.error
            self.mgr.set_health_warning(
                'CEPHADM_AGENT_DOWN',
                summary='%d Cephadm Agent(s) are not reporting. Hosts may be offline' % (
                    len(down_agent_hosts)),
                count=len(down_agent_hosts),
                detail=detail,
            )

    # this function probably seems very unnecessary, but it makes it considerably easier
    # to get the unit tests working. All unit tests that check which daemons were deployed
    # or services setup would have to be individually changed to expect an agent service or
    # daemons, OR we can put this in its own function then mock the function
    def _apply_agent(self) -> None:
        spec = ServiceSpec(
            service_type='agent',
            placement=PlacementSpec(host_pattern='*')
        )
        self.mgr.spec_store.save(spec)

    def _handle_use_agent_setting(self) -> bool:
        need_apply = False
        if self.mgr.use_agent:
            # on the off chance there are still agents hanging around from
            # when we turned the config option off, we need to redeploy them
            # we can tell they're in that state if we don't have a keyring for
            # them in the host cache
            for agent in self.mgr.cache.get_daemons_by_service('agent'):
                if agent.hostname not in self.mgr.agent_cache.agent_keys:
                    self.mgr._schedule_daemon_action(agent.name(), 'redeploy')
            if 'agent' not in self.mgr.spec_store:
                self.mgr.agent_helpers._apply_agent()
                need_apply = True
        else:
            if 'agent' in self.mgr.spec_store:
                self.mgr.spec_store.rm('agent')
                need_apply = True
            self.mgr.agent_cache.agent_counter = {}
            self.mgr.agent_cache.agent_timestamp = {}
            self.mgr.agent_cache.agent_keys = {}
            self.mgr.agent_cache.agent_ports = {}
        return need_apply

    def _check_agent(self, host: str) -> bool:
        down = False
        try:
            assert self.agent
            assert self.agent.ssl_certs.get_root_cert()
        except Exception:
            self.mgr.log.debug(
                f'Delaying checking agent on {host} until cephadm endpoint finished creating root cert')
            return down
        if self.mgr.agent_helpers._agent_down(host):
            down = True
        try:
            agent = self.mgr.cache.get_daemons_by_type('agent', host=host)[0]
            assert agent.daemon_id is not None
            assert agent.hostname is not None
        except Exception as e:
            self.mgr.log.debug(
                f'Could not retrieve agent on host {host} from daemon cache: {e}')
            return down
        try:
            spec = self.mgr.spec_store.active_specs.get('agent', None)
            deps = self.mgr._calc_daemon_deps(spec, 'agent', agent.daemon_id)
            last_deps, last_config = self.mgr.agent_cache.get_agent_last_config_deps(host)
            if not last_config or last_deps != deps:
                # if root cert is the dep that changed, we must use ssh to reconfig
                # so it's necessary to check this one specifically
                root_cert_match = False
                try:
                    root_cert = self.agent.ssl_certs.get_root_cert()
                    if last_deps and root_cert in last_deps:
                        root_cert_match = True
                except Exception:
                    pass
                daemon_spec = CephadmDaemonDeploySpec.from_daemon_description(agent)
                # we need to know the agent port to try to reconfig w/ http
                # otherwise there is no choice but a full ssh reconfig
                if host in self.mgr.agent_cache.agent_ports and root_cert_match and not down:
                    daemon_spec = self.mgr.cephadm_services[daemon_type_to_service(
                        daemon_spec.daemon_type)].prepare_create(daemon_spec)
                    self.mgr.agent_helpers._request_agent_acks(
                        hosts={daemon_spec.host},
                        increment=True,
                        daemon_spec=daemon_spec,
                    )
                else:
                    self.mgr._daemon_action(daemon_spec, action='reconfig')
                return down
        except Exception as e:
            self.mgr.log.debug(
                f'Agent on host {host} not ready to have config and deps checked: {e}')
        action = self.mgr.cache.get_scheduled_daemon_action(agent.hostname, agent.name())
        if action:
            try:
                daemon_spec = CephadmDaemonDeploySpec.from_daemon_description(agent)
                self.mgr._daemon_action(daemon_spec, action=action)
                self.mgr.cache.rm_scheduled_daemon_action(agent.hostname, agent.name())
            except Exception as e:
                self.mgr.log.debug(
                    f'Agent on host {host} not ready to {action}: {e}')
        return down
