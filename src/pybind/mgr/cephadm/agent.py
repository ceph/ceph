import cherrypy
import ipaddress
import json
import logging
import socket
import ssl
import tempfile
import threading
import time

# from orchestrator import OrchestratorError
from mgr_util import verify_tls_files
from orchestrator import DaemonDescriptionStatus
from ceph.utils import datetime_now
from ceph.deployment.inventory import Devices
from ceph.deployment.service_spec import ServiceSpec, PlacementSpec
from cephadm.services.cephadmservice import CephadmDaemonDeploySpec

from datetime import datetime, timedelta
from OpenSSL import crypto
from contextlib import contextmanager
from cryptography import x509
from cryptography.x509.oid import NameOID
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.backends import default_backend

from typing import Any, Dict, Iterator, List, Set, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


def cherrypy_filter(record: logging.LogRecord) -> int:
    blocked = [
        'TLSV1_ALERT_DECRYPT_ERROR'
    ]
    msg = record.getMessage()
    return not any([m for m in blocked if m in msg])


logging.getLogger('cherrypy.access').addFilter(cherrypy_filter)
logging.getLogger('cherrypy.error').addFilter(cherrypy_filter)


class CherryPyThread(threading.Thread):
    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr
        self.cherrypy_shutdown_event = threading.Event()
        self.ssl_certs = SSLCerts(self.mgr)
        self.server_port = 7150
        self.server_addr = self.mgr.get_mgr_ip()
        super(CherryPyThread, self).__init__(target=self.run)

    def run(self) -> None:
        try:
            self.ssl_certs.generate_root_cert()
            cert, key = self.ssl_certs.generate_cert()

            self.key_tmp = tempfile.NamedTemporaryFile()
            self.key_tmp.write(key.encode('utf-8'))
            self.key_tmp.flush()  # pkey_tmp must not be gc'ed
            key_fname = self.key_tmp.name

            self.cert_tmp = tempfile.NamedTemporaryFile()
            self.cert_tmp.write(cert.encode('utf-8'))
            self.cert_tmp.flush()  # cert_tmp must not be gc'ed
            cert_fname = self.cert_tmp.name

            verify_tls_files(cert_fname, key_fname)

            cherrypy.config.update({
                'server.socket_host': self.server_addr,
                'server.socket_port': self.server_port,
                'engine.autoreload.on': False,
                'server.ssl_module': 'builtin',
                'server.ssl_certificate': cert_fname,
                'server.ssl_private_key': key_fname,
            })
            root_conf = {'/': {'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
                               'tools.response_headers.on': True}}
            cherrypy.tree.mount(Root(self.mgr), '/', root_conf)
            self.mgr.log.debug('Starting cherrypy engine...')
            self.start_engine()
            self.mgr.log.debug('Cherrypy engine started.')
            agents_down = []
            for h in self.mgr.cache.get_hosts():
                if self.mgr.agent_helpers._check_agent(h):
                    agents_down.append(h)
            self.mgr.agent_helpers._update_agent_down_healthcheck(agents_down)
            # wait for the shutdown event
            self.cherrypy_shutdown_event.wait()
            self.cherrypy_shutdown_event.clear()
            cherrypy.engine.stop()
            self.mgr.log.debug('Cherrypy engine stopped.')
        except Exception as e:
            self.mgr.log.error(f'Failed to run cephadm cherrypy endpoint: {e}')

    def start_engine(self) -> None:
        port_connect_attempts = 0
        while port_connect_attempts < 150:
            try:
                cherrypy.engine.start()
                self.mgr.log.debug(f'Cephadm endpoint connected to port {self.server_port}')
                return
            except cherrypy.process.wspbus.ChannelFailures as e:
                self.mgr.log.debug(
                    f'{e}. Trying next port.')
                self.server_port += 1
                cherrypy.server.httpserver = None
                cherrypy.config.update({
                    'server.socket_port': self.server_port
                })
                port_connect_attempts += 1
        self.mgr.log.error(
            'Cephadm Endpoint could not find free port in range 7150-7300 and failed to start')

    def shutdown(self) -> None:
        self.mgr.log.debug('Stopping cherrypy engine...')
        self.cherrypy_shutdown_event.set()


class Root:
    exposed = True

    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr = mgr
        self.data = HostData(self.mgr)

    def GET(self) -> str:
        return '''<!DOCTYPE html>
<html>
<head><title>Cephadm HTTP Endpoint</title></head>
<body>
<p>Cephadm HTTP Endpoint is up and running</p>
</body>
</html>'''


class HostData:
    exposed = True

    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr = mgr

    @cherrypy.tools.json_in()
    def POST(self) -> None:
        data: Dict[str, Any] = cherrypy.request.json
        try:
            self.check_request_fields(data)
        except Exception as e:
            self.mgr.log.warning(f'Received bad metadata from an agent: {e}')
        else:
            # if we got here, we've already verified the keyring of the agent. If
            # host agent is reporting on is marked offline, it shouldn't be any more
            self.mgr.offline_hosts_remove(data['host'])
            self.handle_metadata(data)

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
        if host not in self.mgr.cache.agent_keys:
            raise Exception(f'No agent keyring stored for host {host}. Cannot verify agent')
        if data['keyring'] != self.mgr.cache.agent_keys[host]:
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

    def handle_metadata(self, data: Dict[str, Any]) -> None:
        try:
            host = data['host']
            self.mgr.cache.agent_ports[host] = int(data['port'])
            if host not in self.mgr.cache.agent_counter:
                self.mgr.log.debug(
                    f'Got metadata from agent on host {host} with no known counter entry. Starting counter at 1 and requesting new metadata')
                self.mgr.cache.agent_counter[host] = 1
                self.mgr.agent_helpers._request_agent_acks({host})
                return

            # update timestamp of most recent agent update
            self.mgr.cache.agent_timestamp[host] = datetime_now()

            error_daemons_old = set([dd.name() for dd in self.mgr.cache.get_error_daemons()])
            daemon_count_old = len(self.mgr.cache.get_daemons_by_host(host))

            agents_down = []
            for h in self.mgr.cache.get_hosts():
                if self.mgr.agent_helpers._check_agent(h):
                    agents_down.append(h)
            self.mgr.agent_helpers._update_agent_down_healthcheck(agents_down)

            up_to_date = False

            int_ack = int(data['ack'])
            if int_ack == self.mgr.cache.agent_counter[host]:
                up_to_date = True
            else:
                # we got old counter value with message, inform agent of new timestamp
                if not self.mgr.cache.messaging_agent(host):
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

        except Exception as e:
            self.mgr.log.warning(
                f'Failed to update metadata with metadata from agent on host {host}: {e}')


class AgentMessageThread(threading.Thread):
    def __init__(self, host: str, port: int, data: Dict[Any, Any], mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr
        self.host = host
        self.addr = self.mgr.inventory.get_addr(host) if host in self.mgr.inventory else host
        self.port = port
        self.data: str = json.dumps(data)
        super(AgentMessageThread, self).__init__(target=self.run)

    def run(self) -> None:
        self.mgr.log.debug(f'Sending message to agent on host {self.host}')
        self.mgr.cache.sending_agent_message[self.host] = True
        try:
            assert self.mgr.cherrypy_thread
            root_cert = self.mgr.cherrypy_thread.ssl_certs.get_root_cert()
            root_cert_tmp = tempfile.NamedTemporaryFile()
            root_cert_tmp.write(root_cert.encode('utf-8'))
            root_cert_tmp.flush()
            root_cert_fname = root_cert_tmp.name

            cert, key = self.mgr.cherrypy_thread.ssl_certs.generate_cert()

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
            self.mgr.cache.sending_agent_message[self.host] = False
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
            self.mgr.cache.sending_agent_message[self.host] = False
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
                self.mgr.cache.sending_agent_message[self.host] = False
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
                self.mgr.cache.sending_agent_message[self.host] = False
                return
        self.mgr.log.error(f'Could not connect to agent on host {self.host}')
        self.mgr.cache.sending_agent_message[self.host] = False
        return


class AgentLockException(Exception):
    pass


class CephadmAgentHelpers:
    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr: "CephadmOrchestrator" = mgr
        self.agent_locks: Dict[str, threading.Lock] = {}

    def _request_agent_acks(self, hosts: Set[str], increment: bool = False) -> None:
        for host in hosts:
            if increment:
                self.mgr.cache.metadata_up_to_date[host] = False
            if host not in self.mgr.cache.agent_counter:
                self.mgr.cache.agent_counter[host] = 1
            elif increment:
                self.mgr.cache.agent_counter[host] = self.mgr.cache.agent_counter[host] + 1
            message_thread = AgentMessageThread(
                host, self.mgr.cache.agent_ports[host], {'counter': self.mgr.cache.agent_counter[host]}, self.mgr)
            message_thread.start()

    def _agent_down(self, host: str) -> bool:
        # if host is draining or drained (has _no_schedule label) there should not
        # be an agent deployed there and therefore we should return False
        if host not in [h.hostname for h in self.mgr.cache.get_non_draining_hosts()]:
            return False
        # if we haven't deployed an agent on the host yet, don't say an agent is down
        if not self.mgr.cache.get_daemons_by_type('agent', host=host):
            return False
        # if we don't have a timestamp, it's likely because of a mgr fail over.
        # just set the timestamp to now. However, if host was offline before, we
        # should not allow creating a new timestamp to cause it to be marked online
        if host not in self.mgr.cache.agent_timestamp:
            self.mgr.cache.agent_timestamp[host] = datetime_now()
            if host in self.mgr.offline_hosts:
                return False
        # agent hasn't reported in down multiplier * it's refresh rate. Something is likely wrong with it.
        down_mult: float = max(self.mgr.agent_down_multiplier, 1.5)
        time_diff = datetime_now() - self.mgr.cache.agent_timestamp[host]
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
                if agent.hostname not in self.mgr.cache.agent_keys:
                    self.mgr._schedule_daemon_action(agent.name(), 'redeploy')
            if 'agent' not in self.mgr.spec_store:
                self.mgr.agent_helpers._apply_agent()
                need_apply = True
        else:
            if 'agent' in self.mgr.spec_store:
                self.mgr.spec_store.rm('agent')
                need_apply = True
            self.mgr.cache.agent_counter = {}
            self.mgr.cache.agent_timestamp = {}
            self.mgr.cache.agent_keys = {}
            self.mgr.cache.agent_ports = {}
        return need_apply

    @contextmanager
    def agent_lock(self, host: str) -> Iterator[threading.Lock]:
        if host not in self.mgr.agent_helpers.agent_locks:
            self.mgr.agent_helpers.agent_locks[host] = threading.Lock()
        lock = self.mgr.agent_helpers.agent_locks[host]
        if not lock.acquire(False):
            raise AgentLockException()
        try:
            yield lock
        finally:
            lock.release()

    def _check_agent(self, host: str) -> bool:
        try:
            assert self.mgr.cherrypy_thread
            assert self.mgr.cherrypy_thread.ssl_certs.get_root_cert()
        except Exception:
            self.mgr.log.debug(
                f'Delaying checking agent on {host} until cephadm endpoint finished creating root cert')
            return False
        if self.mgr.agent_helpers._agent_down(host):
            if host not in self.mgr.offline_hosts:
                self.mgr.cache.metadata_up_to_date[host] = False
                # In case host is actually offline, it's best to reset the connection to avoid
                # a long timeout trying to use an existing connection to an offline host
                self.mgr.ssh._reset_con(host)

                try:
                    # try to schedule redeploy of agent in case it is individually down
                    agent = self.mgr.cache.get_daemons_by_type('agent', host=host)[0]
                    with self.mgr.agent_helpers.agent_lock(host):
                        daemon_spec = CephadmDaemonDeploySpec.from_daemon_description(agent)
                        self.mgr._daemon_action(daemon_spec, action='redeploy')
                except AgentLockException:
                    self.mgr.log.debug(
                        f'Could not redeploy agent on host {host}. Someone else holds agent\'s lock')
                except Exception as e:
                    self.mgr.log.debug(
                        f'Failed to redeploy agent on host {host}. Agent possibly never deployed: {e}')
            return True
        else:
            try:
                agent = self.mgr.cache.get_daemons_by_type('agent', host=host)[0]
                assert agent.daemon_id is not None
                assert agent.hostname is not None
            except Exception as e:
                self.mgr.log.debug(
                    f'Could not retrieve agent on host {host} from daemon cache: {e}')
                return False
            try:
                spec = self.mgr.spec_store.active_specs.get('agent', None)
                deps = self.mgr._calc_daemon_deps(spec, 'agent', agent.daemon_id)
                last_deps, last_config = self.mgr.cache.get_daemon_last_config_deps(
                    host, agent.name())
                if not last_config or last_deps != deps:
                    daemon_spec = CephadmDaemonDeploySpec.from_daemon_description(agent)
                    with self.mgr.agent_helpers.agent_lock(host):
                        self.mgr._daemon_action(daemon_spec, action='reconfig')
                    return False
            except AgentLockException:
                self.mgr.log.debug(
                    f'Could not reconfig agent on host {host}. Someone else holds agent\'s lock')
            except Exception as e:
                self.mgr.log.debug(
                    f'Agent on host {host} not ready to have config and deps checked: {e}')
            action = self.mgr.cache.get_scheduled_daemon_action(agent.hostname, agent.name())
            if action:
                try:
                    with self.mgr.agent_helpers.agent_lock(host):
                        daemon_spec = CephadmDaemonDeploySpec.from_daemon_description(agent)
                        self.mgr._daemon_action(daemon_spec, action=action)
                        self.mgr.cache.rm_scheduled_daemon_action(agent.hostname, agent.name())
                except AgentLockException:
                    self.mgr.log.debug(
                        f'Could not {action} agent on host {host}. Someone else holds agent\'s lock')
                except Exception as e:
                    self.mgr.log.debug(
                        f'Agent on host {host} not ready to {action}: {e}')
            return False


class SSLCerts:
    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr
        self.root_cert: Any
        self.root_key: Any
        self.root_subj: Any

    def generate_root_cert(self) -> Tuple[str, str]:
        self.root_key = rsa.generate_private_key(
            public_exponent=65537, key_size=4096, backend=default_backend())
        root_public_key = self.root_key.public_key()

        root_builder = x509.CertificateBuilder()

        root_builder = root_builder.subject_name(x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, u'cephadm-root'),
        ]))

        root_builder = root_builder.issuer_name(x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, u'cephadm-root'),
        ]))

        root_builder = root_builder.not_valid_before(datetime.now())
        root_builder = root_builder.not_valid_after(datetime.now() + timedelta(days=(365 * 10 + 3)))
        root_builder = root_builder.serial_number(x509.random_serial_number())
        root_builder = root_builder.public_key(root_public_key)
        root_builder = root_builder.add_extension(
            x509.SubjectAlternativeName(
                [x509.IPAddress(ipaddress.IPv4Address(str(self.mgr.get_mgr_ip())))]
            ),
            critical=False
        )
        root_builder = root_builder.add_extension(
            x509.BasicConstraints(ca=True, path_length=None), critical=True,
        )

        self.root_cert = root_builder.sign(
            private_key=self.root_key, algorithm=hashes.SHA256(), backend=default_backend()
        )

        cert_str = crypto.dump_certificate(crypto.FILETYPE_PEM, self.root_cert).decode('utf-8')
        key_str = self.root_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        ).decode('utf-8')

        return (cert_str, key_str)

    def generate_cert(self, addr: str = '') -> Tuple[str, str]:
        have_ip = True
        if addr:
            try:
                ip = x509.IPAddress(ipaddress.IPv4Address(addr))
            except Exception:
                try:
                    ip = x509.IPAddress(ipaddress.IPv6Address(addr))
                except Exception:
                    have_ip = False
                    pass
        else:
            ip = x509.IPAddress(ipaddress.IPv4Address(self.mgr.get_mgr_ip()))

        private_key = rsa.generate_private_key(
            public_exponent=65537, key_size=4096, backend=default_backend())
        public_key = private_key.public_key()

        builder = x509.CertificateBuilder()

        builder = builder.subject_name(x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, addr if addr else str(self.mgr.get_mgr_ip())),
        ]))

        builder = builder.issuer_name(x509.Name([
            x509.NameAttribute(NameOID.COMMON_NAME, u'cephadm-root'),
        ]))

        builder = builder.not_valid_before(datetime.now())
        builder = builder.not_valid_after(datetime.now() + timedelta(days=(365 * 10 + 3)))
        builder = builder.serial_number(x509.random_serial_number())
        builder = builder.public_key(public_key)
        if have_ip:
            builder = builder.add_extension(
                x509.SubjectAlternativeName(
                    [ip]
                ),
                critical=False
            )
        builder = builder.add_extension(
            x509.BasicConstraints(ca=False, path_length=None), critical=True,
        )

        cert = builder.sign(
            private_key=self.root_key, algorithm=hashes.SHA256(), backend=default_backend()
        )

        cert_str = crypto.dump_certificate(crypto.FILETYPE_PEM, cert).decode('utf-8')
        key_str = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption()
        ).decode('utf-8')

        return (cert_str, key_str)

    def get_root_cert(self) -> str:
        try:
            return crypto.dump_certificate(crypto.FILETYPE_PEM, self.root_cert).decode('utf-8')
        except AttributeError:
            return ''

    def get_root_key(self) -> str:
        try:
            return crypto.dump_certificate(crypto.FILETYPE_PEM, self.root_key).decode('utf-8')
        except AttributeError:
            return ''
