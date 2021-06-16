import cherrypy
import json
import socket
import tempfile
import threading
import time

from mgr_util import verify_tls_files
from ceph.utils import datetime_now
from ceph.deployment.service_spec import ServiceSpec, PlacementSpec

from OpenSSL import crypto
from typing import Any, Dict, Set, Tuple, TYPE_CHECKING
from uuid import uuid4

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator


class CherryPyThread(threading.Thread):
    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr
        self.cherrypy_shutdown_event = threading.Event()
        self.ssl_certs = SSLCerts(self.mgr)
        super(CherryPyThread, self).__init__(target=self.run)

    def run(self) -> None:
        try:
            server_addr = self.mgr.get_mgr_ip()
            server_port = self.mgr.endpoint_port

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

            self.mgr.set_uri('https://{0}:{1}/'.format(server_addr, server_port))

            cherrypy.config.update({
                'server.socket_host': server_addr,
                'server.socket_port': server_port,
                'engine.autoreload.on': False,
                'server.ssl_module': 'builtin',
                'server.ssl_certificate': cert_fname,
                'server.ssl_private_key': key_fname,
            })
            root_conf = {'/': {'request.dispatch': cherrypy.dispatch.MethodDispatcher(),
                               'tools.response_headers.on': True}}
            cherrypy.tree.mount(Root(self.mgr), '/', root_conf)
            self.mgr.log.info('Starting cherrypy engine...')
            cherrypy.engine.start()
            self.mgr.log.info('Cherrypy engine started.')
            # wait for the shutdown event
            self.cherrypy_shutdown_event.wait()
            self.cherrypy_shutdown_event.clear()
            cherrypy.engine.stop()
            self.mgr.log.info('Cherrypy engine stopped.')
        except Exception as e:
            self.mgr.log.error(f'Failed to run cephadm cherrypy endpoint: {e}')

    def shutdown(self) -> None:
        self.mgr.log.info('Stopping cherrypy engine...')
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
        metadata_types = ['ls', 'networks', 'facts']
        metadata_types_str = '{' + ', '.join(metadata_types) + '}'
        if not all(item in data.keys() for item in metadata_types):
            self.mgr.log.warning(
                f'Agent on host {host} reported incomplete metadata. Not all of {metadata_types_str} were present. Received fields {fields}')

    def handle_metadata(self, data: Dict[str, Any]) -> None:
        try:
            host = data['host']
            if host not in self.mgr.cache.agent_counter:
                self.mgr.log.debug(
                    f'Got metadata from agent on host {host} with no known counter entry. Starting counter at 1 and requesting new metadata')
                self.mgr.cache.agent_counter[host] = 1
                self.mgr.agent_helpers._request_agent_acks({host})
                return

            self.mgr.cache.agent_ports[host] = int(data['port'])
            self.mgr.cache.agent_timestamp[host] = datetime_now()
            up_to_date = False

            int_ack = int(data['ack'])
            if int_ack == self.mgr.cache.agent_counter[host]:
                up_to_date = True
            else:
                # we got old counter value with message, inform agent of new timestamp
                self.mgr.agent_helpers._request_agent_acks({host})
                self.mgr.log.debug(
                    f'Received old metadata from agent on host {host}. Requested up-to-date metadata.')

            if up_to_date:
                if 'ls' in data:
                    self.mgr._process_ls_output(host, data['ls'])
                if 'networks' in data:
                    self.mgr.cache.update_host_networks(host, data['networks'])
                if 'facts' in data:
                    self.mgr.cache.update_host_facts(host, json.loads(data['facts']))

                # update timestamp of most recent up-to-date agent update
                self.mgr.cache.metadata_up_to_date[host] = True
                self.mgr.log.debug(
                    f'Received up-to-date metadata from agent on host {host}.')

        except Exception as e:
            self.mgr.log.warning(
                f'Failed to update metadata with metadata from agent on host {host}: {e}')


class AgentMessageThread(threading.Thread):
    def __init__(self, host: str, port: int, counter: int, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr
        self.host = host
        self.port = port
        self.counter = counter
        super(AgentMessageThread, self).__init__(target=self.run)

    def run(self) -> None:
        for retry_wait in [3, 5]:
            try:
                agent_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                agent_socket.connect((self.mgr.inventory.get_addr(self.host), self.port))
                agent_socket.send(str(self.counter).encode())
                agent_response = agent_socket.recv(1024).decode()
                self.mgr.log.debug(f'Received {agent_response} from agent on host {self.host}')
                return
            except ConnectionError as e:
                # if it's a connection error, possibly try to connect again.
                # We could have just deployed agent and it might not be ready
                self.mgr.log.debug(
                    f'Retrying connection to agent on {self.host} in {str(retry_wait)} seconds. Connection failed with: {e}')
                time.sleep(retry_wait)
            except Exception as e:
                # if it's not a connection error, something has gone wrong. Give up.
                self.mgr.log.debug(f'Failed to contact agent on host {self.host}: {e}')
                return
        return


class CephadmAgentHelpers:
    def __init__(self, mgr: "CephadmOrchestrator"):
        self.mgr: "CephadmOrchestrator" = mgr

    def _request_agent_acks(self, hosts: Set[str]) -> None:
        for host in hosts:
            self.mgr.cache.metadata_up_to_date[host] = False
            if host not in self.mgr.cache.agent_counter:
                self.mgr.cache.agent_counter[host] = 1
            else:
                self.mgr.cache.agent_counter[host] = self.mgr.cache.agent_counter[host] + 1
            message_thread = AgentMessageThread(
                host, self.mgr.cache.agent_ports[host], self.mgr.cache.agent_counter[host], self.mgr)
            message_thread.start()

    def _agent_down(self, host: str) -> bool:
        # if we don't have a timestamp, it's likely because of a mgr fail over.
        # just set the timestamp to now
        if host not in self.mgr.cache.agent_timestamp:
            self.mgr.cache.agent_timestamp[host] = datetime_now()
        # agent hasn't reported in 2.5 * it's refresh rate. Something is likely wrong with it.
        time_diff = datetime_now() - self.mgr.cache.agent_timestamp[host]
        if time_diff.total_seconds() > 2.5 * float(self.mgr.agent_refresh_rate):
            return True
        return False

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


class SSLCerts:
    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr
        self.root_cert: Any
        self.root_key: Any
        self.root_subj: Any

    def generate_root_cert(self) -> Tuple[str, str]:
        self.root_key = crypto.PKey()
        self.root_key.generate_key(crypto.TYPE_RSA, 2048)

        self.root_cert = crypto.X509()
        self.root_cert.set_serial_number(int(uuid4()))

        self.root_subj = self.root_cert.get_subject()
        self.root_subj.commonName = "cephadm-root"

        self.root_cert.set_issuer(self.root_subj)
        self.root_cert.set_pubkey(self.root_key)
        self.root_cert.gmtime_adj_notBefore(0)
        self.root_cert.gmtime_adj_notAfter(10 * 365 * 24 * 60 * 60)  # 10 years
        self.root_cert.sign(self.root_key, 'sha256')

        cert_str = crypto.dump_certificate(crypto.FILETYPE_PEM, self.root_cert).decode('utf-8')
        key_str = crypto.dump_privatekey(crypto.FILETYPE_PEM, self.root_key).decode('utf-8')
        return (cert_str, key_str)

    def generate_cert(self) -> Tuple[str, str]:
        key = crypto.PKey()
        key.generate_key(crypto.TYPE_RSA, 2048)

        cert = crypto.X509()
        cert.set_serial_number(int(uuid4()))

        subj = cert.get_subject()
        subj.commonName = str(self.mgr.get_mgr_ip())

        cert.set_issuer(self.root_subj)
        cert.set_pubkey(key)
        cert.gmtime_adj_notBefore(0)
        cert.gmtime_adj_notAfter(10 * 365 * 24 * 60 * 60)  # 10 years
        cert.sign(self.root_key, 'sha256')

        cert_str = crypto.dump_certificate(crypto.FILETYPE_PEM, cert).decode('utf-8')
        key_str = crypto.dump_privatekey(crypto.FILETYPE_PEM, key).decode('utf-8')
        return (cert_str, key_str)

    def get_root_cert(self) -> str:
        return crypto.dump_certificate(crypto.FILETYPE_PEM, self.root_cert).decode('utf-8')

    def get_root_key(self) -> str:
        return crypto.dump_privatekey(crypto.FILETYPE_PEM, self.root_key).decode('utf-8')
