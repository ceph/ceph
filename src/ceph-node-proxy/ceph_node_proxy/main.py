from ceph_node_proxy.redfishdellsystem import RedfishDellSystem
from ceph_node_proxy.api import NodeProxyApi
from ceph_node_proxy.reporter import Reporter
from ceph_node_proxy.util import Config, get_logger, http_req, write_tmp_file, CONFIG
from typing import Dict, Any, Optional

import argparse
import os
import ssl
import json
import time
import signal


class NodeProxyManager:
    def __init__(self, **kw: Any) -> None:
        self.exc: Optional[Exception] = None
        self.log = get_logger(__name__)
        self.mgr_host: str = kw['mgr_host']
        self.cephx_name: str = kw['cephx_name']
        self.cephx_secret: str = kw['cephx_secret']
        self.ca_path: str = kw['ca_path']
        self.api_ssl_crt: str = kw['api_ssl_crt']
        self.api_ssl_key: str = kw['api_ssl_key']
        self.mgr_agent_port: str = str(kw['mgr_agent_port'])
        self.stop: bool = False
        self.ssl_ctx = ssl.create_default_context()
        self.ssl_ctx.check_hostname = True
        self.ssl_ctx.verify_mode = ssl.CERT_REQUIRED
        self.ssl_ctx.load_verify_locations(self.ca_path)
        self.reporter_scheme: str = kw.get('reporter_scheme', 'https')
        self.reporter_endpoint: str = kw.get('reporter_endpoint', '/node-proxy/data')
        self.cephx = {'cephx': {'name': self.cephx_name,
                                'secret': self.cephx_secret}}
        self.config = Config('/etc/ceph/node-proxy.yml', config=CONFIG)

    def run(self) -> None:
        self.init()
        self.loop()

    def init(self) -> None:
        self.init_system()
        self.init_reporter()
        self.init_api()

    def fetch_oob_details(self) -> Dict[str, str]:
        headers, result, status = http_req(hostname=self.mgr_host,
                                           port=self.mgr_agent_port,
                                           data=json.dumps(self.cephx),
                                           endpoint='/node-proxy/oob',
                                           ssl_ctx=self.ssl_ctx)
        if status != 200:
            msg = f'No out of band tool details could be loaded: {status}, {result}'
            self.log.debug(msg)
            raise RuntimeError(msg)

        result_json = json.loads(result)
        oob_details: Dict[str, str] = {
            'host': result_json['result']['addr'],
            'username': result_json['result']['username'],
            'password': result_json['result']['password'],
            'port': result_json['result'].get('port', '443')
        }
        return oob_details

    def init_system(self) -> None:
        oob_details = self.fetch_oob_details()
        self.username: str = oob_details['username']
        self.password: str = oob_details['password']
        try:
            self.system = RedfishDellSystem(host=oob_details['host'],
                                            port=oob_details['port'],
                                            username=oob_details['username'],
                                            password=oob_details['password'],
                                            config=self.config)
            self.system.start()
        except RuntimeError:
            self.log.error("Can't initialize the redfish system.")
            raise

    def init_reporter(self) -> None:
        try:
            self.reporter_agent = Reporter(self.system,
                                           self.cephx,
                                           reporter_scheme=self.reporter_scheme,
                                           reporter_hostname=self.mgr_host,
                                           reporter_port=self.mgr_agent_port,
                                           reporter_endpoint=self.reporter_endpoint)
            self.reporter_agent.start()
        except RuntimeError:
            self.log.error("Can't initialize the reporter.")
            raise

    def init_api(self) -> None:
        try:
            self.log.info('Starting node-proxy API...')
            self.api = NodeProxyApi(self)
            self.api.start()
        except Exception as e:
            self.log.error(f"Can't start node-proxy API: {e}")
            raise

    def loop(self) -> None:
        while not self.stop:
            for thread in [self.system, self.reporter_agent]:
                try:
                    status = thread.check_status()
                    label = 'Ok' if status else 'Critical'
                    self.log.debug(f'{thread} status: {label}')
                except Exception as e:
                    self.log.error(f'{thread} not running: {e.__class__.__name__}: {e}')
                    thread.shutdown()
                    self.init_system()
                    self.init_reporter()
            self.log.debug('All threads are alive, next check in 20sec.')
            time.sleep(20)

    def shutdown(self) -> None:
        self.stop = True
        # if `self.system.shutdown()` is called before self.start(), it will fail.
        if hasattr(self, 'api'):
            self.api.shutdown()
        if hasattr(self, 'reporter_agent'):
            self.reporter_agent.shutdown()
        if hasattr(self, 'system'):
            self.system.shutdown()


def handler(signum: Any, frame: Any, t_mgr: 'NodeProxyManager') -> None:
    t_mgr.system.pending_shutdown = True
    t_mgr.log.info('SIGTERM caught, shutting down threads...')
    t_mgr.shutdown()
    t_mgr.log.info('Logging out from RedFish API')
    t_mgr.system.client.logout()
    raise SystemExit(0)


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Ceph Node-Proxy for HW Monitoring',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--config',
        help='path of config file in json format',
        required=True
    )
    parser.add_argument(
        '--debug',
        help='increase logging verbosity (debug level)',
        action='store_true',
    )

    args = parser.parse_args()
    if args.debug:
        CONFIG['logging']['level'] = 10

    if not os.path.exists(args.config):
        raise Exception(f'No config file found at provided config path: {args.config}')

    with open(args.config, 'r') as f:
        try:
            config_json = f.read()
            config = json.loads(config_json)
        except Exception as e:
            raise Exception(f'Failed to load json config: {str(e)}')

    target_ip = config['target_ip']
    target_port = config['target_port']
    keyring = config['keyring']
    root_cert = config['root_cert.pem']
    listener_cert = config['listener.crt']
    listener_key = config['listener.key']
    name = config['name']

    ca_file = write_tmp_file(root_cert,
                             prefix_name='cephadm-endpoint-root-cert')

    node_proxy_mgr = NodeProxyManager(mgr_host=target_ip,
                                      cephx_name=name,
                                      cephx_secret=keyring,
                                      mgr_agent_port=target_port,
                                      ca_path=ca_file.name,
                                      api_ssl_crt=listener_cert,
                                      api_ssl_key=listener_key)
    signal.signal(signal.SIGTERM,
                  lambda signum, frame: handler(signum, frame, node_proxy_mgr))
    node_proxy_mgr.run()


if __name__ == '__main__':
    main()
