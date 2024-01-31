from threading import Thread
from ceph_node_proxy.redfishdellsystem import RedfishDellSystem
from ceph_node_proxy.api import NodeProxyApi
from ceph_node_proxy.reporter import Reporter
from ceph_node_proxy.util import Config, Logger, http_req, write_tmp_file
from typing import Dict, Any, Optional

import argparse
import traceback
import logging
import os
import ssl
import json
import time

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = {
    'reporter': {
        'check_interval': 5,
        'push_data_max_retries': 30,
        'endpoint': 'https://127.0.0.1:7150/node-proxy/data',
    },
    'system': {
        'refresh_interval': 5
    },
    'server': {
        'port': 8080,
    },
    'logging': {
        'level': 20,
    }
}


class NodeProxyManager(Thread):
    def __init__(self,
                 mgr_host: str,
                 cephx_name: str,
                 cephx_secret: str,
                 ca_path: str,
                 api_ssl_crt: str,
                 api_ssl_key: str,
                 mgr_agent_port: int = 7150):
        super().__init__()
        self.mgr_host = mgr_host
        self.cephx_name = cephx_name
        self.cephx_secret = cephx_secret
        self.ca_path = ca_path
        self.api_ssl_crt = api_ssl_crt
        self.api_ssl_key = api_ssl_key
        self.mgr_agent_port = str(mgr_agent_port)
        self.stop = False
        self.ssl_ctx = ssl.create_default_context()
        self.ssl_ctx.check_hostname = True
        self.ssl_ctx.verify_mode = ssl.CERT_REQUIRED
        self.ssl_ctx.load_verify_locations(self.ca_path)

    def run(self) -> None:
        self.init()
        self.loop()

    def init(self) -> None:
        node_proxy_meta = {
            'cephx': {
                'name': self.cephx_name,
                'secret': self.cephx_secret
            }
        }
        headers, result, status = http_req(hostname=self.mgr_host,
                                           port=self.mgr_agent_port,
                                           data=json.dumps(node_proxy_meta),
                                           endpoint='/node-proxy/oob',
                                           ssl_ctx=self.ssl_ctx)
        if status != 200:
            msg = f'No out of band tool details could be loaded: {status}, {result}'
            logger.debug(msg)
            raise RuntimeError(msg)

        result_json = json.loads(result)
        kwargs = {
            'host': result_json['result']['addr'],
            'username': result_json['result']['username'],
            'password': result_json['result']['password'],
            'cephx': node_proxy_meta['cephx'],
            'mgr_host': self.mgr_host,
            'mgr_agent_port': self.mgr_agent_port,
            'api_ssl_crt': self.api_ssl_crt,
            'api_ssl_key': self.api_ssl_key
        }
        if result_json['result'].get('port'):
            kwargs['port'] = result_json['result']['port']

        self.node_proxy: NodeProxy = NodeProxy(**kwargs)
        self.node_proxy.start()

    def loop(self) -> None:
        while not self.stop:
            try:
                status = self.node_proxy.check_status()
                label = 'Ok' if status else 'Critical'
                logger.debug(f'node-proxy status: {label}')
            except Exception as e:
                logger.error(f'node-proxy not running: {e.__class__.__name__}: {e}')
                time.sleep(120)
                self.init()
            else:
                logger.debug('node-proxy alive, next check in 60sec.')
                time.sleep(60)

    def shutdown(self) -> None:
        self.stop = True
        # if `self.node_proxy.shutdown()` is called before self.start(), it will fail.
        if self.__dict__.get('node_proxy'):
            self.node_proxy.shutdown()


class NodeProxy(Thread):
    def __init__(self, **kw: Any) -> None:
        super().__init__()
        self.username: str = kw.get('username', '')
        self.password: str = kw.get('password', '')
        self.host: str = kw.get('host', '')
        self.port: int = kw.get('port', 443)
        self.cephx: Dict[str, Any] = kw.get('cephx', {})
        self.reporter_scheme: str = kw.get('reporter_scheme', 'https')
        self.mgr_host: str = kw.get('mgr_host', '')
        self.mgr_agent_port: str = kw.get('mgr_agent_port', '')
        self.reporter_endpoint: str = kw.get('reporter_endpoint', '/node-proxy/data')
        self.api_ssl_crt: str = kw.get('api_ssl_crt', '')
        self.api_ssl_key: str = kw.get('api_ssl_key', '')
        self.exc: Optional[Exception] = None
        self.log = Logger(__name__)

    def run(self) -> None:
        try:
            self.main()
        except Exception as e:
            self.exc = e
            return

    def shutdown(self) -> None:
        self.log.logger.info('Shutting down node-proxy...')
        self.system.client.logout()
        self.system.stop_update_loop()
        self.reporter_agent.stop()

    def check_status(self) -> bool:
        if self.__dict__.get('system') and not self.system.run:
            raise RuntimeError('node-proxy encountered an error.')
        if self.exc:
            traceback.print_tb(self.exc.__traceback__)
            self.log.logger.error(f'{self.exc.__class__.__name__}: {self.exc}')
            raise self.exc
        return True

    def main(self) -> None:
        # TODO: add a check and fail if host/username/password/data aren't passed
        self.config = Config('/etc/ceph/node-proxy.yml', default_config=DEFAULT_CONFIG)
        self.log = Logger(__name__, level=self.config.__dict__['logging']['level'])

        # create the redfish system and the obsever
        self.log.logger.info('Server initialization...')
        try:
            self.system = RedfishDellSystem(host=self.host,
                                            port=self.port,
                                            username=self.username,
                                            password=self.password,
                                            config=self.config)
        except RuntimeError:
            self.log.logger.error("Can't initialize the redfish system.")
            raise

        try:
            self.reporter_agent = Reporter(self.system,
                                           self.cephx,
                                           reporter_scheme=self.reporter_scheme,
                                           reporter_hostname=self.mgr_host,
                                           reporter_port=self.mgr_agent_port,
                                           reporter_endpoint=self.reporter_endpoint)
            self.reporter_agent.run()
        except RuntimeError:
            self.log.logger.error("Can't initialize the reporter.")
            raise

        try:
            self.log.logger.info('Starting node-proxy API...')
            self.api = NodeProxyApi(self,
                                    username=self.username,
                                    password=self.password,
                                    ssl_crt=self.api_ssl_crt,
                                    ssl_key=self.api_ssl_key)
            self.api.start()
        except Exception as e:
            self.log.logger.error(f"Can't start node-proxy API: {e}")
            raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Ceph Node-Proxy for HW Monitoring',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        '--config',
        help='path of config file in json format',
        required=True
    )

    args = parser.parse_args()

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

    f = write_tmp_file(root_cert,
                       prefix_name='cephadm-endpoint-root-cert')

    node_proxy_mgr = NodeProxyManager(mgr_host=target_ip,
                                      cephx_name=name,
                                      cephx_secret=keyring,
                                      mgr_agent_port=target_port,
                                      ca_path=f.name,
                                      api_ssl_crt=listener_cert,
                                      api_ssl_key=listener_key)
    if not node_proxy_mgr.is_alive():
        node_proxy_mgr.start()


if __name__ == '__main__':
    main()
