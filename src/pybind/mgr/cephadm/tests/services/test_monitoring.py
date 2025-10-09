from typing import Dict, List
from textwrap import dedent
import json
import urllib.parse
import yaml
import pytest
from unittest.mock import Mock, patch, ANY

from cephadm.serve import CephadmServe
from cephadm.services.service_registry import service_registry
from cephadm.services.cephadmservice import CephadmDaemonDeploySpec
from cephadm.services.monitoring import AlertmanagerService, PrometheusService
from cephadm.services.smb import SMBSpec
from cephadm.module import CephadmOrchestrator
from ceph.deployment.service_spec import (
    AlertManagerSpec,
    CephExporterSpec,
    GrafanaSpec,
    IngressSpec,
    MonitoringSpec,
    NvmeofServiceSpec,
    PlacementSpec,
    PrometheusSpec,
    RGWSpec,
    ServiceSpec,
    MgmtGatewaySpec,
    OAuth2ProxySpec
)
from cephadm.tests.fixtures import with_host, with_service, _run_cephadm, async_side_effect
from cephadm.tlsobject_types import TLSCredentials


cephadm_root_ca = """-----BEGIN CERTIFICATE-----\nMIIE7DCCAtSgAwIBAgIUE8b2zZ64geu2ns3Zfn3/4L+Cf6MwDQYJKoZIhvcNAQEL\nBQAwFzEVMBMGA1UEAwwMY2VwaGFkbS1yb290MB4XDTI0MDYyNjE0NDA1M1oXDTM0\nMDYyNzE0NDA1M1owFzEVMBMGA1UEAwwMY2VwaGFkbS1yb290MIICIjANBgkqhkiG\n9w0BAQEFAAOCAg8AMIICCgKCAgEAsZRJsdtTr9GLG1lWFql5SGc46ldFanNJd1Gl\nqXq5vgZVKRDTmNgAb/XFuNEEmbDAXYIRZolZeYKMHfn0pouPRSel0OsC6/02ZUOW\nIuN89Wgo3IYleCFpkVIumD8URP3hwdu85plRxYZTtlruBaTRH38lssyCqxaOdEt7\nAUhvYhcMPJThB17eOSQ73mb8JEC83vB47fosI7IhZuvXvRSuZwUW30rJanWNhyZq\neS2B8qw2RSO0+77H6gA4ftBnitfsE1Y8/F9Z/f92JOZuSMQXUB07msznPbRJia3f\nueO8gOc32vxd1A1/Qzp14uX34yEGY9ko2lW226cZO29IVUtXOX+LueQttwtdlpz8\ne6Npm09pXhXAHxV/OW3M28MdXmobIqT/m9MfkeAErt5guUeC5y8doz6/3VQRjFEn\nRpN0WkblgnNAQ3DONPc+Qd9Fi/wZV2X7bXoYpNdoWDsEOiE/eLmhG1A2GqU/mneP\nzQ6u79nbdwTYpwqHpa+PvusXeLfKauzI8lLUJotdXy9EK8iHUofibB61OljYye6B\nG3b8C4QfGsw8cDb4APZd/6AZYyMx/V3cGZ+GcOV7WvsC8k7yx5Uqasm/kiGQ3EZo\nuNenNEYoGYrjb8D/8QzqNUTwlEh27/ps80tO7l2GGTvWVZL0PRZbmLDvO77amtOf\nOiRXMoUCAwEAAaMwMC4wGwYDVR0RBBQwEocQAAAAAAAAAAAAAAAAAAAAATAPBgNV\nHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4ICAQAxwzX5AhYEWhTV4VUwUj5+\nqPdl4Q2tIxRokqyE+cDxoSd+6JfGUefUbNyBxDt0HaBq8obDqqrbcytxnn7mpnDu\nhtiauY+I4Amt7hqFOiFA4cCLi2mfok6g2vL53tvhd9IrsfflAU2wy7hL76Ejm5El\nA+nXlkJwps01Whl9pBkUvIbOn3pXX50LT4hb5zN0PSu957rjd2xb4HdfuySm6nW4\n4GxtVWfmGA6zbC4XMEwvkuhZ7kD2qjkAguGDF01uMglkrkCJT3OROlNBuSTSBGqt\ntntp5VytHvb7KTF7GttM3ha8/EU2KYaHM6WImQQTrOfiImAktOk4B3lzUZX3HYIx\n+sByO4P4dCvAoGz1nlWYB2AvCOGbKf0Tgrh4t4jkiF8FHTXGdfvWmjgi1pddCNAy\nn65WOCmVmLZPERAHOk1oBwqyReSvgoCFo8FxbZcNxJdlhM0Z6hzKggm3O3Dl88Xl\n5euqJjh2STkBW8Xuowkg1TOs5XyWvKoDFAUzyzeLOL8YSG+gXV22gPTUaPSVAqdb\nwd0Fx2kjConuC5bgTzQHs8XWA930U3XWZraj21Vaa8UxlBLH4fUro8H5lMSYlZNE\nJHRNW8BkznAClaFSDG3dybLsrzrBFAu/Qb5zVkT1xyq0YkepGB7leXwq6vjWA5Pw\nmZbKSphWfh0qipoqxqhfkw==\n-----END CERTIFICATE-----\n"""

ceph_generated_cert = """-----BEGIN CERTIFICATE-----\nMIICxjCCAa4CEQDIZSujNBlKaLJzmvntjukjMA0GCSqGSIb3DQEBDQUAMCExDTAL\nBgNVBAoMBENlcGgxEDAOBgNVBAMMB2NlcGhhZG0wHhcNMjIwNzEzMTE0NzA3WhcN\nMzIwNzEwMTE0NzA3WjAhMQ0wCwYDVQQKDARDZXBoMRAwDgYDVQQDDAdjZXBoYWRt\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyyMe4DMA+MeYK7BHZMHB\nq7zjliEOcNgxomjU8qbf5USF7Mqrf6+/87XWqj4pCyAW8x0WXEr6A56a+cmBVmt+\nqtWDzl020aoId6lL5EgLLn6/kMDCCJLq++Lg9cEofMSvcZh+lY2f+1p+C+00xent\nrLXvXGOilAZWaQfojT2BpRnNWWIFbpFwlcKrlg2G0cFjV5c1m6a0wpsQ9JHOieq0\nSvwCixajwq3CwAYuuiU1wjI4oJO4Io1+g8yB3nH2Mo/25SApCxMXuXh4kHLQr/T4\n4hqisvG4uJYgKMcSIrWj5o25mclByGi1UI/kZkCUES94i7Z/3ihx4Bad0AMs/9tw\nFwIDAQABMA0GCSqGSIb3DQEBDQUAA4IBAQAf+pwz7Gd7mDwU2LY0TQXsK6/8KGzh\nHuX+ErOb8h5cOAbvCnHjyJFWf6gCITG98k9nxU9NToG0WYuNm/max1y/54f0dtxZ\npUo6KSNl3w6iYCfGOeUIj8isi06xMmeTgMNzv8DYhDt+P2igN6LenqWTVztogkiV\nxQ5ZJFFLEw4sN0CXnrZX3t5ruakxLXLTLKeE0I91YJvjClSBGkVJq26wOKQNHMhx\npWxeydQ5EgPZY+Aviz5Dnxe8aB7oSSovpXByzxURSabOuCK21awW5WJCGNpmqhWK\nZzACBDEstccj57c4OGV0eayHJRsluVr2e9NHRINZA3qdB37e6gsI1xHo\n-----END CERTIFICATE-----\n"""

ceph_generated_key = """-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDLIx7gMwD4x5gr\nsEdkwcGrvOOWIQ5w2DGiaNTypt/lRIXsyqt/r7/ztdaqPikLIBbzHRZcSvoDnpr5\nyYFWa36q1YPOXTbRqgh3qUvkSAsufr+QwMIIkur74uD1wSh8xK9xmH6VjZ/7Wn4L\n7TTF6e2ste9cY6KUBlZpB+iNPYGlGc1ZYgVukXCVwquWDYbRwWNXlzWbprTCmxD0\nkc6J6rRK/AKLFqPCrcLABi66JTXCMjigk7gijX6DzIHecfYyj/blICkLExe5eHiQ\nctCv9PjiGqKy8bi4liAoxxIitaPmjbmZyUHIaLVQj+RmQJQRL3iLtn/eKHHgFp3Q\nAyz/23AXAgMBAAECggEAVoTB3Mm8azlPlaQB9GcV3tiXslSn+uYJ1duCf0sV52dV\nBzKW8s5fGiTjpiTNhGCJhchowqxoaew+o47wmGc2TvqbpeRLuecKrjScD0GkCYyQ\neM2wlshEbz4FhIZdgS6gbuh9WaM1dW/oaZoBNR5aTYo7xYTmNNeyLA/jO2zr7+4W\n5yES1lMSBXpKk7bDGKYY4bsX2b5RLr2Grh2u2bp7hoLABCEvuu8tSQdWXLEXWpXo\njwmV3hc6tabypIa0mj2Dmn2Dmt1ppSO0AZWG/WAizN3f4Z0r/u9HnbVrVmh0IEDw\n3uf2LP5o3msG9qKCbzv3lMgt9mMr70HOKnJ8ohMSKQKBgQDLkNb+0nr152HU9AeJ\nvdz8BeMxcwxCG77iwZphZ1HprmYKvvXgedqWtS6FRU+nV6UuQoPUbQxJBQzrN1Qv\nwKSlOAPCrTJgNgF/RbfxZTrIgCPuK2KM8I89VZv92TSGi362oQA4MazXC8RAWjoJ\nSu1/PHzK3aXOfVNSLrOWvIYeZQKBgQD/dgT6RUXKg0UhmXj7ExevV+c7oOJTDlMl\nvLngrmbjRgPO9VxLnZQGdyaBJeRngU/UXfNgajT/MU8B5fSKInnTMawv/tW7634B\nw3v6n5kNIMIjJmENRsXBVMllDTkT9S7ApV+VoGnXRccbTiDapBThSGd0wri/CuwK\nNWK1YFOeywKBgEDyI/XG114PBUJ43NLQVWm+wx5qszWAPqV/2S5MVXD1qC6zgCSv\nG9NLWN1CIMimCNg6dm7Wn73IM7fzvhNCJgVkWqbItTLG6DFf3/DPODLx1wTMqLOI\nqFqMLqmNm9l1Nec0dKp5BsjRQzq4zp1aX21hsfrTPmwjxeqJZdioqy2VAoGAXR5X\nCCdSHlSlUW8RE2xNOOQw7KJjfWT+WAYoN0c7R+MQplL31rRU7dpm1bLLRBN11vJ8\nMYvlT5RYuVdqQSP6BkrX+hLJNBvOLbRlL+EXOBrVyVxHCkDe+u7+DnC4epbn+N8P\nLYpwqkDMKB7diPVAizIKTBxinXjMu5fkKDs5n+sCgYBbZheYKk5M0sIxiDfZuXGB\nkf4mJdEkTI1KUGRdCwO/O7hXbroGoUVJTwqBLi1tKqLLarwCITje2T200BYOzj82\nqwRkCXGtXPKnxYEEUOiFx9OeDrzsZV00cxsEnX0Zdj+PucQ/J3Cvd0dWUspJfLHJ\n39gnaegswnz9KMQAvzKFdg==\n-----END PRIVATE KEY-----\n"""


class TestMonitoring:
    def _get_config(self, url: str) -> str:

        return f"""
        # This file is generated by cephadm.
        # See https://prometheus.io/docs/alerting/configuration/ for documentation.

        global:
          resolve_timeout: 5m
          http_config:
            tls_config:
              insecure_skip_verify: true

        route:
          receiver: 'default'
          routes:
            - group_by: ['alertname']
              group_wait: 10s
              group_interval: 10s
              repeat_interval: 1h
              receiver: 'ceph-dashboard'

        receivers:
        - name: 'default'
          webhook_configs:
        - name: 'custom-receiver'
          webhook_configs:
        - name: 'ceph-dashboard'
          webhook_configs:
          - url: '{url}/api/prometheus_receiver'
        """

    @pytest.mark.parametrize(
        "dashboard_url,expected_yaml_url",
        [
            # loopback address
            ("http://[::1]:8080", "http://localhost:8080"),
            # IPv6
            (
                "http://[2001:db8:4321:0000:0000:0000:0000:0000]:8080",
                "http://[2001:db8:4321:0000:0000:0000:0000:0000]:8080",
            ),
            # IPv6 to FQDN
            (
                "http://[2001:db8:4321:0000:0000:0000:0000:0000]:8080",
                "http://mgr.fqdn.test:8080",
            ),
            # IPv4
            (
                "http://192.168.0.123:8080",
                "http://192.168.0.123:8080",
            ),
            # IPv4 to FQDN
            (
                "http://192.168.0.123:8080",
                "http://mgr.fqdn.test:8080",
            ),
        ],
    )
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("mgr_module.MgrModule.get")
    @patch("socket.getfqdn")
    def test_alertmanager_config(
        self,
        mock_getfqdn,
        mock_get,
        _run_cephadm,
        cephadm_module: CephadmOrchestrator,
        dashboard_url,
        expected_yaml_url,
    ):
        _run_cephadm.side_effect = async_side_effect(("{}", "", 0))
        mock_get.return_value = {"services": {"dashboard": dashboard_url}}
        purl = urllib.parse.urlparse(expected_yaml_url)
        mock_getfqdn.return_value = purl.hostname

        with with_host(cephadm_module, "test"):
            cephadm_module.cache.update_host_networks('test', {
                '1.2.3.0/24': {
                    'if0': ['1.2.3.1']
                },
            })
            with with_service(cephadm_module, AlertManagerSpec('alertmanager',
                                                               networks=['1.2.3.0/24'],
                                                               only_bind_port_on_networks=True)):
                y = dedent(self._get_config(expected_yaml_url)).lstrip()
                _run_cephadm.assert_called_with(
                    'test',
                    "alertmanager.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'alertmanager.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [9093, 9094],
                            'port_ips': {"9094": "1.2.3.1"},
                        },
                        "meta": {
                            'service_name': 'alertmanager',
                            'ports': [9093, 9094],
                            'ip': '1.2.3.1',
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": {
                            "files": {
                                "alertmanager.yml": y,
                            },
                            "peers": [],
                            "use_url_prefix": False,
                            "ip_to_bind_to": "1.2.3.1",
                        }
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("socket.getfqdn")
    @patch("cephadm.module.CephadmOrchestrator.get_mgr_ip", lambda _: '::1')
    @patch("cephadm.services.monitoring.password_hash", lambda password: 'alertmanager_password_hash')
    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: 'cephadm_root_cert')
    @patch("cephadm.services.cephadmservice.CephadmService.get_certificates",
           lambda instance, dspec, ips=None, fqdns=None: TLSCredentials('mycert', 'mykey'))
    def test_alertmanager_config_when_mgmt_gw_enabled(self, _get_fqdn, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        fqdn = 'host1.test'
        _get_fqdn.return_value = fqdn

        with with_host(cephadm_module, 'test'):
            cephadm_module.secure_monitoring_stack = True
            cephadm_module.set_store(AlertmanagerService.USER_CFG_KEY, 'alertmanager_user')
            cephadm_module.set_store(AlertmanagerService.PASS_CFG_KEY, 'alertmanager_plain_password')

            cephadm_module.cache.update_host_networks('test', {
                'fd12:3456:789a::/64': {
                    'if0': ['fd12:3456:789a::10']
                },
            })
            with with_service(cephadm_module, MgmtGatewaySpec("mgmt-gateway")) as _, \
                 with_service(cephadm_module, AlertManagerSpec('alertmanager',
                                                               networks=['fd12:3456:789a::/64'],
                                                               only_bind_port_on_networks=True)):

                y = dedent("""
                # This file is generated by cephadm.
                # See https://prometheus.io/docs/alerting/configuration/ for documentation.

                global:
                  resolve_timeout: 5m
                  http_config:
                    tls_config:
                      insecure_skip_verify: true

                route:
                  receiver: 'default'
                  routes:
                    - group_by: ['alertname']
                      group_wait: 10s
                      group_interval: 10s
                      repeat_interval: 1h
                      receiver: 'ceph-dashboard'

                receivers:
                - name: 'default'
                  webhook_configs:
                - name: 'custom-receiver'
                  webhook_configs:
                - name: 'ceph-dashboard'
                  webhook_configs:
                  - url: 'https://host_fqdn:29443/internal/dashboard/api/prometheus_receiver'
                    http_config:
                      tls_config:
                        insecure_skip_verify: false
                        ca_file: root_cert.pem
                        cert_file: alertmanager.crt
                        key_file: alertmanager.key
                """).lstrip()

                web_config = dedent("""
                tls_server_config:
                  cert_file: alertmanager.crt
                  key_file: alertmanager.key
                  client_auth_type: RequireAndVerifyClientCert
                  client_ca_file: root_cert.pem
                basic_auth_users:
                    alertmanager_user: alertmanager_password_hash
                """).lstrip()

                _run_cephadm.assert_called_with(
                    'test',
                    "alertmanager.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'alertmanager.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [9093, 9094],
                            'port_ips': {"9094": "fd12:3456:789a::10"}
                        },
                        "meta": {
                            'service_name': 'alertmanager',
                            'ports': [9093, 9094],
                            'ip': 'fd12:3456:789a::10',
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": {
                            "files": {
                                "alertmanager.yml": y,
                                'alertmanager.crt': 'mycert',
                                'alertmanager.key': 'mykey',
                                'web.yml': web_config,
                                'root_cert.pem': 'cephadm_root_cert'
                            },
                            'peers': [],
                            'web_config': '/etc/alertmanager/web.yml',
                            "use_url_prefix": True,
                            "ip_to_bind_to": "fd12:3456:789a::10",
                        }
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("socket.getfqdn")
    @patch("cephadm.module.CephadmOrchestrator.get_mgr_ip", lambda _: '::1')
    @patch("cephadm.services.monitoring.password_hash", lambda password: 'alertmanager_password_hash')
    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: 'cephadm_root_cert')
    @patch("cephadm.services.cephadmservice.CephadmService.get_certificates",
           lambda instance, dspec, ips=None, fqdns=None: TLSCredentials('mycert', 'mykey'))
    def test_alertmanager_config_security_enabled(self, _get_fqdn, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        fqdn = 'host1.test'
        _get_fqdn.return_value = fqdn

        with with_host(cephadm_module, 'test'):
            cephadm_module.secure_monitoring_stack = True
            cephadm_module.set_store(AlertmanagerService.USER_CFG_KEY, 'alertmanager_user')
            cephadm_module.set_store(AlertmanagerService.PASS_CFG_KEY, 'alertmanager_plain_password')
            with with_service(cephadm_module, AlertManagerSpec()):

                y = dedent(f"""
                # This file is generated by cephadm.
                # See https://prometheus.io/docs/alerting/configuration/ for documentation.

                global:
                  resolve_timeout: 5m
                  http_config:
                    tls_config:
                      insecure_skip_verify: true

                route:
                  receiver: 'default'
                  routes:
                    - group_by: ['alertname']
                      group_wait: 10s
                      group_interval: 10s
                      repeat_interval: 1h
                      receiver: 'ceph-dashboard'

                receivers:
                - name: 'default'
                  webhook_configs:
                - name: 'custom-receiver'
                  webhook_configs:
                - name: 'ceph-dashboard'
                  webhook_configs:
                  - url: 'http://{fqdn}:8080/api/prometheus_receiver'
                """).lstrip()

                web_config = dedent("""
                tls_server_config:
                  cert_file: alertmanager.crt
                  key_file: alertmanager.key
                basic_auth_users:
                    alertmanager_user: alertmanager_password_hash
                """).lstrip()

                _run_cephadm.assert_called_with(
                    'test',
                    "alertmanager.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'alertmanager.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [9093, 9094],
                        },
                        "meta": {
                            'service_name': 'alertmanager',
                            'ports': [9093, 9094],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": {
                            "files": {
                                "alertmanager.yml": y,
                                'alertmanager.crt': 'mycert',
                                'alertmanager.key': 'mykey',
                                'web.yml': web_config,
                                'root_cert.pem': 'cephadm_root_cert'
                            },
                            'peers': [],
                            'web_config': '/etc/alertmanager/web.yml',
                            "use_url_prefix": False,
                            "ip_to_bind_to": "",
                        }
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @pytest.mark.parametrize(
        "user_data",
        [
            ({'webhook_urls': ['http://foo.com:9999', 'http://bar.com:1111']}),
            ({'default_webhook_urls': ['http://bar.com:9999', 'http://foo.com:1111']}),
            ({'default_webhook_urls': ['http://bar.com:9999', 'http://foo.com:1111'],
              'webhook_urls': ['http://foo.com:9999', 'http://bar.com:1111']}),
        ],
    )
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("socket.getfqdn")
    @patch("cephadm.module.CephadmOrchestrator.get_mgr_ip", lambda _: '::1')
    @patch("cephadm.services.monitoring.password_hash", lambda password: 'alertmanager_password_hash')
    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: 'cephadm_root_cert')
    @patch('cephadm.cert_mgr.CertMgr.generate_cert', lambda instance, fqdn, ip: ('mycert', 'mykey'))
    def test_alertmanager_config_custom_webhook_urls(
        self,
        _get_fqdn,
        _run_cephadm,
        cephadm_module: CephadmOrchestrator,
        user_data: Dict[str, List[str]]
    ):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        cephadm_module.set_store(AlertmanagerService.USER_CFG_KEY, 'alertmanager_user')
        cephadm_module.set_store(AlertmanagerService.PASS_CFG_KEY, 'alertmanager_plain_password')
        fqdn = 'host1.test'
        _get_fqdn.return_value = fqdn

        print(user_data)

        urls = []
        if 'default_webhook_urls' in user_data:
            urls += user_data['default_webhook_urls']
        if 'webhook_urls' in user_data:
            urls += user_data['webhook_urls']
        tab_over = ' ' * 18  # since we'll be inserting this into an indented string
        webhook_configs_str = '\n'.join(f'{tab_over}- url: \'{u}\'' for u in urls)

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, AlertManagerSpec(user_data=user_data)):

                y = dedent(f"""
                # This file is generated by cephadm.
                # See https://prometheus.io/docs/alerting/configuration/ for documentation.

                global:
                  resolve_timeout: 5m
                  http_config:
                    tls_config:
                      insecure_skip_verify: true

                route:
                  receiver: 'default'
                  routes:
                    - group_by: ['alertname']
                      group_wait: 10s
                      group_interval: 10s
                      repeat_interval: 1h
                      receiver: 'ceph-dashboard'
                      continue: true
                    - group_by: ['alertname']
                      group_wait: 10s
                      group_interval: 10s
                      repeat_interval: 1h
                      receiver: 'custom-receiver'

                receivers:
                - name: 'default'
                  webhook_configs:
                - name: 'custom-receiver'
                  webhook_configs:
{webhook_configs_str}
                - name: 'ceph-dashboard'
                  webhook_configs:
                  - url: 'http://{fqdn}:8080/api/prometheus_receiver'
                """).lstrip()

                _run_cephadm.assert_called_with(
                    'test',
                    "alertmanager.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'alertmanager.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [9093, 9094],
                        },
                        "meta": {
                            'service_name': 'alertmanager',
                            'ports': [9093, 9094],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": {
                            "files": {
                                "alertmanager.yml": y,
                            },
                            'peers': [],
                            "use_url_prefix": False,
                            "ip_to_bind_to": "",
                        }
                    }),
                    use_current_daemon_image=False,
                    error_ok=True,
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("socket.getfqdn")
    @patch("cephadm.module.CephadmOrchestrator.get_mgr_ip", lambda _: '::1')
    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: 'cephadm_root_cert')
    @patch("cephadm.services.cephadmservice.CephadmService.get_certificates",
           lambda instance, dspec, ips=None, fqdns=None: TLSCredentials('mycert', 'mykey'))
    @patch('cephadm.services.cephadmservice.CephExporterService.get_keyring_with_caps', Mock(return_value='[client.ceph-exporter.test]\nkey = fake-secret\n'))
    def test_ceph_exporter_config_security_enabled(self, _get_fqdn, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        fqdn = 'host1.test'
        _get_fqdn.return_value = fqdn

        with with_host(cephadm_module, 'test'):
            cephadm_module.secure_monitoring_stack = True
            with with_service(cephadm_module, CephExporterSpec()):
                _run_cephadm.assert_called_with('test', 'ceph-exporter.test',
                                                ['_orch', 'deploy'], [],
                                                stdin=json.dumps({
                                                    "fsid": "fsid",
                                                    "name": "ceph-exporter.test",
                                                    "image": "",
                                                    "deploy_arguments": [],
                                                    "params": {"tcp_ports": [9926]},
                                                    "meta": {
                                                        "service_name": "ceph-exporter",
                                                        "ports": [9926],
                                                        "ip": None,
                                                        "deployed_by": [],
                                                        "rank": None,
                                                        "rank_generation": None,
                                                        "extra_container_args": None,
                                                        "extra_entrypoint_args": None
                                                    },
                                                    "config_blobs": {
                                                        "config": "",
                                                        "keyring": "[client.ceph-exporter.test]\nkey = fake-secret\n",
                                                        "prio-limit": "5",
                                                        "stats-period": "5",
                                                        "https_enabled": True,
                                                        "files": {
                                                            "ceph-exporter.crt": "mycert",
                                                            "ceph-exporter.key": "mykey"}}}),
                                                error_ok=True,
                                                use_current_daemon_image=False)

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("mgr_module.MgrModule.get")
    @patch("socket.getfqdn")
    def test_node_exporter_config_without_mgmt_gw(
        self,
        mock_getfqdn,
        mock_get,
        _run_cephadm,
        cephadm_module: CephadmOrchestrator,
    ):
        _run_cephadm.side_effect = async_side_effect(("{}", "", 0))
        fqdn = 'host1.test'
        mock_getfqdn.return_value = fqdn

        with with_host(cephadm_module, "test"):
            with with_service(cephadm_module, MonitoringSpec('node-exporter')):
                _run_cephadm.assert_called_with(
                    'test',
                    "node-exporter.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'node-exporter.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [9100],
                        },
                        "meta": {
                            'service_name': 'node-exporter',
                            'ports': [9100],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": {}
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.services.cephadmservice.CephadmService.get_certificates",
           lambda instance, dspec, ips=None, fqdns=None: TLSCredentials(ceph_generated_cert, ceph_generated_key))
    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: cephadm_root_ca)
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("socket.getfqdn")
    def test_node_exporter_config_with_mgmt_gw(
        self,
        mock_getfqdn,
        _run_cephadm,
        cephadm_module: CephadmOrchestrator,
    ):
        _run_cephadm.side_effect = async_side_effect(("{}", "", 0))
        mock_getfqdn.return_value = 'host1.test'

        y = dedent("""
        tls_server_config:
          cert_file: node_exporter.crt
          key_file: node_exporter.key
          client_auth_type: RequireAndVerifyClientCert
          client_ca_file: root_cert.pem
        """).lstrip()

        with with_host(cephadm_module, "test"):
            with with_service(cephadm_module, MgmtGatewaySpec("mgmt-gateway")) as _, \
                 with_service(cephadm_module, MonitoringSpec('node-exporter')):
                _run_cephadm.assert_called_with(
                    'test',
                    "node-exporter.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'node-exporter.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [9100],
                        },
                        "meta": {
                            'service_name': 'node-exporter',
                            'ports': [9100],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": {
                            "files": {
                                "web.yml": y,
                                'root_cert.pem': f"{cephadm_root_ca}",
                                'node_exporter.crt': f"{ceph_generated_cert}",
                                'node_exporter.key': f"{ceph_generated_key}",
                            },
                            'web_config': '/etc/node-exporter/web.yml',
                        }
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.module.CephadmOrchestrator._get_mgr_ips", lambda _: ['192.168.100.100', '::1'])
    def test_prometheus_config_security_disabled(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        pool = 'testpool'
        group = 'mygroup'
        s = RGWSpec(service_id="foo", placement=PlacementSpec(count=1), rgw_frontend_type='beast')
        with with_host(cephadm_module, 'test'):
            # host "test" needs to have networks for keepalive to be placed
            cephadm_module.cache.update_host_networks('test', {
                '1.2.3.0/24': {
                    'if0': ['1.2.3.1']
                },
            })
            with with_service(cephadm_module, MonitoringSpec('node-exporter')) as _, \
                    with_service(cephadm_module, CephExporterSpec('ceph-exporter')) as _, \
                    with_service(cephadm_module, s) as _, \
                    with_service(cephadm_module, AlertManagerSpec('alertmanager')) as _, \
                    with_service(cephadm_module, IngressSpec(service_id='ingress',
                                                             frontend_port=8089,
                                                             monitor_port=8999,
                                                             monitor_user='admin',
                                                             monitor_password='12345',
                                                             keepalived_password='12345',
                                                             virtual_ip="1.2.3.4/32",
                                                             backend_service='rgw.foo',
                                                             enable_stats=True)) as _, \
                    with_service(cephadm_module, NvmeofServiceSpec(service_id=f'{pool}.{group}',
                                                                   group=group,
                                                                   pool=pool)) as _, \
                    with_service(cephadm_module, PrometheusSpec('prometheus',
                                                                networks=['1.2.3.0/24'],
                                                                only_bind_port_on_networks=True)) as _:

                y = dedent("""
                # This file is generated by cephadm.
                global:
                  scrape_interval: 10s
                  evaluation_interval: 10s
                  external_labels:
                    cluster: fsid

                rule_files:
                  - /etc/prometheus/alerting/*

                alerting:
                  alertmanagers:
                    - scheme: http
                      http_sd_configs:
                        - url: http://192.168.100.100:8765/sd/prometheus/sd-config?service=alertmanager
                        - url: http://[::1]:8765/sd/prometheus/sd-config?service=alertmanager

                scrape_configs:
                  - job_name: 'ceph'
                    relabel_configs:
                    - source_labels: [__address__]
                      target_label: cluster
                      replacement: fsid
                    - source_labels: [instance]
                      target_label: instance
                      replacement: 'ceph_cluster'
                    honor_labels: true
                    http_sd_configs:
                    - url: http://192.168.100.100:8765/sd/prometheus/sd-config?service=ceph
                    - url: http://[::1]:8765/sd/prometheus/sd-config?service=ceph

                  - job_name: 'ceph-exporter'
                    relabel_configs:
                    - source_labels: [__address__]
                      target_label: cluster
                      replacement: fsid
                    honor_labels: true
                    http_sd_configs:
                    - url: http://192.168.100.100:8765/sd/prometheus/sd-config?service=ceph-exporter
                    - url: http://[::1]:8765/sd/prometheus/sd-config?service=ceph-exporter

                  - job_name: 'ingress'
                    relabel_configs:
                    - source_labels: [__address__]
                      target_label: cluster
                      replacement: fsid
                    honor_labels: true
                    http_sd_configs:
                    - url: http://192.168.100.100:8765/sd/prometheus/sd-config?service=ingress
                    - url: http://[::1]:8765/sd/prometheus/sd-config?service=ingress

                  - job_name: 'node-exporter'
                    relabel_configs:
                    - source_labels: [__address__]
                      target_label: cluster
                      replacement: fsid
                    honor_labels: true
                    http_sd_configs:
                    - url: http://192.168.100.100:8765/sd/prometheus/sd-config?service=node-exporter
                    - url: http://[::1]:8765/sd/prometheus/sd-config?service=node-exporter

                  - job_name: 'nvmeof'
                    relabel_configs:
                    - source_labels: [__address__]
                      target_label: cluster
                      replacement: fsid
                    honor_labels: true
                    http_sd_configs:
                    - url: http://192.168.100.100:8765/sd/prometheus/sd-config?service=nvmeof
                    - url: http://[::1]:8765/sd/prometheus/sd-config?service=nvmeof


                """).lstrip()

                _run_cephadm.assert_called_with(
                    'test',
                    "prometheus.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'prometheus.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [9095],
                            'port_ips': {'8765': '1.2.3.1'}
                        },
                        "meta": {
                            'service_name': 'prometheus',
                            'ports': [9095],
                            'ip': '1.2.3.1',
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": {
                            "files": {
                                "prometheus.yml": y,
                                "/etc/prometheus/alerting/custom_alerts.yml": "",
                            },
                            'retention_time': '15d',
                            'retention_size': '0',
                            'ip_to_bind_to': '1.2.3.1',
                            "use_url_prefix": False
                        },
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.module.CephadmOrchestrator.get_unique_name")
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.module.CephadmOrchestrator._get_mgr_ips", lambda _: ['::1'])
    @patch("cephadm.services.monitoring.password_hash", lambda password: 'prometheus_password_hash')
    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: 'cephadm_root_cert')
    @patch("cephadm.services.cephadmservice.CephadmService.get_certificates",
           lambda instance, dspec, ips=None, fqdns=None: TLSCredentials('mycert', 'mykey'))
    def test_prometheus_config_security_enabled(self, _run_cephadm, _get_uname, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        _get_uname.return_value = 'test'
        pool = 'testpool'
        group = 'mygroup'
        s = RGWSpec(service_id="foo", placement=PlacementSpec(count=1), rgw_frontend_type='beast')
        smb_spec = SMBSpec(cluster_id='foxtrot', config_uri='rados://.smb/foxtrot/config.json',)

        with with_host(cephadm_module, 'test'):
            cephadm_module.secure_monitoring_stack = True
            cephadm_module.set_store(PrometheusService.USER_CFG_KEY, 'prometheus_user')
            cephadm_module.set_store(PrometheusService.PASS_CFG_KEY, 'prometheus_plain_password')
            cephadm_module.set_store(AlertmanagerService.USER_CFG_KEY, 'alertmanager_user')
            cephadm_module.set_store(AlertmanagerService.PASS_CFG_KEY, 'alertmanager_plain_password')
            cephadm_module.http_server.service_discovery.username = 'sd_user'
            cephadm_module.http_server.service_discovery.password = 'sd_password'
            # host "test" needs to have networks for keepalive to be placed
            cephadm_module.cache.update_host_networks('test', {
                '1.2.3.0/24': {
                    'if0': ['1.2.3.1']
                },
            })
            with with_service(cephadm_module, MonitoringSpec('node-exporter')) as _, \
                    with_service(cephadm_module, smb_spec) as _, \
                    with_service(cephadm_module, CephExporterSpec('ceph-exporter')) as _, \
                    with_service(cephadm_module, s) as _, \
                    with_service(cephadm_module, AlertManagerSpec('alertmanager')) as _, \
                    with_service(cephadm_module, IngressSpec(service_id='ingress',
                                                             frontend_port=8089,
                                                             monitor_port=8999,
                                                             monitor_user='admin',
                                                             monitor_password='12345',
                                                             keepalived_password='12345',
                                                             virtual_ip="1.2.3.4/32",
                                                             backend_service='rgw.foo',
                                                             enable_stats=True)) as _, \
                    with_service(cephadm_module, NvmeofServiceSpec(service_id=f'{pool}.{group}',
                                                                   group=group,
                                                                   pool=pool)) as _, \
                    with_service(cephadm_module, PrometheusSpec('prometheus')) as _:

                web_config = dedent("""
                tls_server_config:
                  cert_file: prometheus.crt
                  key_file: prometheus.key
                basic_auth_users:
                    prometheus_user: prometheus_password_hash
                """).lstrip()

                y = dedent("""
                # This file is generated by cephadm.
                global:
                  scrape_interval: 10s
                  evaluation_interval: 10s
                  external_labels:
                    cluster: fsid

                rule_files:
                  - /etc/prometheus/alerting/*

                alerting:
                  alertmanagers:
                    - scheme: https
                      basic_auth:
                        username: alertmanager_user
                        password: alertmanager_plain_password
                      tls_config:
                        ca_file: root_cert.pem
                        cert_file: prometheus.crt
                        key_file: prometheus.key
                      path_prefix: '/'
                      http_sd_configs:
                        - url: https://[::1]:8765/sd/prometheus/sd-config?service=alertmanager
                          basic_auth:
                            username: sd_user
                            password: sd_password
                          tls_config:
                            ca_file: root_cert.pem
                            cert_file: prometheus.crt
                            key_file: prometheus.key

                scrape_configs:
                  - job_name: 'ceph'
                    relabel_configs:
                    - source_labels: [__address__]
                      target_label: cluster
                      replacement: fsid
                    - source_labels: [instance]
                      target_label: instance
                      replacement: 'ceph_cluster'
                    scheme: https
                    tls_config:
                      ca_file: root_cert.pem
                      cert_file: prometheus.crt
                      key_file: prometheus.key
                    honor_labels: true
                    http_sd_configs:
                    - url: https://[::1]:8765/sd/prometheus/sd-config?service=ceph
                      basic_auth:
                        username: sd_user
                        password: sd_password
                      tls_config:
                        ca_file: root_cert.pem
                        cert_file: prometheus.crt
                        key_file: prometheus.key

                  - job_name: 'ceph-exporter'
                    relabel_configs:
                    - source_labels: [__address__]
                      target_label: cluster
                      replacement: fsid
                    scheme: https
                    tls_config:
                      ca_file: root_cert.pem
                      cert_file: prometheus.crt
                      key_file: prometheus.key
                    honor_labels: true
                    http_sd_configs:
                    - url: https://[::1]:8765/sd/prometheus/sd-config?service=ceph-exporter
                      basic_auth:
                        username: sd_user
                        password: sd_password
                      tls_config:
                        ca_file: root_cert.pem
                        cert_file: prometheus.crt
                        key_file: prometheus.key

                  - job_name: 'ingress'
                    relabel_configs:
                    - source_labels: [__address__]
                      target_label: cluster
                      replacement: fsid
                    scheme: https
                    tls_config:
                      ca_file: root_cert.pem
                      cert_file: prometheus.crt
                      key_file: prometheus.key
                    honor_labels: true
                    http_sd_configs:
                    - url: https://[::1]:8765/sd/prometheus/sd-config?service=ingress
                      basic_auth:
                        username: sd_user
                        password: sd_password
                      tls_config:
                        ca_file: root_cert.pem
                        cert_file: prometheus.crt
                        key_file: prometheus.key

                  - job_name: 'node-exporter'
                    relabel_configs:
                    - source_labels: [__address__]
                      target_label: cluster
                      replacement: fsid
                    scheme: https
                    tls_config:
                      ca_file: root_cert.pem
                      cert_file: prometheus.crt
                      key_file: prometheus.key
                    honor_labels: true
                    http_sd_configs:
                    - url: https://[::1]:8765/sd/prometheus/sd-config?service=node-exporter
                      basic_auth:
                        username: sd_user
                        password: sd_password
                      tls_config:
                        ca_file: root_cert.pem
                        cert_file: prometheus.crt
                        key_file: prometheus.key

                  - job_name: 'nvmeof'
                    relabel_configs:
                    - source_labels: [__address__]
                      target_label: cluster
                      replacement: fsid
                    scheme: https
                    tls_config:
                      ca_file: root_cert.pem
                      cert_file: prometheus.crt
                      key_file: prometheus.key
                    honor_labels: true
                    http_sd_configs:
                    - url: https://[::1]:8765/sd/prometheus/sd-config?service=nvmeof
                      basic_auth:
                        username: sd_user
                        password: sd_password
                      tls_config:
                        ca_file: root_cert.pem
                        cert_file: prometheus.crt
                        key_file: prometheus.key

                  - job_name: 'smb'
                    relabel_configs:
                    - source_labels: [__address__]
                      target_label: cluster
                      replacement: fsid
                    scheme: https
                    tls_config:
                      ca_file: root_cert.pem
                      cert_file: prometheus.crt
                      key_file: prometheus.key
                    honor_labels: true
                    http_sd_configs:
                    - url: https://[::1]:8765/sd/prometheus/sd-config?service=smb
                      basic_auth:
                        username: sd_user
                        password: sd_password
                      tls_config:
                        ca_file: root_cert.pem
                        cert_file: prometheus.crt
                        key_file: prometheus.key


                """).lstrip()

                _run_cephadm.assert_called_with(
                    'test',
                    "prometheus.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'prometheus.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [9095],
                        },
                        "meta": {
                            'service_name': 'prometheus',
                            'ports': [9095],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": {
                            'files': {
                                'prometheus.yml': y,
                                'root_cert.pem': 'cephadm_root_cert',
                                'web.yml': web_config,
                                'prometheus.crt': 'mycert',
                                'prometheus.key': 'mykey',
                                "/etc/prometheus/alerting/custom_alerts.yml": "",
                            },
                            'retention_time': '15d',
                            'retention_size': '0',
                            'ip_to_bind_to': '',
                            "use_url_prefix": False,
                            'web_config': '/etc/prometheus/web.yml'
                        },
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_loki_config(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, MonitoringSpec('loki')) as _:

                y = dedent("""
                # This file is generated by cephadm.
                auth_enabled: false

                server:
                  http_listen_port: 3100
                  grpc_listen_port: 8080

                common:
                  path_prefix: /loki
                  storage:
                    filesystem:
                      chunks_directory: /loki/chunks
                      rules_directory: /loki/rules
                  replication_factor: 1
                  ring:
                    instance_addr: 127.0.0.1
                    kvstore:
                      store: inmemory

                schema_config:
                  configs:
                    - from: 2020-10-24
                      store: boltdb-shipper
                      object_store: filesystem
                      schema: v11
                      index:
                        prefix: index_
                        period: 24h
                    - from: 2024-05-03
                      store: tsdb
                      object_store: filesystem
                      schema: v13
                      index:
                        prefix: index_
                        period: 24h""").lstrip()

                _run_cephadm.assert_called_with(
                    'test',
                    "loki.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'loki.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [3100],
                        },
                        "meta": {
                            'service_name': 'loki',
                            'ports': [3100],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": {
                            "files": {
                                "loki.yml": y
                            },
                        },
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_promtail_config(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, ServiceSpec('mgr')) as _, \
                    with_service(cephadm_module, MonitoringSpec('promtail')) as _:

                y = dedent("""
                # This file is generated by cephadm.
                server:
                  http_listen_port: 9080
                  grpc_listen_port: 0

                positions:
                  filename: /tmp/positions.yaml

                clients:
                  - url: http://:3100/loki/api/v1/push

                scrape_configs:
                - job_name: system
                  static_configs:
                  - labels:
                      job: Cluster Logs
                      __path__: /var/log/ceph/**/*.log""").lstrip()

                _run_cephadm.assert_called_with(
                    'test',
                    "promtail.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'promtail.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [9080],
                        },
                        "meta": {
                            'service_name': 'promtail',
                            'ports': [9080],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": {
                            "files": {
                                "promtail.yml": y
                            },
                        },
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.module.CephadmOrchestrator.get_mgr_ip", lambda _: '1::4')
    @patch("cephadm.module.CephadmOrchestrator.get_fqdn", lambda a, b: 'host_fqdn')
    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: cephadm_root_ca)
    @patch("cephadm.services.cephadmservice.CephadmService.get_certificates",
           lambda instance, dspec, ips=None, fqdns=None: TLSCredentials(ceph_generated_cert, ceph_generated_key))
    def test_grafana_config_with_mgmt_gw_and_ouath2_proxy(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(("{}", "", 0))

        def inline_certificate(multi_line_cert):
            """
            Converts a multi-line certificate into a one-line string with escaped newlines.
            """
            return '\\n'.join([line.strip() for line in multi_line_cert.splitlines()])

        oneline_cephadm_root_ca = inline_certificate(cephadm_root_ca)
        oneline_ceph_generated_cert = inline_certificate(ceph_generated_cert)
        oneline_ceph_generated_key = inline_certificate(ceph_generated_key)

        y = dedent(f"""
             # This file is generated by cephadm.
             apiVersion: 1

             deleteDatasources:
               - name: 'Dashboard1'
                 orgId: 1

             datasources:
               - name: 'Dashboard1'
                 type: 'prometheus'
                 access: 'proxy'
                 orgId: 1
                 url: 'https://host_fqdn:29443/internal/prometheus'
                 basicAuth: true
                 isDefault: true
                 editable: false
                 basicAuthUser: admin
                 jsonData:
                    graphiteVersion: "1.1"
                    tlsAuth: false
                    tlsAuthWithCACert: true
                    tlsSkipVerify: false
                 secureJsonData:
                   basicAuthPassword: admin
                   tlsCACert: "{oneline_cephadm_root_ca}"
                   tlsClientCert: "{oneline_ceph_generated_cert}"
                   tlsClientKey: "{oneline_ceph_generated_key}"

               - name: 'Loki'
                 type: 'loki'
                 access: 'proxy'
                 url: ''
                 basicAuth: false
                 isDefault: false
                 editable: false""").lstrip()

        oauth2_spec = OAuth2ProxySpec(provider_display_name='my_idp_provider',
                                      client_id='my_client_id',
                                      client_secret='my_client_secret',
                                      oidc_issuer_url='http://192.168.10.10:8888/dex',
                                      cookie_secret='kbAEM9opAmuHskQvt0AW8oeJRaOM2BYy5Loba0kZ0SQ=',
                                      ssl_cert=ceph_generated_cert,
                                      ssl_key=ceph_generated_key)

        with with_host(cephadm_module, "test"):
            cephadm_module.cert_mgr.save_cert('grafana_ssl_cert', ceph_generated_cert, host='test')
            cephadm_module.cert_mgr.save_key('grafana_ssl_key', ceph_generated_key, host='test')
            with with_service(cephadm_module, PrometheusSpec("prometheus")) as _, \
                 with_service(cephadm_module, MgmtGatewaySpec("mgmt-gateway")) as _, \
                 with_service(cephadm_module, oauth2_spec) as _, \
                 with_service(cephadm_module, ServiceSpec("mgr")) as _, with_service(
                     cephadm_module, GrafanaSpec("grafana")
            ) as _:
                files = {
                    'grafana.ini': dedent("""
                        # This file is generated by cephadm.
                        [users]
                          default_theme = light
                        [auth.anonymous]
                          enabled = true
                          org_name = 'Main Org.'
                          org_role = 'Viewer'
                        [server]
                          domain = 'host_fqdn'
                          protocol = https
                          cert_file = /etc/grafana/certs/cert_file
                          cert_key = /etc/grafana/certs/cert_key
                          http_port = 3000
                          http_addr = 
                          root_url = %(protocol)s://%(domain)s:%(http_port)s/grafana/
                          serve_from_sub_path = true
                        [snapshots]
                          external_enabled = false
                        [security]
                          disable_initial_admin_creation = true
                          cookie_secure = true
                          cookie_samesite = none
                          allow_embedding = true
                        [auth]
                          disable_login_form = true
                        [auth.proxy]
                          enabled = true
                          header_name = X-WEBAUTH-USER
                          header_property = username
                          auto_sign_up = true
                          sync_ttl = 15
                          whitelist = 1::4
                          headers_encoded = false
                          enable_login_token = false
                          headers = Role:X-WEBAUTH-ROLE
                        [analytics]
                          check_for_updates = false
                          reporting_enabled = false
                        [plugins]
                          check_for_plugin_updates = false
                          public_key_retrieval_disabled = true""").lstrip(),  # noqa: W291
                    "provisioning/datasources/ceph-dashboard.yml": y,
                    'certs/cert_file': dedent(f"""
                        # generated by cephadm\n{ceph_generated_cert}""").lstrip(),
                    'certs/cert_key': dedent(f"""
                        # generated by cephadm\n{ceph_generated_key}""").lstrip(),
                    'provisioning/dashboards/default.yml': dedent("""
                        # This file is generated by cephadm.
                        apiVersion: 1

                        providers:
                          - name: 'Ceph Dashboard'
                            orgId: 1
                            folder: ''
                            type: file
                            disableDeletion: false
                            updateIntervalSeconds: 3
                            editable: false
                            options:
                              path: '/etc/grafana/provisioning/dashboards'""").lstrip(),
                }

                _run_cephadm.assert_called_with(
                    'test',
                    "grafana.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'grafana.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [3000],
                        },
                        "meta": {
                            'service_name': 'grafana',
                            'ports': [3000],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": {
                            "files": files,
                        },
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.module.CephadmOrchestrator.get_mgr_ip", lambda _: '1::4')
    @patch("cephadm.module.CephadmOrchestrator.get_fqdn", lambda a, b: 'host_fqdn')
    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: cephadm_root_ca)
    @patch("cephadm.services.cephadmservice.CephadmService.get_certificates",
           lambda instance, dspec, ips=None, fqdns=None: TLSCredentials(ceph_generated_cert, ceph_generated_key))
    def test_grafana_config_with_mgmt_gw(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(("{}", "", 0))

        def inline_certificate(multi_line_cert):
            """
            Converts a multi-line certificate into a one-line string with escaped newlines.
            """
            return '\\n'.join([line.strip() for line in multi_line_cert.splitlines()])

        oneline_cephadm_root_ca = inline_certificate(cephadm_root_ca)
        oneline_ceph_generated_cert = inline_certificate(ceph_generated_cert)
        oneline_ceph_generated_key = inline_certificate(ceph_generated_key)

        y = dedent(f"""
             # This file is generated by cephadm.
             apiVersion: 1

             deleteDatasources:
               - name: 'Dashboard1'
                 orgId: 1

             datasources:
               - name: 'Dashboard1'
                 type: 'prometheus'
                 access: 'proxy'
                 orgId: 1
                 url: 'https://host_fqdn:29443/internal/prometheus'
                 basicAuth: true
                 isDefault: true
                 editable: false
                 basicAuthUser: admin
                 jsonData:
                    graphiteVersion: "1.1"
                    tlsAuth: false
                    tlsAuthWithCACert: true
                    tlsSkipVerify: false
                 secureJsonData:
                   basicAuthPassword: admin
                   tlsCACert: "{oneline_cephadm_root_ca}"
                   tlsClientCert: "{oneline_ceph_generated_cert}"
                   tlsClientKey: "{oneline_ceph_generated_key}"

               - name: 'Loki'
                 type: 'loki'
                 access: 'proxy'
                 url: ''
                 basicAuth: false
                 isDefault: false
                 editable: false""").lstrip()

        with with_host(cephadm_module, "test"):
            with with_service(
                cephadm_module, PrometheusSpec("prometheus")
            ) as _, with_service(cephadm_module, MgmtGatewaySpec("mgmt-gateway")) as _, \
                with_service(cephadm_module, ServiceSpec("mgr")) as _, with_service(
                cephadm_module, GrafanaSpec("grafana")
            ) as _:
                cephadm_module.cert_mgr.save_self_signed_cert_key_pair('grafana',
                                                                       TLSCredentials(ceph_generated_cert, ceph_generated_key),
                                                                       host='test')
                files = {
                    'grafana.ini': dedent("""
                        # This file is generated by cephadm.
                        [users]
                          default_theme = light
                        [auth.anonymous]
                          enabled = true
                          org_name = 'Main Org.'
                          org_role = 'Viewer'
                        [server]
                          domain = 'host_fqdn'
                          protocol = https
                          cert_file = /etc/grafana/certs/cert_file
                          cert_key = /etc/grafana/certs/cert_key
                          http_port = 3000
                          http_addr = 
                          root_url = %(protocol)s://%(domain)s:%(http_port)s/grafana/
                          serve_from_sub_path = true
                        [snapshots]
                          external_enabled = false
                        [security]
                          disable_initial_admin_creation = true
                          cookie_secure = true
                          cookie_samesite = none
                          allow_embedding = true
                        [analytics]
                          check_for_updates = false
                          reporting_enabled = false
                        [plugins]
                          check_for_plugin_updates = false
                          public_key_retrieval_disabled = true""").lstrip(),  # noqa: W291
                    "provisioning/datasources/ceph-dashboard.yml": y,
                    'certs/cert_file': dedent(f"""
                        # generated by cephadm\n{ceph_generated_cert}""").lstrip(),
                    'certs/cert_key': dedent(f"""
                        # generated by cephadm\n{ceph_generated_key}""").lstrip(),
                    'provisioning/dashboards/default.yml': dedent("""
                        # This file is generated by cephadm.
                        apiVersion: 1

                        providers:
                          - name: 'Ceph Dashboard'
                            orgId: 1
                            folder: ''
                            type: file
                            disableDeletion: false
                            updateIntervalSeconds: 3
                            editable: false
                            options:
                              path: '/etc/grafana/provisioning/dashboards'""").lstrip(),
                }

                _run_cephadm.assert_called_with(
                    'test',
                    "grafana.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'grafana.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [3000],
                        },
                        "meta": {
                            'service_name': 'grafana',
                            'ports': [3000],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": {
                            "files": files,
                        },
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.module.CephadmOrchestrator.get_mgr_ip", lambda _: '1::4')
    @patch("cephadm.module.CephadmOrchestrator.get_fqdn", lambda a, b: 'host_fqdn')
    @patch("cephadm.services.cephadmservice.CephadmService.get_certificates",
           lambda instance, dspec, ips=None, fqdns=None: TLSCredentials(ceph_generated_cert, ceph_generated_key))
    def test_grafana_config(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(("{}", "", 0))

        with with_host(cephadm_module, "test"):
            with with_service(
                cephadm_module, PrometheusSpec("prometheus")
            ) as _, with_service(cephadm_module, ServiceSpec("mgr")) as _, with_service(
                cephadm_module, GrafanaSpec("grafana")
            ) as _:
                cephadm_module.cert_mgr.save_self_signed_cert_key_pair('grafana',
                                                                       TLSCredentials(ceph_generated_cert, ceph_generated_key),
                                                                       host='test')
                files = {
                    'grafana.ini': dedent("""
                        # This file is generated by cephadm.
                        [users]
                          default_theme = light
                        [auth.anonymous]
                          enabled = true
                          org_name = 'Main Org.'
                          org_role = 'Viewer'
                        [server]
                          domain = 'host_fqdn'
                          protocol = https
                          cert_file = /etc/grafana/certs/cert_file
                          cert_key = /etc/grafana/certs/cert_key
                          http_port = 3000
                          http_addr = 
                        [snapshots]
                          external_enabled = false
                        [security]
                          disable_initial_admin_creation = true
                          cookie_secure = true
                          cookie_samesite = none
                          allow_embedding = true
                        [analytics]
                          check_for_updates = false
                          reporting_enabled = false
                        [plugins]
                          check_for_plugin_updates = false
                          public_key_retrieval_disabled = true""").lstrip(),  # noqa: W291
                    'provisioning/datasources/ceph-dashboard.yml': dedent("""
                        # This file is generated by cephadm.
                        apiVersion: 1

                        deleteDatasources:
                          - name: 'Dashboard1'
                            orgId: 1

                        datasources:
                          - name: 'Dashboard1'
                            type: 'prometheus'
                            access: 'proxy'
                            orgId: 1
                            url: 'http://host_fqdn:9095'
                            basicAuth: false
                            isDefault: true
                            editable: false

                          - name: 'Loki'
                            type: 'loki'
                            access: 'proxy'
                            url: ''
                            basicAuth: false
                            isDefault: false
                            editable: false""").lstrip(),
                    'certs/cert_file': dedent(f"""
                        # generated by cephadm\n{ceph_generated_cert}""").lstrip(),
                    'certs/cert_key': dedent(f"""
                        # generated by cephadm\n{ceph_generated_key}""").lstrip(),
                    'provisioning/dashboards/default.yml': dedent("""
                        # This file is generated by cephadm.
                        apiVersion: 1

                        providers:
                          - name: 'Ceph Dashboard'
                            orgId: 1
                            folder: ''
                            type: file
                            disableDeletion: false
                            updateIntervalSeconds: 3
                            editable: false
                            options:
                              path: '/etc/grafana/provisioning/dashboards'""").lstrip(),
                }

                _run_cephadm.assert_called_with(
                    'test',
                    "grafana.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'grafana.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [3000],
                        },
                        "meta": {
                            'service_name': 'grafana',
                            'ports': [3000],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": {
                            "files": files,
                        },
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_grafana_initial_admin_pw(self, cephadm_module: CephadmOrchestrator):
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, ServiceSpec('mgr')) as _, \
                    with_service(cephadm_module, GrafanaSpec(initial_admin_password='secure')):
                out = service_registry.get_service('grafana').generate_config(
                    CephadmDaemonDeploySpec('test', 'daemon', 'grafana'))
                assert out == (
                    {
                        'files':
                            {
                                'grafana.ini':
                                    '# This file is generated by cephadm.\n'
                                    '[users]\n'
                                    '  default_theme = light\n'
                                    '[auth.anonymous]\n'
                                    '  enabled = true\n'
                                    "  org_name = 'Main Org.'\n"
                                    "  org_role = 'Viewer'\n"
                                    '[server]\n'
                                    "  domain = 'host_fqdn'\n"
                                    '  protocol = https\n'
                                    '  cert_file = /etc/grafana/certs/cert_file\n'
                                    '  cert_key = /etc/grafana/certs/cert_key\n'
                                    '  http_port = 3000\n'
                                    '  http_addr = \n'
                                    '[snapshots]\n'
                                    '  external_enabled = false\n'
                                    '[security]\n'
                                    '  admin_user = admin\n'
                                    '  admin_password = secure\n'
                                    '  cookie_secure = true\n'
                                    '  cookie_samesite = none\n'
                                    '  allow_embedding = true\n'
                                    '[analytics]\n'
                                    '  check_for_updates = false\n'
                                    '  reporting_enabled = false\n'
                                    '[plugins]\n'
                                    '  check_for_plugin_updates = false\n'
                                    '  public_key_retrieval_disabled = true',
                                'provisioning/datasources/ceph-dashboard.yml':
                                    "# This file is generated by cephadm.\n"
                                    "apiVersion: 1\n\n"
                                    'deleteDatasources:\n\n'
                                    'datasources:\n\n'
                                    "  - name: 'Loki'\n"
                                    "    type: 'loki'\n"
                                    "    access: 'proxy'\n"
                                    "    url: ''\n"
                                    '    basicAuth: false\n'
                                    '    isDefault: false\n'
                                    '    editable: false',
                                'certs/cert_file': ANY,
                                'certs/cert_key': ANY,
                                'provisioning/dashboards/default.yml':
                                    '# This file is generated by cephadm.\n'
                                    'apiVersion: 1\n\n'
                                    'providers:\n'
                                    "  - name: 'Ceph Dashboard'\n"
                                    '    orgId: 1\n'
                                    "    folder: ''\n"
                                    '    type: file\n'
                                    '    disableDeletion: false\n'
                                    '    updateIntervalSeconds: 3\n'
                                    '    editable: false\n'
                                    '    options:\n'
                                    "      path: '/etc/grafana/provisioning/dashboards'"
                            }}, ['secure_monitoring_stack:False'])

    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_grafana_no_anon_access(self, cephadm_module: CephadmOrchestrator):
        # with anonymous_access set to False, expecting the [auth.anonymous] section
        # to not be present in the grafana config. Note that we require an initial_admin_password
        # to be provided when anonymous_access is False
        cephadm_module._init_cert_mgr()
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, ServiceSpec('mgr')) as _, \
                    with_service(cephadm_module, GrafanaSpec(anonymous_access=False, initial_admin_password='secure')):
                out = service_registry.get_service('grafana').generate_config(
                    CephadmDaemonDeploySpec('test', 'daemon', 'grafana'))
                assert out == (
                    {
                        'files':
                            {
                                'grafana.ini':
                                    '# This file is generated by cephadm.\n'
                                    '[users]\n'
                                    '  default_theme = light\n'
                                    '[server]\n'
                                    "  domain = 'host_fqdn'\n"
                                    '  protocol = https\n'
                                    '  cert_file = /etc/grafana/certs/cert_file\n'
                                    '  cert_key = /etc/grafana/certs/cert_key\n'
                                    '  http_port = 3000\n'
                                    '  http_addr = \n'
                                    '[snapshots]\n'
                                    '  external_enabled = false\n'
                                    '[security]\n'
                                    '  admin_user = admin\n'
                                    '  admin_password = secure\n'
                                    '  cookie_secure = true\n'
                                    '  cookie_samesite = none\n'
                                    '  allow_embedding = true\n'
                                    '[analytics]\n'
                                    '  check_for_updates = false\n'
                                    '  reporting_enabled = false\n'
                                    '[plugins]\n'
                                    '  check_for_plugin_updates = false\n'
                                    '  public_key_retrieval_disabled = true',
                                'provisioning/datasources/ceph-dashboard.yml':
                                    "# This file is generated by cephadm.\n"
                                    "apiVersion: 1\n\n"
                                    'deleteDatasources:\n\n'
                                    'datasources:\n\n'
                                    "  - name: 'Loki'\n"
                                    "    type: 'loki'\n"
                                    "    access: 'proxy'\n"
                                    "    url: ''\n"
                                    '    basicAuth: false\n'
                                    '    isDefault: false\n'
                                    '    editable: false',
                                'certs/cert_file': ANY,
                                'certs/cert_key': ANY,
                                'provisioning/dashboards/default.yml':
                                    '# This file is generated by cephadm.\n'
                                    'apiVersion: 1\n\n'
                                    'providers:\n'
                                    "  - name: 'Ceph Dashboard'\n"
                                    '    orgId: 1\n'
                                    "    folder: ''\n"
                                    '    type: file\n'
                                    '    disableDeletion: false\n'
                                    '    updateIntervalSeconds: 3\n'
                                    '    editable: false\n'
                                    '    options:\n'
                                    "      path: '/etc/grafana/provisioning/dashboards'"
                            }}, ['secure_monitoring_stack:False'])

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_monitoring_ports(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test'):

            yaml_str = """service_type: alertmanager
service_name: alertmanager
placement:
    count: 1
spec:
    port: 4200
"""
            yaml_file = yaml.safe_load(yaml_str)
            spec = ServiceSpec.from_json(yaml_file)

            with patch("cephadm.services.monitoring.AlertmanagerService.generate_config", return_value=({}, [])):
                with with_service(cephadm_module, spec):

                    CephadmServe(cephadm_module)._check_daemons()

                    _run_cephadm.assert_called_with(
                        'test',
                        "alertmanager.test",
                        ['_orch', 'deploy'],
                        [],
                        stdin=json.dumps({
                            "fsid": "fsid",
                            "name": 'alertmanager.test',
                            "image": '',
                            "deploy_arguments": [],
                            "params": {
                                'tcp_ports': [4200, 9094],
                            },
                            "meta": {
                                'service_name': 'alertmanager',
                                'ports': [4200, 9094],
                                'ip': None,
                                'deployed_by': [],
                                'rank': None,
                                'rank_generation': None,
                                'extra_container_args': None,
                                'extra_entrypoint_args': None,
                            },
                            "config_blobs": {},
                        }),
                        error_ok=True,
                        use_current_daemon_image=False,
                    )
