import json
from textwrap import dedent
from unittest.mock import patch
from typing import List

from orchestrator._interface import DaemonDescription

from cephadm.module import CephadmOrchestrator
from ceph.deployment.service_spec import (
    MgmtGatewaySpec,
    OAuth2ProxySpec
)
from cephadm.services.service_registry import service_registry
from cephadm.tests.fixtures import with_host, with_service, async_side_effect
from cephadm.tlsobject_types import TLSCredentials


cephadm_root_ca = """-----BEGIN CERTIFICATE-----\nMIIE7DCCAtSgAwIBAgIUE8b2zZ64geu2ns3Zfn3/4L+Cf6MwDQYJKoZIhvcNAQEL\nBQAwFzEVMBMGA1UEAwwMY2VwaGFkbS1yb290MB4XDTI0MDYyNjE0NDA1M1oXDTM0\nMDYyNzE0NDA1M1owFzEVMBMGA1UEAwwMY2VwaGFkbS1yb290MIICIjANBgkqhkiG\n9w0BAQEFAAOCAg8AMIICCgKCAgEAsZRJsdtTr9GLG1lWFql5SGc46ldFanNJd1Gl\nqXq5vgZVKRDTmNgAb/XFuNEEmbDAXYIRZolZeYKMHfn0pouPRSel0OsC6/02ZUOW\nIuN89Wgo3IYleCFpkVIumD8URP3hwdu85plRxYZTtlruBaTRH38lssyCqxaOdEt7\nAUhvYhcMPJThB17eOSQ73mb8JEC83vB47fosI7IhZuvXvRSuZwUW30rJanWNhyZq\neS2B8qw2RSO0+77H6gA4ftBnitfsE1Y8/F9Z/f92JOZuSMQXUB07msznPbRJia3f\nueO8gOc32vxd1A1/Qzp14uX34yEGY9ko2lW226cZO29IVUtXOX+LueQttwtdlpz8\ne6Npm09pXhXAHxV/OW3M28MdXmobIqT/m9MfkeAErt5guUeC5y8doz6/3VQRjFEn\nRpN0WkblgnNAQ3DONPc+Qd9Fi/wZV2X7bXoYpNdoWDsEOiE/eLmhG1A2GqU/mneP\nzQ6u79nbdwTYpwqHpa+PvusXeLfKauzI8lLUJotdXy9EK8iHUofibB61OljYye6B\nG3b8C4QfGsw8cDb4APZd/6AZYyMx/V3cGZ+GcOV7WvsC8k7yx5Uqasm/kiGQ3EZo\nuNenNEYoGYrjb8D/8QzqNUTwlEh27/ps80tO7l2GGTvWVZL0PRZbmLDvO77amtOf\nOiRXMoUCAwEAAaMwMC4wGwYDVR0RBBQwEocQAAAAAAAAAAAAAAAAAAAAATAPBgNV\nHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4ICAQAxwzX5AhYEWhTV4VUwUj5+\nqPdl4Q2tIxRokqyE+cDxoSd+6JfGUefUbNyBxDt0HaBq8obDqqrbcytxnn7mpnDu\nhtiauY+I4Amt7hqFOiFA4cCLi2mfok6g2vL53tvhd9IrsfflAU2wy7hL76Ejm5El\nA+nXlkJwps01Whl9pBkUvIbOn3pXX50LT4hb5zN0PSu957rjd2xb4HdfuySm6nW4\n4GxtVWfmGA6zbC4XMEwvkuhZ7kD2qjkAguGDF01uMglkrkCJT3OROlNBuSTSBGqt\ntntp5VytHvb7KTF7GttM3ha8/EU2KYaHM6WImQQTrOfiImAktOk4B3lzUZX3HYIx\n+sByO4P4dCvAoGz1nlWYB2AvCOGbKf0Tgrh4t4jkiF8FHTXGdfvWmjgi1pddCNAy\nn65WOCmVmLZPERAHOk1oBwqyReSvgoCFo8FxbZcNxJdlhM0Z6hzKggm3O3Dl88Xl\n5euqJjh2STkBW8Xuowkg1TOs5XyWvKoDFAUzyzeLOL8YSG+gXV22gPTUaPSVAqdb\nwd0Fx2kjConuC5bgTzQHs8XWA930U3XWZraj21Vaa8UxlBLH4fUro8H5lMSYlZNE\nJHRNW8BkznAClaFSDG3dybLsrzrBFAu/Qb5zVkT1xyq0YkepGB7leXwq6vjWA5Pw\nmZbKSphWfh0qipoqxqhfkw==\n-----END CERTIFICATE-----\n"""

ceph_generated_cert = """-----BEGIN CERTIFICATE-----\nMIICxjCCAa4CEQDIZSujNBlKaLJzmvntjukjMA0GCSqGSIb3DQEBDQUAMCExDTAL\nBgNVBAoMBENlcGgxEDAOBgNVBAMMB2NlcGhhZG0wHhcNMjIwNzEzMTE0NzA3WhcN\nMzIwNzEwMTE0NzA3WjAhMQ0wCwYDVQQKDARDZXBoMRAwDgYDVQQDDAdjZXBoYWRt\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyyMe4DMA+MeYK7BHZMHB\nq7zjliEOcNgxomjU8qbf5USF7Mqrf6+/87XWqj4pCyAW8x0WXEr6A56a+cmBVmt+\nqtWDzl020aoId6lL5EgLLn6/kMDCCJLq++Lg9cEofMSvcZh+lY2f+1p+C+00xent\nrLXvXGOilAZWaQfojT2BpRnNWWIFbpFwlcKrlg2G0cFjV5c1m6a0wpsQ9JHOieq0\nSvwCixajwq3CwAYuuiU1wjI4oJO4Io1+g8yB3nH2Mo/25SApCxMXuXh4kHLQr/T4\n4hqisvG4uJYgKMcSIrWj5o25mclByGi1UI/kZkCUES94i7Z/3ihx4Bad0AMs/9tw\nFwIDAQABMA0GCSqGSIb3DQEBDQUAA4IBAQAf+pwz7Gd7mDwU2LY0TQXsK6/8KGzh\nHuX+ErOb8h5cOAbvCnHjyJFWf6gCITG98k9nxU9NToG0WYuNm/max1y/54f0dtxZ\npUo6KSNl3w6iYCfGOeUIj8isi06xMmeTgMNzv8DYhDt+P2igN6LenqWTVztogkiV\nxQ5ZJFFLEw4sN0CXnrZX3t5ruakxLXLTLKeE0I91YJvjClSBGkVJq26wOKQNHMhx\npWxeydQ5EgPZY+Aviz5Dnxe8aB7oSSovpXByzxURSabOuCK21awW5WJCGNpmqhWK\nZzACBDEstccj57c4OGV0eayHJRsluVr2e9NHRINZA3qdB37e6gsI1xHo\n-----END CERTIFICATE-----\n"""

ceph_generated_key = """-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDLIx7gMwD4x5gr\nsEdkwcGrvOOWIQ5w2DGiaNTypt/lRIXsyqt/r7/ztdaqPikLIBbzHRZcSvoDnpr5\nyYFWa36q1YPOXTbRqgh3qUvkSAsufr+QwMIIkur74uD1wSh8xK9xmH6VjZ/7Wn4L\n7TTF6e2ste9cY6KUBlZpB+iNPYGlGc1ZYgVukXCVwquWDYbRwWNXlzWbprTCmxD0\nkc6J6rRK/AKLFqPCrcLABi66JTXCMjigk7gijX6DzIHecfYyj/blICkLExe5eHiQ\nctCv9PjiGqKy8bi4liAoxxIitaPmjbmZyUHIaLVQj+RmQJQRL3iLtn/eKHHgFp3Q\nAyz/23AXAgMBAAECggEAVoTB3Mm8azlPlaQB9GcV3tiXslSn+uYJ1duCf0sV52dV\nBzKW8s5fGiTjpiTNhGCJhchowqxoaew+o47wmGc2TvqbpeRLuecKrjScD0GkCYyQ\neM2wlshEbz4FhIZdgS6gbuh9WaM1dW/oaZoBNR5aTYo7xYTmNNeyLA/jO2zr7+4W\n5yES1lMSBXpKk7bDGKYY4bsX2b5RLr2Grh2u2bp7hoLABCEvuu8tSQdWXLEXWpXo\njwmV3hc6tabypIa0mj2Dmn2Dmt1ppSO0AZWG/WAizN3f4Z0r/u9HnbVrVmh0IEDw\n3uf2LP5o3msG9qKCbzv3lMgt9mMr70HOKnJ8ohMSKQKBgQDLkNb+0nr152HU9AeJ\nvdz8BeMxcwxCG77iwZphZ1HprmYKvvXgedqWtS6FRU+nV6UuQoPUbQxJBQzrN1Qv\nwKSlOAPCrTJgNgF/RbfxZTrIgCPuK2KM8I89VZv92TSGi362oQA4MazXC8RAWjoJ\nSu1/PHzK3aXOfVNSLrOWvIYeZQKBgQD/dgT6RUXKg0UhmXj7ExevV+c7oOJTDlMl\nvLngrmbjRgPO9VxLnZQGdyaBJeRngU/UXfNgajT/MU8B5fSKInnTMawv/tW7634B\nw3v6n5kNIMIjJmENRsXBVMllDTkT9S7ApV+VoGnXRccbTiDapBThSGd0wri/CuwK\nNWK1YFOeywKBgEDyI/XG114PBUJ43NLQVWm+wx5qszWAPqV/2S5MVXD1qC6zgCSv\nG9NLWN1CIMimCNg6dm7Wn73IM7fzvhNCJgVkWqbItTLG6DFf3/DPODLx1wTMqLOI\nqFqMLqmNm9l1Nec0dKp5BsjRQzq4zp1aX21hsfrTPmwjxeqJZdioqy2VAoGAXR5X\nCCdSHlSlUW8RE2xNOOQw7KJjfWT+WAYoN0c7R+MQplL31rRU7dpm1bLLRBN11vJ8\nMYvlT5RYuVdqQSP6BkrX+hLJNBvOLbRlL+EXOBrVyVxHCkDe+u7+DnC4epbn+N8P\nLYpwqkDMKB7diPVAizIKTBxinXjMu5fkKDs5n+sCgYBbZheYKk5M0sIxiDfZuXGB\nkf4mJdEkTI1KUGRdCwO/O7hXbroGoUVJTwqBLi1tKqLLarwCITje2T200BYOzj82\nqwRkCXGtXPKnxYEEUOiFx9OeDrzsZV00cxsEnX0Zdj+PucQ/J3Cvd0dWUspJfLHJ\n39gnaegswnz9KMQAvzKFdg==\n-----END PRIVATE KEY-----\n"""


class TestMgmtGateway:
    def test_ipv6_formatting_helpers(self, cephadm_module: CephadmOrchestrator):
        # verify that endpoints generated by the mgmt-gateway helper methods
        # correctly bracket IPv6 addresses before the port portion is added.
        svc = service_registry.get_service('mgmt-gateway')

        # service discovery endpoints use a fixed port from the orchestrator
        port = cephadm_module.service_discovery_port
        mgr_daemons = [
            DaemonDescription(daemon_type='mgr', hostname='h1', ip='fe80::1', ports=[port]),
            DaemonDescription(daemon_type='mgr', hostname='h2', ip='192.0.2.1', ports=[port]),
        ]
        cephadm_module.cache.get_daemons_by_service = lambda name: mgr_daemons if name == 'mgr' else []

        sd = svc.get_service_discovery_endpoints()
        assert sd == [f'[fe80::1]:{port}', f'192.0.2.1:{port}']

        # generic service endpoints also need the same treatment
        foo_daemons = [DaemonDescription(daemon_type='foo', hostname='f1', ip='fe80::2', ports=[8080])]
        cephadm_module.cache.get_daemons_by_service = lambda name: foo_daemons if name == 'foo' else []
        assert svc.get_service_endpoints('foo') == ['[fe80::2]:8080']

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.services.mgmt_gateway.MgmtGatewayService.get_service_endpoints")
    @patch("cephadm.services.mgmt_gateway.MgmtGatewayService.get_service_discovery_endpoints")
    @patch("cephadm.services.cephadmservice.CephadmService.get_certificates",
           lambda instance, dspec, ips=None: TLSCredentials(ceph_generated_cert, ceph_generated_key))
    @patch("cephadm.services.mgmt_gateway.MgmtGatewayService.get_self_signed_certificates_with_label",
           lambda instance, svc_spec, dspec, label, ip: TLSCredentials(ceph_generated_cert, ceph_generated_key))
    @patch("cephadm.module.CephadmOrchestrator.get_mgr_ip", lambda _: '::1')
    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: cephadm_root_ca)
    @patch("cephadm.services.mgmt_gateway.get_dashboard_endpoints", lambda _: (["ceph-node-2:8443", "ceph-node-2:8443"], "https"))
    def test_mgmt_gateway_config_no_auth(self,
                                         get_service_discovery_endpoints_mock: List[str],
                                         get_service_endpoints_mock: List[str],
                                         _run_cephadm,
                                         cephadm_module: CephadmOrchestrator):

        def get_services_endpoints(name):
            if name == 'prometheus':
                return ["192.168.100.100:9095", "192.168.100.101:9095"]
            elif name == 'grafana':
                return ["ceph-node-2:3000", "ceph-node-2:3000"]
            elif name == 'alertmanager':
                return ["192.168.100.100:9093", "192.168.100.102:9093"]
            return []

        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        get_service_endpoints_mock.side_effect = get_services_endpoints
        get_service_discovery_endpoints_mock.side_effect = lambda: ["ceph-node-0:8765", "ceph-node-2:8765"]

        server_port = 5555
        spec = MgmtGatewaySpec(port=server_port,
                               ssl_cert=ceph_generated_cert,
                               ssl_key=ceph_generated_key)

        expected = {
            "fsid": "fsid",
            "name": "mgmt-gateway.ceph-node",
            "image": "",
            "deploy_arguments": [],
            "params": {"tcp_ports": [server_port]},
            "meta": {
                "service_name": "mgmt-gateway",
                "ports": [server_port],
                "ip": None,
                "deployed_by": [],
                "rank": None,
                "rank_generation": None,
                "extra_container_args": None,
                "extra_entrypoint_args": None
            },
            "config_blobs": {
                "files": {
                    "nginx.conf": dedent("""
                                         # This file is generated by cephadm.
                                         worker_rlimit_nofile 8192;

                                         events {
                                             worker_connections 4096;
                                         }

                                         http {

                                             #access_log /dev/stdout;
                                             error_log /dev/stderr warn;
                                             client_header_buffer_size 32K;
                                             large_client_header_buffers 4 32k;
                                             proxy_busy_buffers_size 512k;
                                             proxy_buffers 4 512k;
                                             proxy_buffer_size 256K;
                                             proxy_headers_hash_max_size 1024;
                                             proxy_headers_hash_bucket_size 128;


                                             upstream service_discovery_servers {
                                              server ceph-node-0:8765;
                                              server ceph-node-2:8765;
                                             }

                                             upstream dashboard_servers {
                                              server ceph-node-2:8443;
                                              server ceph-node-2:8443;
                                             }

                                             upstream grafana_servers {
                                              server ceph-node-2:3000;
                                              server ceph-node-2:3000;
                                             }

                                             upstream prometheus_servers {
                                              ip_hash;
                                              server 192.168.100.100:9095;
                                              server 192.168.100.101:9095;
                                             }

                                             upstream alertmanager_servers {
                                              server 192.168.100.100:9093;
                                              server 192.168.100.102:9093;
                                             }

                                             include /etc/nginx_external_server.conf;
                                             include /etc/nginx_internal_server.conf;
                                         }"""),
                    "nginx_external_server.conf": dedent("""
                                             server {
                                                 listen                    5555 ssl;
                                                 listen                    [::]:5555 ssl;
                                                 ssl_certificate            /etc/nginx/ssl/nginx.crt;
                                                 ssl_certificate_key /etc/nginx/ssl/nginx.key;
                                                 ssl_protocols            TLSv1.3;
                                                 # from:  https://ssl-config.mozilla.org/#server=nginx
                                                 ssl_ciphers              ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES256-GCM-SHA384:DHE-RSA-CHACHA20-POLY1305;

                                                 # Only return Nginx in server header, no extra info will be provided
                                                 server_tokens             off;

                                                 # Perfect Forward Secrecy(PFS) is frequently compromised without this
                                                 ssl_prefer_server_ciphers on;

                                                 # Enable SSL session caching for improved performance
                                                 ssl_session_tickets       off;
                                                 ssl_session_timeout       1d;
                                                 ssl_session_cache         shared:SSL:10m;

                                                 # OCSP stapling
                                                 ssl_stapling              on;
                                                 ssl_stapling_verify       on;
                                                 resolver_timeout 5s;

                                                 # Security headers
                                                 ## X-Content-Type-Options: avoid MIME type sniffing
                                                 add_header X-Content-Type-Options nosniff;
                                                 ## Strict Transport Security (HSTS): Yes
                                                 add_header Strict-Transport-Security "max-age=31536000; includeSubdomains; preload";
                                                 ## Enables the Cross-site scripting (XSS) filter in browsers.
                                                 add_header X-XSS-Protection "1; mode=block";
                                                 ## Content-Security-Policy (CSP): FIXME
                                                 # add_header Content-Security-Policy "default-src 'self'; script-src 'self'; object-src 'none'; base-uri 'none'; require-trusted-types-for 'script'; frame-ancestors 'self';";


                                                 location / {
                                                     proxy_pass https://dashboard_servers;
                                                     proxy_next_upstream error timeout invalid_header http_500 http_502 http_503 http_504;
                                                 }

                                                 location /grafana {
                                                     proxy_pass https://grafana_servers;
                                                     # clear any Authorization header as Prometheus and Alertmanager are using basic-auth browser
                                                     # will send this header if Grafana is running on the same node as one of those services
                                                     proxy_set_header Authorization "";
                                                     proxy_buffering off;
                                                 }

                                                 location /prometheus {
                                                     proxy_pass https://prometheus_servers;

                                                     proxy_ssl_certificate /etc/nginx/ssl/nginx_internal.crt;
                                                     proxy_ssl_certificate_key /etc/nginx/ssl/nginx_internal.key;
                                                     proxy_ssl_trusted_certificate /etc/nginx/ssl/ca.crt;
                                                     proxy_ssl_verify on;
                                                     proxy_ssl_verify_depth 2;
                                                 }

                                                 location /alertmanager {
                                                     proxy_pass https://alertmanager_servers;

                                                     proxy_ssl_certificate /etc/nginx/ssl/nginx_internal.crt;
                                                     proxy_ssl_certificate_key /etc/nginx/ssl/nginx_internal.key;
                                                     proxy_ssl_trusted_certificate /etc/nginx/ssl/ca.crt;
                                                     proxy_ssl_verify on;
                                                     proxy_ssl_verify_depth 2;
                                                 }
                                             }"""),
                    "nginx_internal_server.conf": dedent("""
                                             server {
                                                 ssl_client_certificate /etc/nginx/ssl/ca.crt;
                                                 ssl_verify_client on;

                                                 listen              29443 ssl;
                                                 listen              [::]:29443 ssl;
                                                 ssl_certificate     /etc/nginx/ssl/nginx_internal.crt;
                                                 ssl_certificate_key /etc/nginx/ssl/nginx_internal.key;
                                                 ssl_protocols       TLSv1.3;
                                                 # from:  https://ssl-config.mozilla.org/#server=nginx
                                                 ssl_ciphers         ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES256-GCM-SHA384:DHE-RSA-CHACHA20-POLY1305;
                                                 ssl_prefer_server_ciphers on;

                                                 location /internal/sd {
                                                     rewrite ^/internal/(.*) /$1 break;
                                                     proxy_pass https://service_discovery_servers;
                                                     proxy_next_upstream error timeout invalid_header http_500 http_502 http_503 http_504;
                                                 }

                                                 location /internal/dashboard {
                                                     rewrite ^/internal/dashboard/(.*) /$1 break;
                                                     proxy_pass https://dashboard_servers;
                                                     proxy_next_upstream error timeout invalid_header http_500 http_502 http_503 http_504;
                                                 }

                                                 location /internal/grafana {
                                                     rewrite ^/internal/grafana/(.*) /$1 break;
                                                     proxy_pass https://grafana_servers;
                                                 }

                                                 location /internal/prometheus {
                                                     rewrite ^/internal/prometheus/(.*) /prometheus/$1 break;
                                                     proxy_pass https://prometheus_servers;

                                                     proxy_ssl_certificate /etc/nginx/ssl/nginx_internal.crt;
                                                     proxy_ssl_certificate_key /etc/nginx/ssl/nginx_internal.key;
                                                     proxy_ssl_trusted_certificate /etc/nginx/ssl/ca.crt;
                                                     proxy_ssl_verify on;
                                                     proxy_ssl_verify_depth 2;
                                                 }

                                                 location /internal/alertmanager {
                                                     rewrite ^/internal/alertmanager/(.*) /alertmanager/$1 break;
                                                     proxy_pass https://alertmanager_servers;

                                                     proxy_ssl_certificate /etc/nginx/ssl/nginx_internal.crt;
                                                     proxy_ssl_certificate_key /etc/nginx/ssl/nginx_internal.key;
                                                     proxy_ssl_trusted_certificate /etc/nginx/ssl/ca.crt;
                                                     proxy_ssl_verify on;
                                                     proxy_ssl_verify_depth 2;
                                                 }
                                             }"""),
                    "nginx_internal.crt": f"{ceph_generated_cert}",
                    "nginx_internal.key": f"{ceph_generated_key}",
                    "ca.crt": f"{cephadm_root_ca}",
                    "nginx.crt": f"{ceph_generated_cert}",
                    "nginx.key": f"{ceph_generated_key}",
                }
            }
        }

        with with_host(cephadm_module, 'ceph-node'):
            with with_service(cephadm_module, spec):
                _run_cephadm.assert_called_with(
                    'ceph-node',
                    'mgmt-gateway.ceph-node',
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps(expected),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.services.mgmt_gateway.MgmtGatewayService.get_service_endpoints")
    @patch("cephadm.services.mgmt_gateway.MgmtGatewayService.get_service_discovery_endpoints")
    @patch("cephadm.services.cephadmservice.CephadmService.get_certificates",
           lambda instance, dspec, ips=None: TLSCredentials(ceph_generated_cert, ceph_generated_key))
    @patch("cephadm.services.mgmt_gateway.MgmtGatewayService.get_self_signed_certificates_with_label",
           lambda instance, svc_spec, dspec, label, ip: TLSCredentials(ceph_generated_cert, ceph_generated_key))
    @patch("cephadm.module.CephadmOrchestrator.get_mgr_ip", lambda _: '::1')
    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: cephadm_root_ca)
    @patch("cephadm.services.mgmt_gateway.get_dashboard_endpoints", lambda _: (["ceph-node-2:8443", "ceph-node-2:8443"], "https"))
    def test_mgmt_gateway_config_with_auth(self,
                                           get_service_discovery_endpoints_mock: List[str],
                                           get_service_endpoints_mock: List[str],
                                           _run_cephadm,
                                           cephadm_module: CephadmOrchestrator):

        def get_services_endpoints(name):
            if name == 'prometheus':
                return ["192.168.100.100:9095", "192.168.100.101:9095"]
            elif name == 'grafana':
                return ["ceph-node-2:3000", "ceph-node-2:3000"]
            elif name == 'alertmanager':
                return ["192.168.100.100:9093", "192.168.100.102:9093"]
            elif name == 'oauth2-proxy':
                return ["192.168.100.101:4180", "192.168.100.102:4180"]
            return []

        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        get_service_endpoints_mock.side_effect = get_services_endpoints
        get_service_discovery_endpoints_mock.side_effect = lambda: ["ceph-node-0:8765", "ceph-node-2:8765"]

        server_port = 5555
        spec = MgmtGatewaySpec(port=server_port,
                               ssl_cert=ceph_generated_cert,
                               ssl_key=ceph_generated_key,
                               enable_auth=True)

        expected = {
            "fsid": "fsid",
            "name": "mgmt-gateway.ceph-node",
            "image": "",
            "deploy_arguments": [],
            "params": {"tcp_ports": [server_port]},
            "meta": {
                "service_name": "mgmt-gateway",
                "ports": [server_port],
                "ip": None,
                "deployed_by": [],
                "rank": None,
                "rank_generation": None,
                "extra_container_args": None,
                "extra_entrypoint_args": None
            },
            "config_blobs": {
                "files": {
                    "nginx.conf": dedent("""
                                         # This file is generated by cephadm.
                                         worker_rlimit_nofile 8192;

                                         events {
                                             worker_connections 4096;
                                         }

                                         http {

                                             #access_log /dev/stdout;
                                             error_log /dev/stderr warn;
                                             client_header_buffer_size 32K;
                                             large_client_header_buffers 4 32k;
                                             proxy_busy_buffers_size 512k;
                                             proxy_buffers 4 512k;
                                             proxy_buffer_size 256K;
                                             proxy_headers_hash_max_size 1024;
                                             proxy_headers_hash_bucket_size 128;

                                             upstream oauth2_proxy_servers {
                                              server 192.168.100.101:4180;
                                              server 192.168.100.102:4180;
                                             }

                                             upstream service_discovery_servers {
                                              server ceph-node-0:8765;
                                              server ceph-node-2:8765;
                                             }

                                             upstream dashboard_servers {
                                              server ceph-node-2:8443;
                                              server ceph-node-2:8443;
                                             }

                                             upstream grafana_servers {
                                              server ceph-node-2:3000;
                                              server ceph-node-2:3000;
                                             }

                                             upstream prometheus_servers {
                                              ip_hash;
                                              server 192.168.100.100:9095;
                                              server 192.168.100.101:9095;
                                             }

                                             upstream alertmanager_servers {
                                              server 192.168.100.100:9093;
                                              server 192.168.100.102:9093;
                                             }

                                             include /etc/nginx_external_server.conf;
                                             include /etc/nginx_internal_server.conf;
                                         }"""),
                    "nginx_external_server.conf": dedent("""
                                             server {
                                                 listen                    5555 ssl;
                                                 listen                    [::]:5555 ssl;
                                                 ssl_certificate            /etc/nginx/ssl/nginx.crt;
                                                 ssl_certificate_key /etc/nginx/ssl/nginx.key;
                                                 ssl_protocols            TLSv1.3;
                                                 # from:  https://ssl-config.mozilla.org/#server=nginx
                                                 ssl_ciphers              ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES256-GCM-SHA384:DHE-RSA-CHACHA20-POLY1305;

                                                 # Only return Nginx in server header, no extra info will be provided
                                                 server_tokens             off;

                                                 # Perfect Forward Secrecy(PFS) is frequently compromised without this
                                                 ssl_prefer_server_ciphers on;

                                                 # Enable SSL session caching for improved performance
                                                 ssl_session_tickets       off;
                                                 ssl_session_timeout       1d;
                                                 ssl_session_cache         shared:SSL:10m;

                                                 # OCSP stapling
                                                 ssl_stapling              on;
                                                 ssl_stapling_verify       on;
                                                 resolver_timeout 5s;

                                                 # Security headers
                                                 ## X-Content-Type-Options: avoid MIME type sniffing
                                                 add_header X-Content-Type-Options nosniff;
                                                 ## Strict Transport Security (HSTS): Yes
                                                 add_header Strict-Transport-Security "max-age=31536000; includeSubdomains; preload";
                                                 ## Enables the Cross-site scripting (XSS) filter in browsers.
                                                 add_header X-XSS-Protection "1; mode=block";
                                                 ## Content-Security-Policy (CSP): FIXME
                                                 # add_header Content-Security-Policy "default-src 'self'; script-src 'self'; object-src 'none'; base-uri 'none'; require-trusted-types-for 'script'; frame-ancestors 'self';";

                                                 location /oauth2/ {
                                                     proxy_pass https://oauth2_proxy_servers;
                                                     proxy_set_header Host $host;
                                                     proxy_set_header X-Real-IP $remote_addr;
                                                     proxy_set_header X-Scheme $scheme;
                                                     # Check for original-uri header
                                                     proxy_set_header X-Auth-Request-Redirect $scheme://$host$request_uri;
                                                 }

                                                 location = /oauth2/auth {
                                                     internal;
                                                     proxy_pass https://oauth2_proxy_servers;
                                                     proxy_set_header Host $host;
                                                     proxy_set_header X-Real-IP $remote_addr;
                                                     proxy_set_header X-Scheme $scheme;
                                                     # nginx auth_request includes headers but not body
                                                     proxy_set_header Content-Length "";
                                                     proxy_pass_request_body off;
                                                 }

                                                 location / {
                                                     proxy_pass https://dashboard_servers;
                                                     proxy_next_upstream error timeout invalid_header http_500 http_502 http_503 http_504;
                                                     auth_request /oauth2/auth;
                                                     error_page 401 = /oauth2/sign_in;

                                                     auth_request_set $email $upstream_http_x_auth_request_email;
                                                     proxy_set_header X-Email $email;

                                                     auth_request_set $groups $upstream_http_x_auth_request_groups;
                                                     proxy_set_header X-User-Groups $groups;

                                                     auth_request_set $user $upstream_http_x_auth_request_user;
                                                     proxy_set_header X-User $user;

                                                     auth_request_set $token $upstream_http_x_auth_request_access_token;
                                                     proxy_set_header X-Access-Token $token;

                                                     auth_request_set $auth_cookie $upstream_http_set_cookie;
                                                     add_header Set-Cookie $auth_cookie;

                                                     proxy_set_header Host $host;
                                                     proxy_set_header X-Real-IP $remote_addr;
                                                     proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
                                                     proxy_set_header X-Forwarded-Host $host:80;
                                                     proxy_set_header X-Forwarded-Port 80;
                                                     proxy_set_header X-Forwarded-Server $host;
                                                     proxy_set_header X-Forwarded-Groups $groups;

                                                     proxy_http_version 1.1;

                                                     proxy_set_header X-Forwarded-Proto "https";
                                                     proxy_ssl_verify off;
                                                 }

                                                 location /grafana {
                                                     proxy_pass https://grafana_servers;
                                                     # clear any Authorization header as Prometheus and Alertmanager are using basic-auth browser
                                                     # will send this header if Grafana is running on the same node as one of those services
                                                     proxy_set_header Authorization "";
                                                     proxy_buffering off;
                                                     auth_request /oauth2/auth;
                                                     error_page 401 = /oauth2/sign_in;

                                                     proxy_set_header X-Original-URI "/";

                                                     auth_request_set $user $upstream_http_x_auth_request_user;
                                                     auth_request_set $email $upstream_http_x_auth_request_email;
                                                     proxy_set_header X-WEBAUTH-USER $user;
                                                     proxy_set_header X-WEBAUTH-EMAIL $email;

                                                     # Pass role header to Grafana
                                                     proxy_set_header X-WEBAUTH-ROLE $http_x_auth_request_role;

                                                     proxy_set_header Host $host;
                                                     proxy_set_header X-Real-IP $remote_addr;
                                                     proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
                                                     proxy_set_header X-Forwarded-Proto $scheme;

                                                     auth_request_set $auth_cookie $upstream_http_set_cookie;
                                                     add_header Set-Cookie $auth_cookie;

                                                     proxy_set_header X-Forwarded-Proto $scheme;
                                                 }

                                                 location /prometheus {
                                                     proxy_pass https://prometheus_servers;

                                                     proxy_ssl_certificate /etc/nginx/ssl/nginx_internal.crt;
                                                     proxy_ssl_certificate_key /etc/nginx/ssl/nginx_internal.key;
                                                     proxy_ssl_trusted_certificate /etc/nginx/ssl/ca.crt;
                                                     proxy_ssl_verify on;
                                                     proxy_ssl_verify_depth 2;
                                                     auth_request /oauth2/auth;
                                                     error_page 401 = /oauth2/sign_in;

                                                     auth_request_set $user $upstream_http_x_auth_request_user;
                                                     auth_request_set $email $upstream_http_x_auth_request_email;
                                                     proxy_set_header X-User $user;
                                                     proxy_set_header X-Email $email;

                                                     auth_request_set $auth_cookie $upstream_http_set_cookie;
                                                     add_header Set-Cookie $auth_cookie;
                                                 }

                                                 location /alertmanager {
                                                     proxy_pass https://alertmanager_servers;

                                                     proxy_ssl_certificate /etc/nginx/ssl/nginx_internal.crt;
                                                     proxy_ssl_certificate_key /etc/nginx/ssl/nginx_internal.key;
                                                     proxy_ssl_trusted_certificate /etc/nginx/ssl/ca.crt;
                                                     proxy_ssl_verify on;
                                                     proxy_ssl_verify_depth 2;
                                                     auth_request /oauth2/auth;
                                                     error_page 401 = /oauth2/sign_in;

                                                     auth_request_set $user $upstream_http_x_auth_request_user;
                                                     auth_request_set $email $upstream_http_x_auth_request_email;
                                                     proxy_set_header X-User $user;
                                                     proxy_set_header X-Email $email;

                                                     auth_request_set $auth_cookie $upstream_http_set_cookie;
                                                     add_header Set-Cookie $auth_cookie;
                                                 }
                                             }"""),
                    "nginx_internal_server.conf": dedent("""
                                             server {
                                                 ssl_client_certificate /etc/nginx/ssl/ca.crt;
                                                 ssl_verify_client on;

                                                 listen              29443 ssl;
                                                 listen              [::]:29443 ssl;
                                                 ssl_certificate     /etc/nginx/ssl/nginx_internal.crt;
                                                 ssl_certificate_key /etc/nginx/ssl/nginx_internal.key;
                                                 ssl_protocols       TLSv1.3;
                                                 # from:  https://ssl-config.mozilla.org/#server=nginx
                                                 ssl_ciphers         ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES256-GCM-SHA384:DHE-RSA-CHACHA20-POLY1305;
                                                 ssl_prefer_server_ciphers on;

                                                 location /internal/sd {
                                                     rewrite ^/internal/(.*) /$1 break;
                                                     proxy_pass https://service_discovery_servers;
                                                     proxy_next_upstream error timeout invalid_header http_500 http_502 http_503 http_504;
                                                 }

                                                 location /internal/dashboard {
                                                     rewrite ^/internal/dashboard/(.*) /$1 break;
                                                     proxy_pass https://dashboard_servers;
                                                     proxy_next_upstream error timeout invalid_header http_500 http_502 http_503 http_504;
                                                 }

                                                 location /internal/grafana {
                                                     rewrite ^/internal/grafana/(.*) /$1 break;
                                                     proxy_pass https://grafana_servers;
                                                 }

                                                 location /internal/prometheus {
                                                     rewrite ^/internal/prometheus/(.*) /prometheus/$1 break;
                                                     proxy_pass https://prometheus_servers;

                                                     proxy_ssl_certificate /etc/nginx/ssl/nginx_internal.crt;
                                                     proxy_ssl_certificate_key /etc/nginx/ssl/nginx_internal.key;
                                                     proxy_ssl_trusted_certificate /etc/nginx/ssl/ca.crt;
                                                     proxy_ssl_verify on;
                                                     proxy_ssl_verify_depth 2;
                                                 }

                                                 location /internal/alertmanager {
                                                     rewrite ^/internal/alertmanager/(.*) /alertmanager/$1 break;
                                                     proxy_pass https://alertmanager_servers;

                                                     proxy_ssl_certificate /etc/nginx/ssl/nginx_internal.crt;
                                                     proxy_ssl_certificate_key /etc/nginx/ssl/nginx_internal.key;
                                                     proxy_ssl_trusted_certificate /etc/nginx/ssl/ca.crt;
                                                     proxy_ssl_verify on;
                                                     proxy_ssl_verify_depth 2;
                                                 }
                                             }"""),
                    "nginx_internal.crt": f"{ceph_generated_cert}",
                    "nginx_internal.key": f"{ceph_generated_key}",
                    "ca.crt": f"{cephadm_root_ca}",
                    "nginx.crt": f"{ceph_generated_cert}",
                    "nginx.key": f"{ceph_generated_key}",
                }
            }
        }

        with with_host(cephadm_module, 'ceph-node'):
            with with_service(cephadm_module, spec):
                _run_cephadm.assert_called_with(
                    'ceph-node',
                    'mgmt-gateway.ceph-node',
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps(expected),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.services.mgmt_gateway.MgmtGatewayService.get_service_endpoints")
    @patch("cephadm.services.mgmt_gateway.MgmtGatewayService.get_service_discovery_endpoints")
    @patch("cephadm.services.mgmt_gateway.MgmtGatewayService.get_self_signed_certificates_with_label")
    @patch("cephadm.services.cephadmservice.CephadmService.get_certificates",
           lambda instance, dspec, ips=None: TLSCredentials(ceph_generated_cert, ceph_generated_key))
    @patch("cephadm.module.CephadmOrchestrator.get_mgr_ip", lambda _: '::1')
    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: cephadm_root_ca)
    @patch("cephadm.services.mgmt_gateway.get_dashboard_endpoints",
           lambda _: (["ceph-node-2:8443", "ceph-node-2:8443"], "https"))
    def test_mgmt_gateway_internal_cert_san_includes_vip(
        self,
        get_self_signed_mock,
        get_service_discovery_endpoints_mock,
        get_service_endpoints_mock,
        _run_cephadm,
        cephadm_module: CephadmOrchestrator,
    ):
        vip = "10.0.0.200"

        def get_services_endpoints(name):
            if name == 'prometheus':
                return ["192.168.100.100:9095", "192.168.100.101:9095"]
            if name == 'grafana':
                return ["ceph-node-2:3000", "ceph-node-2:3000"]
            if name == 'alertmanager':
                return ["192.168.100.100:9093", "192.168.100.102:9093"]
            if name == 'oauth2-proxy':
                return []
            return []

        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        get_service_endpoints_mock.side_effect = get_services_endpoints
        get_service_discovery_endpoints_mock.return_value = ["ceph-node-0:8765", "ceph-node-2:8765"]
        get_self_signed_mock.return_value = TLSCredentials(ceph_generated_cert, ceph_generated_key)

        server_port = 5555
        spec = MgmtGatewaySpec(
            port=server_port,
            virtual_ip=vip,  # HA mode
            ssl_cert=ceph_generated_cert,
            ssl_key=ceph_generated_key,
        )

        with with_host(cephadm_module, 'ceph-node'):
            with with_service(cephadm_module, spec):
                # Ensure VIP was used when minting the internal cert (so it goes into SANs)
                # get_self_signed_certificates_with_label(svc_spec, daemon_spec, label, ip)
                args, _ = get_self_signed_mock.call_args
                assert args[2] == 'internal'
                assert args[3] == vip
                deployed = json.loads(_run_cephadm.call_args.kwargs['stdin'])
                assert deployed['config_blobs']['files']['nginx_internal.crt'] == ceph_generated_cert

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.services.mgmt_gateway.MgmtGatewayService.get_service_endpoints")
    @patch("cephadm.services.cephadmservice.CephadmService.get_certificates",
           lambda instance, dspec, ips=None: TLSCredentials(ceph_generated_cert, ceph_generated_key))
    @patch("cephadm.services.mgmt_gateway.MgmtGatewayService.get_self_signed_certificates_with_label",
           lambda instance, svc_spec, dspec, label, ip: TLSCredentials(ceph_generated_cert, ceph_generated_key))
    @patch("cephadm.module.CephadmOrchestrator.get_mgr_ip", lambda _: '::1')
    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: cephadm_root_ca)
    @patch("cephadm.services.mgmt_gateway.get_dashboard_endpoints", lambda _: (["ceph-node-2:8443", "ceph-node-2:8443"], "https"))
    def test_oauth2_proxy_service(self, get_service_endpoints_mock, _run_cephadm, cephadm_module):
        self.oauth2_proxy_service_common(get_service_endpoints_mock, _run_cephadm, cephadm_module, virtual_ip=None)

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.services.mgmt_gateway.MgmtGatewayService.get_service_endpoints")
    @patch("cephadm.services.cephadmservice.CephadmService.get_certificates",
           lambda instance, dspec, ips=None: TLSCredentials(ceph_generated_cert, ceph_generated_key))
    @patch("cephadm.services.oauth2_proxy.OAuth2ProxyService.get_certificates",
           lambda instance, dspec, ips=None: TLSCredentials(ceph_generated_cert, ceph_generated_key))
    @patch("cephadm.services.mgmt_gateway.MgmtGatewayService.get_self_signed_certificates_with_label",
           lambda instance, svc_spec, dspec, label, ip: TLSCredentials(ceph_generated_cert, ceph_generated_key))
    @patch("cephadm.module.CephadmOrchestrator.get_mgr_ip", lambda _: '::1')
    @patch('cephadm.cert_mgr.CertMgr.get_root_ca', lambda instance: cephadm_root_ca)
    @patch("cephadm.services.mgmt_gateway.get_dashboard_endpoints", lambda _: (["ceph-node-2:8443", "ceph-node-2:8443"], "https"))
    def test_oauth2_proxy_service_with_ha(self, get_service_endpoints_mock, _run_cephadm, cephadm_module):
        self.oauth2_proxy_service_common(get_service_endpoints_mock, _run_cephadm, cephadm_module, virtual_ip="192.168.100.200")

    def oauth2_proxy_service_common(self, get_service_endpoints_mock, _run_cephadm, cephadm_module: CephadmOrchestrator, virtual_ip=None):
        def get_services_endpoints(name):
            if name == 'prometheus':
                return ["192.168.100.100:9095", "192.168.100.101:9095"]
            elif name == 'grafana':
                return ["ceph-node-2:3000", "ceph-node-2:3000"]
            elif name == 'alertmanager':
                return ["192.168.100.100:9093", "192.168.100.102:9093"]
            return []

        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        get_service_endpoints_mock.side_effect = get_services_endpoints

        server_port = 5555
        mgmt_gw_spec = MgmtGatewaySpec(port=server_port,
                                       ssl_cert=ceph_generated_cert,
                                       ssl_key=ceph_generated_key,
                                       enable_auth=True,
                                       virtual_ip=virtual_ip)

        allowed_domain = '192.168.100.1:8080'
        oauth2_spec = OAuth2ProxySpec(provider_display_name='my_idp_provider',
                                      client_id='my_client_id',
                                      client_secret='my_client_secret',
                                      oidc_issuer_url='http://192.168.10.10:8888/dex',
                                      cookie_secret='kbAEM9opAmuHskQvt0AW8oeJRaOM2BYy5Loba0kZ0SQ=',
                                      ssl_cert=ceph_generated_cert,
                                      ssl_key=ceph_generated_key,
                                      allowlist_domains=[allowed_domain])

        whitelist_domains = f"{allowed_domain},1::4,ceph-node" if virtual_ip is None else f"{allowed_domain},{virtual_ip},1::4,ceph-node"
        redirect_url = f"https://{virtual_ip if virtual_ip else 'host_fqdn'}:5555/oauth2/callback"
        expected = {
            "fsid": "fsid",
            "name": "oauth2-proxy.ceph-node",
            "image": "",
            "deploy_arguments": [],
            "params": {"tcp_ports": [4180]},
            "meta": {
                "service_name": "oauth2-proxy",
                "ports": [4180],
                "ip": None,
                "deployed_by": [],
                "rank": None,
                "rank_generation": None,
                "extra_container_args": None,
                "extra_entrypoint_args": None
            },
            "config_blobs": {
                "files": {
                    "oauth2-proxy.conf": dedent(f"""
                                         # Listen on port 4180 for incoming HTTP traffic.
                                         https_address= "0.0.0.0:4180"

                                         skip_provider_button= true
                                         skip_jwt_bearer_tokens= true

                                         # OIDC provider configuration.
                                         provider= "oidc"
                                         provider_display_name= "my_idp_provider"
                                         client_id= "my_client_id"
                                         client_secret= "my_client_secret"
                                         oidc_issuer_url= "http://192.168.10.10:8888/dex"
                                         redirect_url= "{redirect_url}"


                                         # following configuration is needed to avoid getting Forbidden
                                         # when using chrome like browsers as they handle 3rd party cookies
                                         # more strictly than Firefox
                                         cookie_samesite= "none"
                                         cookie_secure= true
                                         cookie_expire= "5h"
                                         cookie_refresh= "2h"

                                         pass_access_token= true
                                         pass_authorization_header= true
                                         pass_basic_auth= true
                                         pass_user_headers= true
                                         set_xauthrequest= true

                                         # Secret value for encrypting cookies.
                                         cookie_secret= "kbAEM9opAmuHskQvt0AW8oeJRaOM2BYy5Loba0kZ0SQ="
                                         email_domains= "*"
                                         whitelist_domains= "{whitelist_domains}\""""),
                    "oauth2-proxy.crt": f"{ceph_generated_cert}",
                    "oauth2-proxy.key": f"{ceph_generated_key}",
                }
            }
        }

        with with_host(cephadm_module, 'ceph-node'):
            with with_service(cephadm_module, mgmt_gw_spec) as _, with_service(cephadm_module, oauth2_spec):
                _run_cephadm.assert_called_with(
                    'ceph-node',
                    'oauth2-proxy.ceph-node',
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps(expected),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_mgmt_gateway_default_port_is_443_when_unspecified(
        self,
        _run_cephadm,
        cephadm_module: CephadmOrchestrator,
    ):
        """
        When no --port is provided and the spec has no port field,
        the mgmt-gateway daemon spec must use port 443 so that
        firewalld can open the correct port.
        """

        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        # NOTE: no port passed here, let's test the defaults
        spec = MgmtGatewaySpec()
        with with_host(cephadm_module, 'ceph-node'):
            with with_service(cephadm_module, spec):
                HTTPS_PORT = 443
                # Inspect the daemon spec passed to cephadm
                deployed = json.loads(_run_cephadm.call_args.kwargs['stdin'])
                # The default port must be 443 (from get_port_start)
                assert 'tcp_ports' in deployed['params']
                assert deployed['params']['tcp_ports'] == [HTTPS_PORT]
                assert deployed['meta']['ports'] == [HTTPS_PORT]
