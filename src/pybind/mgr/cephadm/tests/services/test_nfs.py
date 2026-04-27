import contextlib
from unittest.mock import MagicMock, patch, ANY

import pytest

from cephadm.services.service_registry import service_registry
from cephadm.services.cephadmservice import CephadmDaemonDeploySpec
from cephadm.module import CephadmOrchestrator
from ceph.deployment.service_spec import (
    NFSServiceSpec,
    PlacementSpec,
    RGWSpec,
    IngressSpec,
    SpecValidationError,
)
from cephadm.tests.fixtures import with_host, with_service, wait, async_side_effect


cephadm_root_ca = """-----BEGIN CERTIFICATE-----\nMIIE7DCCAtSgAwIBAgIUE8b2zZ64geu2ns3Zfn3/4L+Cf6MwDQYJKoZIhvcNAQEL\nBQAwFzEVMBMGA1UEAwwMY2VwaGFkbS1yb290MB4XDTI0MDYyNjE0NDA1M1oXDTM0\nMDYyNzE0NDA1M1owFzEVMBMGA1UEAwwMY2VwaGFkbS1yb290MIICIjANBgkqhkiG\n9w0BAQEFAAOCAg8AMIICCgKCAgEAsZRJsdtTr9GLG1lWFql5SGc46ldFanNJd1Gl\nqXq5vgZVKRDTmNgAb/XFuNEEmbDAXYIRZolZeYKMHfn0pouPRSel0OsC6/02ZUOW\nIuN89Wgo3IYleCFpkVIumD8URP3hwdu85plRxYZTtlruBaTRH38lssyCqxaOdEt7\nAUhvYhcMPJThB17eOSQ73mb8JEC83vB47fosI7IhZuvXvRSuZwUW30rJanWNhyZq\neS2B8qw2RSO0+77H6gA4ftBnitfsE1Y8/F9Z/f92JOZuSMQXUB07msznPbRJia3f\nueO8gOc32vxd1A1/Qzp14uX34yEGY9ko2lW226cZO29IVUtXOX+LueQttwtdlpz8\ne6Npm09pXhXAHxV/OW3M28MdXmobIqT/m9MfkeAErt5guUeC5y8doz6/3VQRjFEn\nRpN0WkblgnNAQ3DONPc+Qd9Fi/wZV2X7bXoYpNdoWDsEOiE/eLmhG1A2GqU/mneP\nzQ6u79nbdwTYpwqHpa+PvusXeLfKauzI8lLUJotdXy9EK8iHUofibB61OljYye6B\nG3b8C4QfGsw8cDb4APZd/6AZYyMx/V3cGZ+GcOV7WvsC8k7yx5Uqasm/kiGQ3EZo\nuNenNEYoGYrjb8D/8QzqNUTwlEh27/ps80tO7l2GGTvWVZL0PRZbmLDvO77amtOf\nOiRXMoUCAwEAAaMwMC4wGwYDVR0RBBQwEocQAAAAAAAAAAAAAAAAAAAAATAPBgNV\nHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4ICAQAxwzX5AhYEWhTV4VUwUj5+\nqPdl4Q2tIxRokqyE+cDxoSd+6JfGUefUbNyBxDt0HaBq8obDqqrbcytxnn7mpnDu\nhtiauY+I4Amt7hqFOiFA4cCLi2mfok6g2vL53tvhd9IrsfflAU2wy7hL76Ejm5El\nA+nXlkJwps01Whl9pBkUvIbOn3pXX50LT4hb5zN0PSu957rjd2xb4HdfuySm6nW4\n4GxtVWfmGA6zbC4XMEwvkuhZ7kD2qjkAguGDF01uMglkrkCJT3OROlNBuSTSBGqt\ntntp5VytHvb7KTF7GttM3ha8/EU2KYaHM6WImQQTrOfiImAktOk4B3lzUZX3HYIx\n+sByO4P4dCvAoGz1nlWYB2AvCOGbKf0Tgrh4t4jkiF8FHTXGdfvWmjgi1pddCNAy\nn65WOCmVmLZPERAHOk1oBwqyReSvgoCFo8FxbZcNxJdlhM0Z6hzKggm3O3Dl88Xl\n5euqJjh2STkBW8Xuowkg1TOs5XyWvKoDFAUzyzeLOL8YSG+gXV22gPTUaPSVAqdb\nwd0Fx2kjConuC5bgTzQHs8XWA930U3XWZraj21Vaa8UxlBLH4fUro8H5lMSYlZNE\nJHRNW8BkznAClaFSDG3dybLsrzrBFAu/Qb5zVkT1xyq0YkepGB7leXwq6vjWA5Pw\nmZbKSphWfh0qipoqxqhfkw==\n-----END CERTIFICATE-----\n"""

ceph_generated_cert = """-----BEGIN CERTIFICATE-----\nMIICxjCCAa4CEQDIZSujNBlKaLJzmvntjukjMA0GCSqGSIb3DQEBDQUAMCExDTAL\nBgNVBAoMBENlcGgxEDAOBgNVBAMMB2NlcGhhZG0wHhcNMjIwNzEzMTE0NzA3WhcN\nMzIwNzEwMTE0NzA3WjAhMQ0wCwYDVQQKDARDZXBoMRAwDgYDVQQDDAdjZXBoYWRt\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyyMe4DMA+MeYK7BHZMHB\nq7zjliEOcNgxomjU8qbf5USF7Mqrf6+/87XWqj4pCyAW8x0WXEr6A56a+cmBVmt+\nqtWDzl020aoId6lL5EgLLn6/kMDCCJLq++Lg9cEofMSvcZh+lY2f+1p+C+00xent\nrLXvXGOilAZWaQfojT2BpRnNWWIFbpFwlcKrlg2G0cFjV5c1m6a0wpsQ9JHOieq0\nSvwCixajwq3CwAYuuiU1wjI4oJO4Io1+g8yB3nH2Mo/25SApCxMXuXh4kHLQr/T4\n4hqisvG4uJYgKMcSIrWj5o25mclByGi1UI/kZkCUES94i7Z/3ihx4Bad0AMs/9tw\nFwIDAQABMA0GCSqGSIb3DQEBDQUAA4IBAQAf+pwz7Gd7mDwU2LY0TQXsK6/8KGzh\nHuX+ErOb8h5cOAbvCnHjyJFWf6gCITG98k9nxU9NToG0WYuNm/max1y/54f0dtxZ\npUo6KSNl3w6iYCfGOeUIj8isi06xMmeTgMNzv8DYhDt+P2igN6LenqWTVztogkiV\nxQ5ZJFFLEw4sN0CXnrZX3t5ruakxLXLTLKeE0I91YJvjClSBGkVJq26wOKQNHMhx\npWxeydQ5EgPZY+Aviz5Dnxe8aB7oSSovpXByzxURSabOuCK21awW5WJCGNpmqhWK\nZzACBDEstccj57c4OGV0eayHJRsluVr2e9NHRINZA3qdB37e6gsI1xHo\n-----END CERTIFICATE-----\n"""

ceph_generated_key = """-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDLIx7gMwD4x5gr\nsEdkwcGrvOOWIQ5w2DGiaNTypt/lRIXsyqt/r7/ztdaqPikLIBbzHRZcSvoDnpr5\nyYFWa36q1YPOXTbRqgh3qUvkSAsufr+QwMIIkur74uD1wSh8xK9xmH6VjZ/7Wn4L\n7TTF6e2ste9cY6KUBlZpB+iNPYGlGc1ZYgVukXCVwquWDYbRwWNXlzWbprTCmxD0\nkc6J6rRK/AKLFqPCrcLABi66JTXCMjigk7gijX6DzIHecfYyj/blICkLExe5eHiQ\nctCv9PjiGqKy8bi4liAoxxIitaPmjbmZyUHIaLVQj+RmQJQRL3iLtn/eKHHgFp3Q\nAyz/23AXAgMBAAECggEAVoTB3Mm8azlPlaQB9GcV3tiXslSn+uYJ1duCf0sV52dV\nBzKW8s5fGiTjpiTNhGCJhchowqxoaew+o47wmGc2TvqbpeRLuecKrjScD0GkCYyQ\neM2wlshEbz4FhIZdgS6gbuh9WaM1dW/oaZoBNR5aTYo7xYTmNNeyLA/jO2zr7+4W\n5yES1lMSBXpKk7bDGKYY4bsX2b5RLr2Grh2u2bp7hoLABCEvuu8tSQdWXLEXWpXo\njwmV3hc6tabypIa0mj2Dmn2Dmt1ppSO0AZWG/WAizN3f4Z0r/u9HnbVrVmh0IEDw\n3uf2LP5o3msG9qKCbzv3lMgt9mMr70HOKnJ8ohMSKQKBgQDLkNb+0nr152HU9AeJ\nvdz8BeMxcwxCG77iwZphZ1HprmYKvvXgedqWtS6FRU+nV6UuQoPUbQxJBQzrN1Qv\nwKSlOAPCrTJgNgF/RbfxZTrIgCPuK2KM8I89VZv92TSGi362oQA4MazXC8RAWjoJ\nSu1/PHzK3aXOfVNSLrOWvIYeZQKBgQD/dgT6RUXKg0UhmXj7ExevV+c7oOJTDlMl\nvLngrmbjRgPO9VxLnZQGdyaBJeRngU/UXfNgajT/MU8B5fSKInnTMawv/tW7634B\nw3v6n5kNIMIjJmENRsXBVMllDTkT9S7ApV+VoGnXRccbTiDapBThSGd0wri/CuwK\nNWK1YFOeywKBgEDyI/XG114PBUJ43NLQVWm+wx5qszWAPqV/2S5MVXD1qC6zgCSv\nG9NLWN1CIMimCNg6dm7Wn73IM7fzvhNCJgVkWqbItTLG6DFf3/DPODLx1wTMqLOI\nqFqMLqmNm9l1Nec0dKp5BsjRQzq4zp1aX21hsfrTPmwjxeqJZdioqy2VAoGAXR5X\nCCdSHlSlUW8RE2xNOOQw7KJjfWT+WAYoN0c7R+MQplL31rRU7dpm1bLLRBN11vJ8\nMYvlT5RYuVdqQSP6BkrX+hLJNBvOLbRlL+EXOBrVyVxHCkDe+u7+DnC4epbn+N8P\nLYpwqkDMKB7diPVAizIKTBxinXjMu5fkKDs5n+sCgYBbZheYKk5M0sIxiDfZuXGB\nkf4mJdEkTI1KUGRdCwO/O7hXbroGoUVJTwqBLi1tKqLLarwCITje2T200BYOzj82\nqwRkCXGtXPKnxYEEUOiFx9OeDrzsZV00cxsEnX0Zdj+PucQ/J3Cvd0dWUspJfLHJ\n39gnaegswnz9KMQAvzKFdg==\n-----END PRIVATE KEY-----\n"""


class TestNFS:
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.services.nfs.NFSService.fence_old_ranks", MagicMock())
    @patch("cephadm.services.nfs.NFSService.run_grace_tool", MagicMock())
    @patch("cephadm.services.nfs.NFSService.purge", MagicMock())
    @patch("cephadm.services.nfs.NFSService.create_rados_config_obj", MagicMock())
    def test_nfs_config_monitoring_ip(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test', addr='1.2.3.7'):
            cephadm_module.cache.update_host_networks('test', {
                '1.2.3.0/24': {
                    'if0': ['1.2.3.1']
                }
            })

            nfs_spec = NFSServiceSpec(service_id="foo", placement=PlacementSpec(hosts=['test']),
                                      monitoring_ip_addrs={'test': '1.2.3.1'})
            with with_service(cephadm_module, nfs_spec) as _:
                nfs_generated_conf, _ = service_registry.get_service('nfs').generate_config(
                    CephadmDaemonDeploySpec(host='test', daemon_id='foo.test.0.0', service_name=nfs_spec.service_name()))
                ganesha_conf = nfs_generated_conf['files']['ganesha.conf']
                assert "Monitoring_Addr = 1.2.3.1" in ganesha_conf

            nfs_spec = NFSServiceSpec(service_id="foo", placement=PlacementSpec(hosts=['test']),
                                      monitoring_networks=['1.2.3.0/24'])
            with with_service(cephadm_module, nfs_spec) as _:
                nfs_generated_conf, _ = service_registry.get_service('nfs').generate_config(
                    CephadmDaemonDeploySpec(host='test', daemon_id='foo.test.0.0', service_name=nfs_spec.service_name()))
                ganesha_conf = nfs_generated_conf['files']['ganesha.conf']
                assert "Monitoring_Addr = 1.2.3.1" in ganesha_conf

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.services.nfs.NFSService.fence_old_ranks", MagicMock())
    @patch("cephadm.services.nfs.NFSService.run_grace_tool", MagicMock())
    @patch("cephadm.services.nfs.NFSService.purge", MagicMock())
    @patch("cephadm.services.nfs.NFSService.create_rados_config_obj", MagicMock())
    def test_nfs_config_bind_addr(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'host1', addr='1.2.3.7'):
            cephadm_module.cache.update_host_networks('host1', {
                '1.2.3.0/24': {
                    'if0': ['1.2.3.7']
                }
            })

            nfs_spec = NFSServiceSpec(service_id="foo", placement=PlacementSpec(hosts=['host1']),
                                      ip_addrs={'host1': '1.2.3.7'})
            with with_service(cephadm_module, nfs_spec, status_running=True) as _:
                dds = wait(cephadm_module, cephadm_module.list_daemons())
                daemon_spec = CephadmDaemonDeploySpec.from_daemon_description(dds[0])
                nfs_generated_conf, _ = service_registry.get_service('nfs').generate_config(daemon_spec)
                ganesha_conf = nfs_generated_conf['files']['ganesha.conf']
                assert "Bind_addr = 1.2.3.7" in ganesha_conf

        with with_host(cephadm_module, 'host1', addr='1.2.3.7'):
            cephadm_module.cache.update_host_networks('host1', {
                '1.2.3.0/24': {
                    'if0': ['1.2.3.7']
                }
            })
            nfs_spec = NFSServiceSpec(service_id="foo", placement=PlacementSpec(hosts=['host1']),
                                      networks=['1.2.3.0/24'])
            with with_service(cephadm_module, nfs_spec, status_running=True) as _:
                dds = wait(cephadm_module, cephadm_module.list_daemons())
                daemon_spec = CephadmDaemonDeploySpec.from_daemon_description(dds[0])
                nfs_generated_conf, _ = service_registry.get_service('nfs').generate_config(daemon_spec)
                ganesha_conf = nfs_generated_conf['files']['ganesha.conf']
                assert "Bind_addr = 1.2.3.7" in ganesha_conf

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_ingress_without_haproxy_stats(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test', addr='1.2.3.7'):
            cephadm_module.cache.update_host_networks('test', {
                '1.2.3.0/24': {
                    'if0': [
                        '1.2.3.4',  # simulate already assigned VIP
                        '1.2.3.1',  # simulate interface IP
                    ]
                }
            })

            # the ingress backend
            s = RGWSpec(service_id="foo", placement=PlacementSpec(count=1),
                        rgw_frontend_type='beast')

            ispec = IngressSpec(service_type='ingress',
                                service_id='test',
                                backend_service='rgw.foo',
                                frontend_port=8089,
                                monitor_port=8999,
                                monitor_user='admin',
                                monitor_password='12345',
                                keepalived_password='12345',
                                virtual_interface_networks=['1.2.3.0/24'],
                                virtual_ip="1.2.3.4/32")
            with with_service(cephadm_module, s) as _, with_service(cephadm_module, ispec) as _:
                # generate the haproxy conf based on the specified spec
                haproxy_generated_conf = service_registry.get_service('ingress').haproxy_generate_config(
                    CephadmDaemonDeploySpec(host='test', daemon_id='ingress', service_name=ispec.service_name()))

                haproxy_expected_conf = {
                    'files':
                        {
                            'haproxy.cfg':
                                '# This file is generated by cephadm.'
                                '\nglobal\n    log         '
                                '127.0.0.1 local2\n    '
                                'chroot      /var/lib/haproxy\n    '
                                'pidfile     /var/lib/haproxy/haproxy.pid\n    '
                                'maxconn     8000\n    '
                                'daemon\n    '
                                'stats socket /var/lib/haproxy/stats\n'
                                '\ndefaults\n    '
                                'mode                    http\n    '
                                'log                     global\n    '
                                'option                  httplog\n    '
                                'option                  dontlognull\n    '
                                'option http-server-close\n    '
                                'option forwardfor       except 127.0.0.0/8\n    '
                                'option                  redispatch\n    '
                                'retries                 3\n    '
                                'timeout queue           20s\n    '
                                'timeout connect         5s\n    '
                                'timeout http-request    1s\n    '
                                'timeout http-keep-alive 5s\n    '
                                'timeout client          30s\n    '
                                'timeout server          30s\n    '
                                'timeout check           5s\n    '
                                'maxconn                 8000\n'
                                '\nfrontend stats\n'
                                '    mode http\n'
                                '    bind 1.2.3.4:8999\n'
                                '    bind 1.2.3.7:8999\n'
                                '    monitor-uri /health\n'
                                '\nfrontend frontend\n    '
                                'bind 1.2.3.4:8089\n    '
                                'default_backend backend\n\n'
                                'backend backend\n    '
                                'option forwardfor\n    '
                                'balance static-rr\n    '
                                'option httpchk HEAD / HTTP/1.0\n    '
                                'server '
                                + haproxy_generated_conf[1][0] + ' 1.2.3.7:80 check weight 100 inter 2s\n'
                        }
                }
                gen_config_lines = [line.rstrip() for line in haproxy_generated_conf[0]['files']['haproxy.cfg'].splitlines()]
                exp_config_lines = [line.rstrip() for line in haproxy_expected_conf['files']['haproxy.cfg'].splitlines()]
                assert gen_config_lines == exp_config_lines

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_ingress_haproxy_ssl(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test', addr='1.2.3.7'):
            cephadm_module.cache.update_host_networks('test', {
                '1.2.3.0/24': {
                    'if0': [
                        '1.2.3.4',  # simulate already assigned VIP
                        '1.2.3.1',  # simulate interface IP
                    ]
                }
            })

            # the ingress backend
            s = RGWSpec(service_id="foo", placement=PlacementSpec(count=1),
                        rgw_frontend_type='beast')

            ispec = IngressSpec(service_type='ingress',
                                service_id='test',
                                backend_service='rgw.foo',
                                frontend_port=8089,
                                monitor_port=8999,
                                monitor_user='admin',
                                monitor_password='12345',
                                keepalived_password='12345',
                                virtual_interface_networks=['1.2.3.0/24'],
                                virtual_ip="1.2.3.4/32",
                                ssl=True,
                                enable_stats=True,
                                monitor_ssl=True)
            with with_service(cephadm_module, s) as _, with_service(cephadm_module, ispec) as _:
                # generate the haproxy conf based on the specified spec
                haproxy_generated_conf = service_registry.get_service('ingress').haproxy_generate_config(
                    CephadmDaemonDeploySpec(host='test', daemon_id='ingress', service_name=ispec.service_name()))

                haproxy_expected_conf = {
                    'files':
                        {
                            'haproxy.cfg':
                                '# This file is generated by cephadm.'
                                '\nglobal\n    log         '
                                '127.0.0.1 local2\n    '
                                'chroot      /var/lib/haproxy\n    '
                                'pidfile     /var/lib/haproxy/haproxy.pid\n    '
                                'maxconn     8000\n    '
                                'daemon\n    '
                                'stats socket /var/lib/haproxy/stats\n'
                                '\ndefaults\n    '
                                'mode                    http\n    '
                                'log                     global\n    '
                                'option                  httplog\n    '
                                'option                  dontlognull\n    '
                                'option http-server-close\n    '
                                'option forwardfor       except 127.0.0.0/8\n    '
                                'option                  redispatch\n    '
                                'retries                 3\n    '
                                'timeout queue           20s\n    '
                                'timeout connect         5s\n    '
                                'timeout http-request    1s\n    '
                                'timeout http-keep-alive 5s\n    '
                                'timeout client          30s\n    '
                                'timeout server          30s\n    '
                                'timeout check           5s\n    '
                                'maxconn                 8000\n'
                                '\nfrontend stats\n    '
                                'mode http\n    '
                                'bind 1.2.3.4:8999 ssl crt /var/lib/haproxy/haproxy.pem\n    '
                                'bind 1.2.3.7:8999 ssl crt /var/lib/haproxy/haproxy.pem\n    '
                                'stats enable\n    '
                                'stats uri /stats\n    '
                                'stats refresh 10s\n    '
                                'stats auth admin:12345\n    '
                                'http-request use-service prometheus-exporter if { path /metrics }\n    '
                                'monitor-uri /health\n'
                                '\nfrontend frontend\n    '
                                'bind 1.2.3.4:8089 ssl crt /var/lib/haproxy/haproxy.pem\n    '
                                'default_backend backend\n\n'
                                'backend backend\n    '
                                'option forwardfor\n    '
                                'balance static-rr\n    '
                                'option httpchk HEAD / HTTP/1.0\n    '
                                'server '
                                + haproxy_generated_conf[1][0] + ' 1.2.3.7:80 check weight 100 inter 2s\n'
                        }
                }
                gen_config_lines = [line.rstrip() for line in haproxy_generated_conf[0]['files']['haproxy.cfg'].splitlines()]
                exp_config_lines = [line.rstrip() for line in haproxy_expected_conf['files']['haproxy.cfg'].splitlines()]
                assert gen_config_lines == exp_config_lines

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_ingress_haproxy_with_different_stats_cert(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test', addr='1.2.3.7'):
            cephadm_module.cache.update_host_networks('test', {
                '1.2.3.0/24': {
                    'if0': [
                        '1.2.3.4',  # simulate already assigned VIP
                        '1.2.3.1',  # simulate interface IP
                    ]
                }
            })

            # the ingress backend
            s = RGWSpec(service_id="foo", placement=PlacementSpec(count=1),
                        rgw_frontend_type='beast')

            ispec = IngressSpec(service_type='ingress',
                                service_id='test',
                                backend_service='rgw.foo',
                                frontend_port=8089,
                                monitor_port=8999,
                                monitor_user='admin',
                                monitor_password='12345',
                                keepalived_password='12345',
                                virtual_interface_networks=['1.2.3.0/24'],
                                virtual_ip="1.2.3.4/32",
                                ssl=True,
                                enable_stats=True,
                                monitor_ssl=True,
                                monitor_cert_source='cephadm-signed'
                                )
            with with_service(cephadm_module, s) as _, with_service(cephadm_module, ispec) as _:
                # generate the haproxy conf based on the specified spec
                haproxy_generated_conf = service_registry.get_service('ingress').haproxy_generate_config(
                    CephadmDaemonDeploySpec(host='test', daemon_id='ingress', service_name=ispec.service_name()))

                haproxy_expected_conf = {
                    'files':
                        {
                            'haproxy.cfg':
                                '# This file is generated by cephadm.'
                                '\nglobal\n    log         '
                                '127.0.0.1 local2\n    '
                                'chroot      /var/lib/haproxy\n    '
                                'pidfile     /var/lib/haproxy/haproxy.pid\n    '
                                'maxconn     8000\n    '
                                'daemon\n    '
                                'stats socket /var/lib/haproxy/stats\n'
                                '\ndefaults\n    '
                                'mode                    http\n    '
                                'log                     global\n    '
                                'option                  httplog\n    '
                                'option                  dontlognull\n    '
                                'option http-server-close\n    '
                                'option forwardfor       except 127.0.0.0/8\n    '
                                'option                  redispatch\n    '
                                'retries                 3\n    '
                                'timeout queue           20s\n    '
                                'timeout connect         5s\n    '
                                'timeout http-request    1s\n    '
                                'timeout http-keep-alive 5s\n    '
                                'timeout client          30s\n    '
                                'timeout server          30s\n    '
                                'timeout check           5s\n    '
                                'maxconn                 8000\n'
                                '\nfrontend stats\n    '
                                'mode http\n    '
                                'bind 1.2.3.4:8999 ssl crt /var/lib/haproxy/stats_haproxy.pem\n    '
                                'bind 1.2.3.7:8999 ssl crt /var/lib/haproxy/stats_haproxy.pem\n    '
                                'stats enable\n    '
                                'stats uri /stats\n    '
                                'stats refresh 10s\n    '
                                'stats auth admin:12345\n    '
                                'http-request use-service prometheus-exporter if { path /metrics }\n    '
                                'monitor-uri /health\n'
                                '\nfrontend frontend\n    '
                                'bind 1.2.3.4:8089 ssl crt /var/lib/haproxy/haproxy.pem\n    '
                                'default_backend backend\n\n'
                                'backend backend\n    '
                                'option forwardfor\n    '
                                'balance static-rr\n    '
                                'option httpchk HEAD / HTTP/1.0\n    '
                                'server '
                                + haproxy_generated_conf[1][0] + ' 1.2.3.7:80 check weight 100 inter 2s\n'
                        }
                }
                gen_config_lines = [line.rstrip() for line in haproxy_generated_conf[0]['files']['haproxy.cfg'].splitlines()]
                exp_config_lines = [line.rstrip() for line in haproxy_expected_conf['files']['haproxy.cfg'].splitlines()]
                assert gen_config_lines == exp_config_lines

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_ingress_haproxy_monitor_ip(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test', addr='1.2.3.7'):
            cephadm_module.cache.update_host_networks('test', {
                '1.2.3.0/24': {
                    'if0': [
                        '1.2.3.4',  # simulate already assigned VIP
                        '1.2.3.1',  # simulate interface IP
                    ]
                }
            })

            # the ingress backend
            s = RGWSpec(service_id="foo", placement=PlacementSpec(count=1),
                        rgw_frontend_type='beast')

            ispec = IngressSpec(service_type='ingress',
                                service_id='test',
                                backend_service='rgw.foo',
                                frontend_port=8089,
                                monitor_port=8999,
                                monitor_user='admin',
                                monitor_password='12345',
                                keepalived_password='12345',
                                virtual_interface_networks=['1.2.3.0/24'],
                                virtual_ip="1.2.3.4/32",
                                enable_stats=True,
                                monitor_ip_addrs={'test': '1.2.3.1'})
            with with_service(cephadm_module, s) as _, with_service(cephadm_module, ispec) as _:
                # generate the haproxy conf based on the specified spec
                haproxy_generated_conf = service_registry.get_service('ingress').haproxy_generate_config(
                    CephadmDaemonDeploySpec(host='test', daemon_id='ingress', service_name=ispec.service_name()))

                haproxy_expected_conf = {
                    'files':
                        {
                            'haproxy.cfg':
                                '# This file is generated by cephadm.'
                                '\nglobal\n    log         '
                                '127.0.0.1 local2\n    '
                                'chroot      /var/lib/haproxy\n    '
                                'pidfile     /var/lib/haproxy/haproxy.pid\n    '
                                'maxconn     8000\n    '
                                'daemon\n    '
                                'stats socket /var/lib/haproxy/stats\n'
                                '\ndefaults\n    '
                                'mode                    http\n    '
                                'log                     global\n    '
                                'option                  httplog\n    '
                                'option                  dontlognull\n    '
                                'option http-server-close\n    '
                                'option forwardfor       except 127.0.0.0/8\n    '
                                'option                  redispatch\n    '
                                'retries                 3\n    '
                                'timeout queue           20s\n    '
                                'timeout connect         5s\n    '
                                'timeout http-request    1s\n    '
                                'timeout http-keep-alive 5s\n    '
                                'timeout client          30s\n    '
                                'timeout server          30s\n    '
                                'timeout check           5s\n    '
                                'maxconn                 8000\n'
                                '\nfrontend stats\n    '
                                'mode http\n    '
                                'bind 1.2.3.1:8999\n    '
                                'stats enable\n    '
                                'stats uri /stats\n    '
                                'stats refresh 10s\n    '
                                'stats auth admin:12345\n    '
                                'http-request use-service prometheus-exporter if { path /metrics }\n    '
                                'monitor-uri /health\n'
                                '\nfrontend frontend\n    '
                                'bind 1.2.3.4:8089\n    '
                                'default_backend backend\n\n'
                                'backend backend\n    '
                                'option forwardfor\n    '
                                'balance static-rr\n    '
                                'option httpchk HEAD / HTTP/1.0\n    '
                                'server '
                                + haproxy_generated_conf[1][0] + ' 1.2.3.7:80 check weight 100 inter 2s\n'
                        }
                }

                gen_config_lines = [line.rstrip() for line in haproxy_generated_conf[0]['files']['haproxy.cfg'].splitlines()]
                exp_config_lines = [line.rstrip() for line in haproxy_expected_conf['files']['haproxy.cfg'].splitlines()]

                assert gen_config_lines == exp_config_lines

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.services.nfs.NFSService.fence_old_ranks", MagicMock())
    @patch("cephadm.services.nfs.NFSService.run_grace_tool", MagicMock())
    @patch("cephadm.services.nfs.NFSService.purge", MagicMock())
    @patch("cephadm.services.nfs.NFSService.create_rados_config_obj", MagicMock())
    def test_nfs_tls(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test', addr='1.2.3.7'):
            cephadm_module.cache.update_host_networks('test', {
                '1.2.3.0/24': {
                    'if0': ['1.2.3.1']
                }
            })

            nfs_spec = NFSServiceSpec(service_id="foo", placement=PlacementSpec(hosts=['test']),
                                      ssl=True, ssl_cert=ceph_generated_cert, ssl_key=ceph_generated_key,
                                      ssl_ca_cert=cephadm_root_ca, certificate_source='inline', tls_ktls=True,
                                      tls_debug=True, tls_min_version='TLSv1.3',
                                      tls_ciphers='ECDHE-ECDSA-AES256')
            with with_service(cephadm_module, nfs_spec) as _:
                nfs_generated_conf, _ = service_registry.get_service('nfs').generate_config(
                    CephadmDaemonDeploySpec(host='test', daemon_id='foo.test.0.0', service_name=nfs_spec.service_name()))
                ganesha_conf = nfs_generated_conf['files']['ganesha.conf']
                expected_tls_block = (
                    'TLS_CONFIG{\n'
                    '        Enable_TLS = True;\n'
                    '        TLS_Cert_File = /etc/ganesha/tls/tls_cert.pem;\n'
                    '        TLS_Key_File = /etc/ganesha/tls/tls_key.pem;\n'
                    '        TLS_CA_File = /etc/ganesha/tls/tls_ca_cert.pem;\n'
                    '        TLS_Ciphers = "ECDHE-ECDSA-AES256";\n'
                    '        TLS_Min_Version = "TLSv1.3";\n'
                    '        Enable_KTLS = True;\n'
                    '        Enable_debug = True;\n'
                    '}\n'
                )
                assert expected_tls_block in ganesha_conf

    @patch("cephadm.serve.CephadmServe._run_cephadm_json")
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.services.nfs.NFSService.fence_old_ranks", MagicMock())
    @patch("cephadm.services.nfs.NFSService.run_grace_tool", MagicMock())
    @patch("cephadm.services.nfs.NFSService.purge", MagicMock())
    @patch("cephadm.services.nfs.NFSService.create_rados_config_obj", MagicMock())
    def test_nfs_config_rdma_enabled(self, _run_cephadm, _run_cephadm_json, cephadm_module: CephadmOrchestrator):
        """NFS with enable_rdma=True: ganesha.conf has RDMA protocols (nfsrdma, rpcrdma)."""
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        # Mock list-rdma only: return RDMA devices for list-rdma; [] for ls; {} for others (.get)

        async def mock_list_rdma(host, entity, command, *args, **kwargs):
            if command == 'list-rdma':
                return [{'link': 'rdma0/1', 'state': 'ACTIVE',
                         'physical_state': 'LINK_UP', 'netdev': 'eth0'}]
            if command == 'ls':
                return []
            return {}
        _run_cephadm_json.side_effect = mock_list_rdma

        with with_host(cephadm_module, 'host1', addr='1.2.3.7'):
            nfs_spec = NFSServiceSpec(
                service_id="foo",
                placement=PlacementSpec(hosts=['host1']),
                enable_rdma=True,
            )
            with with_service(cephadm_module, nfs_spec) as _:
                nfs_generated_conf, _ = service_registry.get_service('nfs').generate_config(
                    CephadmDaemonDeploySpec(
                        host='host1',
                        daemon_id='foo.host1.0.0',
                        service_name=nfs_spec.service_name(),
                        ports=[2049, 9587, 20049],
                    ))
                ganesha_conf = nfs_generated_conf['files']['ganesha.conf']
                assert "Protocols = 3, 4, nfsrdma, rpcrdma" in ganesha_conf

    @patch("cephadm.serve.CephadmServe._run_cephadm_json")
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.services.nfs.NFSService.fence_old_ranks", MagicMock())
    @patch("cephadm.services.nfs.NFSService.run_grace_tool", MagicMock())
    @patch("cephadm.services.nfs.NFSService.purge", MagicMock())
    @patch("cephadm.services.nfs.NFSService.create_rados_config_obj", MagicMock())
    def test_nfs_config_rdma_custom_port(self, _run_cephadm, _run_cephadm_json, cephadm_module: CephadmOrchestrator):
        """NFS with enable_rdma and rdma_port: ganesha.conf has NFS_RDMA_Port."""
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        # Mock list-rdma only: return RDMA devices for list-rdma; [] for ls; {} for others (.get)

        async def mock_list_rdma(host, entity, command, *args, **kwargs):
            if command == 'list-rdma':
                return [{'link': 'rdma0/1', 'state': 'ACTIVE',
                         'physical_state': 'LINK_UP', 'netdev': 'eth0'}]
            if command == 'ls':
                return []
            return {}
        _run_cephadm_json.side_effect = mock_list_rdma

        with with_host(cephadm_module, 'host1', addr='1.2.3.7'):
            nfs_spec = NFSServiceSpec(
                service_id="foo",
                placement=PlacementSpec(hosts=['host1']),
                enable_rdma=True,
                rdma_port=1234,
            )
            with with_service(cephadm_module, nfs_spec) as _:
                nfs_generated_conf, _ = service_registry.get_service('nfs').generate_config(
                    CephadmDaemonDeploySpec(
                        host='host1',
                        daemon_id='foo.host1.0.0',
                        service_name=nfs_spec.service_name(),
                        ports=[2049, 9587, 1234],
                    ))
                ganesha_conf = nfs_generated_conf['files']['ganesha.conf']
                assert "Protocols = 3, 4, nfsrdma, rpcrdma" in ganesha_conf
                assert "NFS_RDMA_Port = 1234" in ganesha_conf

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.services.nfs.NFSService.fence_old_ranks", MagicMock())
    @patch("cephadm.services.nfs.NFSService.run_grace_tool", MagicMock())
    @patch("cephadm.services.nfs.NFSService.purge", MagicMock())
    @patch("cephadm.services.nfs.NFSService.create_rados_config_obj", MagicMock())
    def test_nfs_config_rdma_disabled(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        """NFS without RDMA: ganesha.conf has Protocols = 3, 4 and no NFS_RDMA_Port."""
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'host1', addr='1.2.3.7'):
            nfs_spec = NFSServiceSpec(
                service_id="foo",
                placement=PlacementSpec(hosts=['host1']),
            )
            with with_service(cephadm_module, nfs_spec) as _:
                nfs_generated_conf, _ = service_registry.get_service('nfs').generate_config(
                    CephadmDaemonDeploySpec(
                        host='host1',
                        daemon_id='foo.host1.0.0',
                        service_name=nfs_spec.service_name(),
                    ))
                ganesha_conf = nfs_generated_conf['files']['ganesha.conf']
                assert "Protocols = 3, 4" in ganesha_conf
                assert "nfsrdma" not in ganesha_conf
                assert "NFS_RDMA_Port" not in ganesha_conf


def test_nfs_colocation_ports_validation():
    """Test validation of colocation_ports in NFSServiceSpec"""
    # Valid case: correct number of colocation_ports (count=3, need 2 additional)
    spec = NFSServiceSpec(
        service_id='mynfs',
        placement=PlacementSpec(count=3),
        port=2049,
        monitoring_port=9587,
        colocation_ports=[
            {'data_port': 3049, 'monitoring_port': 9588},
            {'data_port': 4049, 'monitoring_port': 9589}
        ]
    )
    spec.validate()  # Should not raise

    # Invalid case: too few colocation_ports (count=4, need 3 additional, but only 1 provided)
    with pytest.raises(SpecValidationError) as e:
        spec = NFSServiceSpec(
            service_id='mynfs',
            placement=PlacementSpec(count=4),
            port=2049,
            monitoring_port=9587,
            colocation_ports=[{'data_port': 3049, 'monitoring_port': 9588}]
        )
        spec.validate()
    assert "colocation_ports requires 3 entries for count=4 (got 1)" in str(e.value)

    # Invalid case: missing required field
    with pytest.raises(SpecValidationError) as e:
        spec = NFSServiceSpec(
            service_id='mynfs',
            placement=PlacementSpec(count=3),
            port=2049,
            monitoring_port=9587,
            colocation_ports=[
                {'data_port': 3049},  # Missing monitoring_port
                {'data_port': 4049, 'monitoring_port': 9589}
            ]
        )
        spec.validate()
    assert "missing required fields: monitoring_port" in str(e.value)


def test_nfs_colocation_ports_validation_with_rdma():
    """Test colocation_ports with enable_rdma requires rdma_port in each entry."""
    # Valid: enable_rdma=True, count=3, 2 colocation entries with data_port, monitoring_port, rdma_port
    spec = NFSServiceSpec(
        service_id='mynfs',
        placement=PlacementSpec(count=3),
        port=2049,
        monitoring_port=9587,
        enable_rdma=True,
        rdma_port=20049,
        colocation_ports=[
            {'data_port': 3049, 'monitoring_port': 9588, 'rdma_port': 20050},
            {'data_port': 4049, 'monitoring_port': 9589, 'rdma_port': 20051},
        ]
    )
    spec.validate()

    # Invalid: enable_rdma=True but colocation entry missing rdma_port
    with pytest.raises(SpecValidationError) as e:
        spec = NFSServiceSpec(
            service_id='mynfs',
            placement=PlacementSpec(count=3),
            port=2049,
            monitoring_port=9587,
            enable_rdma=True,
            colocation_ports=[
                {'data_port': 3049, 'monitoring_port': 9588},  # missing rdma_port
                {'data_port': 4049, 'monitoring_port': 9589, 'rdma_port': 20051},
            ]
        )
        spec.validate()
    assert "missing required fields: rdma_port" in str(e.value)


@patch("cephadm.services.nfs.NFSService.run_grace_tool", MagicMock())
@patch("cephadm.services.nfs.NFSService.purge", MagicMock())
@patch("cephadm.services.nfs.NFSService.create_rados_config_obj", MagicMock())
def test_nfs_choose_next_action(cephadm_module, mock_cephadm):
    nfs_spec = NFSServiceSpec(
        service_id="foo",
        placement=PlacementSpec(hosts=['test']),
        ssl=True,
        ssl_cert=ceph_generated_cert,
        ssl_key=ceph_generated_key,
        ssl_ca_cert=cephadm_root_ca,
        certificate_source='inline',
    )
    with contextlib.ExitStack() as stack:
        stack.enter_context(with_host(cephadm_module, "test"))
        stack.enter_context(with_service(cephadm_module, nfs_spec))
        nfs_spec.tls_ktls = True
        cephadm_module.apply([nfs_spec])
        # manually invoke _check_daemons to trigger a call to
        # _daemon_action so we can check what action was chosen
        mock_cephadm.serve(cephadm_module)._check_daemons()
        mock_cephadm._daemon_action.assert_called_with(ANY, action="redeploy")
        # NB: it appears that the code is designed to redeploy unless all
        # dependencies are prefixed with 'kmip' but I can't find any code
        # that would produce any dependencies prefixed with 'kmip'!


@patch("cephadm.services.nfs.NFSService.run_grace_tool", MagicMock())
@patch("cephadm.services.nfs.NFSService.purge", MagicMock())
@patch("cephadm.services.nfs.NFSService.create_rados_config_obj", MagicMock())
def test_ingress_for_nfs_choose_next_action(cephadm_module, mock_cephadm):
    nfs_spec = NFSServiceSpec(
        service_id="foo",
        placement=PlacementSpec(hosts=['test']),
    )
    ingress_spec = IngressSpec(
        service_id='bar',
        backend_service='nfs.foo',
        frontend_port=2468,
        monitor_port=8642,
        virtual_ip='1.2.3.0/24',
        placement=PlacementSpec(hosts=['test']),
    )
    with contextlib.ExitStack() as stack:
        stack.enter_context(with_host(cephadm_module, "test"))
        stack.enter_context(with_host(cephadm_module, "test2"))
        cephadm_module.cache.update_host_networks(
            'test',
            {
                '1.2.3.0/24': {
                    'if0': [
                        '1.2.3.4',  # simulate already assigned VIP
                        '1.2.3.1',  # simulate interface IP
                    ]
                }
            },
        )
        stack.enter_context(with_service(cephadm_module, nfs_spec))
        # For whatever reason this mocked out version of the nfs deamon
        # doesn't get assigned ports and ingress needs them. So we
        # manually help it along here.
        nfs_dds = cephadm_module.cache.get_daemons_by_service(
            ingress_spec.backend_service
        )
        nfs_dd = nfs_dds[0]
        nfs_dd.ports = [8765]
        mock_cephadm.serve(cephadm_module)._check_daemons()
        stack.enter_context(with_service(cephadm_module, ingress_spec))
        ingress_spec.placement = PlacementSpec(hosts=['test', 'test2'])
        cephadm_module.apply([ingress_spec])
        # manually invoke _check_daemons to trigger a call to
        # _daemon_action so we can check what action was chosen
        mock_cephadm.serve(cephadm_module)._check_daemons()
        mock_cephadm._daemon_action.assert_called_with(ANY, action="redeploy")
