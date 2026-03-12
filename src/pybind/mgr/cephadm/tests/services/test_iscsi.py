import json
from typing import Dict, List
from unittest.mock import MagicMock, call, patch

from cephadm.services.cephadmservice import CephadmDaemonDeploySpec
from cephadm.services.iscsi import IscsiService
from cephadm.module import CephadmOrchestrator
from ceph.deployment.service_spec import IscsiServiceSpec
from cephadm.tests.fixtures import with_host, with_service, async_side_effect
from cephadm.tlsobject_types import TLSCredentials
from orchestrator._interface import DaemonDescription


ceph_generated_cert = """-----BEGIN CERTIFICATE-----\nMIICxjCCAa4CEQDIZSujNBlKaLJzmvntjukjMA0GCSqGSIb3DQEBDQUAMCExDTAL\nBgNVBAoMBENlcGgxEDAOBgNVBAMMB2NlcGhhZG0wHhcNMjIwNzEzMTE0NzA3WhcN\nMzIwNzEwMTE0NzA3WjAhMQ0wCwYDVQQKDARDZXBoMRAwDgYDVQQDDAdjZXBoYWRt\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyyMe4DMA+MeYK7BHZMHB\nq7zjliEOcNgxomjU8qbf5USF7Mqrf6+/87XWqj4pCyAW8x0WXEr6A56a+cmBVmt+\nqtWDzl020aoId6lL5EgLLn6/kMDCCJLq++Lg9cEofMSvcZh+lY2f+1p+C+00xent\nrLXvXGOilAZWaQfojT2BpRnNWWIFbpFwlcKrlg2G0cFjV5c1m6a0wpsQ9JHOieq0\nSvwCixajwq3CwAYuuiU1wjI4oJO4Io1+g8yB3nH2Mo/25SApCxMXuXh4kHLQr/T4\n4hqisvG4uJYgKMcSIrWj5o25mclByGi1UI/kZkCUES94i7Z/3ihx4Bad0AMs/9tw\nFwIDAQABMA0GCSqGSIb3DQEBDQUAA4IBAQAf+pwz7Gd7mDwU2LY0TQXsK6/8KGzh\nHuX+ErOb8h5cOAbvCnHjyJFWf6gCITG98k9nxU9NToG0WYuNm/max1y/54f0dtxZ\npUo6KSNl3w6iYCfGOeUIj8isi06xMmeTgMNzv8DYhDt+P2igN6LenqWTVztogkiV\nxQ5ZJFFLEw4sN0CXnrZX3t5ruakxLXLTLKeE0I91YJvjClSBGkVJq26wOKQNHMhx\npWxeydQ5EgPZY+Aviz5Dnxe8aB7oSSovpXByzxURSabOuCK21awW5WJCGNpmqhWK\nZzACBDEstccj57c4OGV0eayHJRsluVr2e9NHRINZA3qdB37e6gsI1xHo\n-----END CERTIFICATE-----\n"""

ceph_generated_key = """-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDLIx7gMwD4x5gr\nsEdkwcGrvOOWIQ5w2DGiaNTypt/lRIXsyqt/r7/ztdaqPikLIBbzHRZcSvoDnpr5\nyYFWa36q1YPOXTbRqgh3qUvkSAsufr+QwMIIkur74uD1wSh8xK9xmH6VjZ/7Wn4L\n7TTF6e2ste9cY6KUBlZpB+iNPYGlGc1ZYgVukXCVwquWDYbRwWNXlzWbprTCmxD0\nkc6J6rRK/AKLFqPCrcLABi66JTXCMjigk7gijX6DzIHecfYyj/blICkLExe5eHiQ\nctCv9PjiGqKy8bi4liAoxxIitaPmjbmZyUHIaLVQj+RmQJQRL3iLtn/eKHHgFp3Q\nAyz/23AXAgMBAAECggEAVoTB3Mm8azlPlaQB9GcV3tiXslSn+uYJ1duCf0sV52dV\nBzKW8s5fGiTjpiTNhGCJhchowqxoaew+o47wmGc2TvqbpeRLuecKrjScD0GkCYyQ\neM2wlshEbz4FhIZdgS6gbuh9WaM1dW/oaZoBNR5aTYo7xYTmNNeyLA/jO2zr7+4W\n5yES1lMSBXpKk7bDGKYY4bsX2b5RLr2Grh2u2bp7hoLABCEvuu8tSQdWXLEXWpXo\njwmV3hc6tabypIa0mj2Dmn2Dmt1ppSO0AZWG/WAizN3f4Z0r/u9HnbVrVmh0IEDw\n3uf2LP5o3msG9qKCbzv3lMgt9mMr70HOKnJ8ohMSKQKBgQDLkNb+0nr152HU9AeJ\nvdz8BeMxcwxCG77iwZphZ1HprmYKvvXgedqWtS6FRU+nV6UuQoPUbQxJBQzrN1Qv\nwKSlOAPCrTJgNgF/RbfxZTrIgCPuK2KM8I89VZv92TSGi362oQA4MazXC8RAWjoJ\nSu1/PHzK3aXOfVNSLrOWvIYeZQKBgQD/dgT6RUXKg0UhmXj7ExevV+c7oOJTDlMl\nvLngrmbjRgPO9VxLnZQGdyaBJeRngU/UXfNgajT/MU8B5fSKInnTMawv/tW7634B\nw3v6n5kNIMIjJmENRsXBVMllDTkT9S7ApV+VoGnXRccbTiDapBThSGd0wri/CuwK\nNWK1YFOeywKBgEDyI/XG114PBUJ43NLQVWm+wx5qszWAPqV/2S5MVXD1qC6zgCSv\nG9NLWN1CIMimCNg6dm7Wn73IM7fzvhNCJgVkWqbItTLG6DFf3/DPODLx1wTMqLOI\nqFqMLqmNm9l1Nec0dKp5BsjRQzq4zp1aX21hsfrTPmwjxeqJZdioqy2VAoGAXR5X\nCCdSHlSlUW8RE2xNOOQw7KJjfWT+WAYoN0c7R+MQplL31rRU7dpm1bLLRBN11vJ8\nMYvlT5RYuVdqQSP6BkrX+hLJNBvOLbRlL+EXOBrVyVxHCkDe+u7+DnC4epbn+N8P\nLYpwqkDMKB7diPVAizIKTBxinXjMu5fkKDs5n+sCgYBbZheYKk5M0sIxiDfZuXGB\nkf4mJdEkTI1KUGRdCwO/O7hXbroGoUVJTwqBLi1tKqLLarwCITje2T200BYOzj82\nqwRkCXGtXPKnxYEEUOiFx9OeDrzsZV00cxsEnX0Zdj+PucQ/J3Cvd0dWUspJfLHJ\n39gnaegswnz9KMQAvzKFdg==\n-----END PRIVATE KEY-----\n"""


class FakeInventory:
    def get_addr(self, name: str) -> str:
        return '1.2.3.4'


class FakeMgr:
    def __init__(self):
        self.config = ''
        self.set_mon_crush_locations: Dict[str, List[str]] = {}
        self.check_mon_command = MagicMock(side_effect=self._check_mon_command)
        self.mon_command = MagicMock(side_effect=self._check_mon_command)
        self.template = MagicMock()
        self.log = MagicMock()
        self.cert_mgr = MagicMock()
        self.inventory = FakeInventory()

    def _check_mon_command(self, cmd_dict, inbuf=None):
        prefix = cmd_dict.get('prefix')
        if prefix == 'get-cmd':
            return 0, self.config, ''
        if prefix == 'set-cmd':
            self.config = cmd_dict.get('value')
            return 0, 'value set', ''
        if prefix in ['auth get']:
            return 0, '[foo]\nkeyring = asdf\n', ''
        if prefix == 'quorum_status':
            # actual quorum status output from testing
            # note in this output all of the mons have blank crush locations
            return 0, """{"election_epoch": 14, "quorum": [0, 1, 2], "quorum_names": ["vm-00", "vm-01", "vm-02"], "quorum_leader_name": "vm-00", "quorum_age": 101, "features": {"quorum_con": "4540138322906710015", "quorum_mon": ["kraken", "luminous", "mimic", "osdmap-prune", "nautilus", "octopus", "pacific", "elector-pinging", "quincy", "reef"]}, "monmap": {"epoch": 3, "fsid": "9863e1b8-6f24-11ed-8ad8-525400c13ad2", "modified": "2022-11-28T14:00:29.972488Z", "created": "2022-11-28T13:57:55.847497Z", "min_mon_release": 18, "min_mon_release_name": "reef", "election_strategy": 1, "disallowed_leaders: ": "", "stretch_mode": false, "tiebreaker_mon": "", "features": {"persistent": ["kraken", "luminous", "mimic", "osdmap-prune", "nautilus", "octopus", "pacific", "elector-pinging", "quincy", "reef"], "optional": []}, "mons": [{"rank": 0, "name": "vm-00", "public_addrs": {"addrvec": [{"type": "v2", "addr": "192.168.122.61:3300", "nonce": 0}, {"type": "v1", "addr": "192.168.122.61:6789", "nonce": 0}]}, "addr": "192.168.122.61:6789/0", "public_addr": "192.168.122.61:6789/0", "priority": 0, "weight": 0, "crush_location": "{}"}, {"rank": 1, "name": "vm-01", "public_addrs": {"addrvec": [{"type": "v2", "addr": "192.168.122.63:3300", "nonce": 0}, {"type": "v1", "addr": "192.168.122.63:6789", "nonce": 0}]}, "addr": "192.168.122.63:6789/0", "public_addr": "192.168.122.63:6789/0", "priority": 0, "weight": 0, "crush_location": "{}"}, {"rank": 2, "name": "vm-02", "public_addrs": {"addrvec": [{"type": "v2", "addr": "192.168.122.82:3300", "nonce": 0}, {"type": "v1", "addr": "192.168.122.82:6789", "nonce": 0}]}, "addr": "192.168.122.82:6789/0", "public_addr": "192.168.122.82:6789/0", "priority": 0, "weight": 0, "crush_location": "{}"}]}}""", ''
        if prefix == 'mon set_location':
            self.set_mon_crush_locations[cmd_dict.get('name')] = cmd_dict.get('args')
            return 0, '', ''
        return -1, '', 'error'

    def get_minimal_ceph_conf(self) -> str:
        return ''

    def get_mgr_ip(self) -> str:
        return '1.2.3.4'


class TestISCSIService:

    mgr = FakeMgr()
    iscsi_service = IscsiService(mgr)

    iscsi_spec = IscsiServiceSpec(service_type='iscsi', service_id="a")
    iscsi_spec.daemon_type = "iscsi"
    iscsi_spec.daemon_id = "a"
    iscsi_spec.spec = MagicMock()
    iscsi_spec.spec.daemon_type = "iscsi"
    iscsi_spec.spec.ssl_cert = ''
    iscsi_spec.api_user = "user"
    iscsi_spec.api_password = "password"
    iscsi_spec.api_port = 5000
    iscsi_spec.api_secure = False
    iscsi_spec.ssl_cert = "cert"
    iscsi_spec.ssl_key = "key"

    mgr.spec_store = MagicMock()
    mgr.spec_store.all_specs.get.return_value = iscsi_spec

    @patch("cephadm.services.cephadmservice.CephadmService.get_certificates",
           lambda instance, dspec, ips=None: TLSCredentials(ceph_generated_cert, ceph_generated_key))
    def test_iscsi_client_caps(self):

        iscsi_daemon_spec = CephadmDaemonDeploySpec(
            host='host', daemon_id='a', service_name=self.iscsi_spec.service_name())

        self.iscsi_service.prepare_create(iscsi_daemon_spec)

        expected_caps = ['mon',
                         'profile rbd, allow command "osd blocklist", allow command "config-key get" with "key" prefix "iscsi/"',
                         'mgr', 'allow command "service status"',
                         'osd', 'allow rwx']

        expected_call = call({'prefix': 'auth get-or-create',
                              'entity': 'client.iscsi.a',
                              'caps': expected_caps})
        expected_call2 = call({'prefix': 'auth caps',
                               'entity': 'client.iscsi.a',
                               'caps': expected_caps})
        expected_call3 = call({'prefix': 'auth get',
                               'entity': 'client.iscsi.a'})

        assert expected_call in self.mgr.mon_command.mock_calls
        assert expected_call2 in self.mgr.mon_command.mock_calls
        assert expected_call3 in self.mgr.mon_command.mock_calls

    @patch('cephadm.utils.resolve_ip')
    @patch("cephadm.services.cephadmservice.CephadmService.get_certificates",
           lambda instance, dspec, ips=None: TLSCredentials(ceph_generated_cert, ceph_generated_key))
    def test_iscsi_dashboard_config(self, mock_resolve_ip):

        self.mgr.check_mon_command = MagicMock()
        self.mgr.check_mon_command.return_value = ('', '{"gateways": {}}', '')

        # Case 1: use IPV4 address
        id1 = DaemonDescription(daemon_type='iscsi', hostname="testhost1",
                                daemon_id="a", ip='192.168.1.1')
        daemon_list = [id1]
        mock_resolve_ip.return_value = '192.168.1.1'

        self.iscsi_service.config_dashboard(daemon_list)

        dashboard_expected_call = call({'prefix': 'dashboard iscsi-gateway-add',
                                        'name': 'testhost1'},
                                       'http://user:password@192.168.1.1:5000')

        assert dashboard_expected_call in self.mgr.check_mon_command.mock_calls

        # Case 2: use IPV6 address
        self.mgr.check_mon_command.reset_mock()

        id1 = DaemonDescription(daemon_type='iscsi', hostname="testhost1",
                                daemon_id="a", ip='FEDC:BA98:7654:3210:FEDC:BA98:7654:3210')
        mock_resolve_ip.return_value = 'FEDC:BA98:7654:3210:FEDC:BA98:7654:3210'

        self.iscsi_service.config_dashboard(daemon_list)

        dashboard_expected_call = call({'prefix': 'dashboard iscsi-gateway-add',
                                        'name': 'testhost1'},
                                       'http://user:password@[FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]:5000')

        assert dashboard_expected_call in self.mgr.check_mon_command.mock_calls

        # Case 3: IPV6 Address . Secure protocol
        self.mgr.check_mon_command.reset_mock()

        self.iscsi_spec.api_secure = True

        self.iscsi_service.config_dashboard(daemon_list)

        dashboard_expected_call = call({'prefix': 'dashboard iscsi-gateway-add',
                                        'name': 'testhost1'},
                                       'https://user:password@[FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]:5000')

        assert dashboard_expected_call in self.mgr.check_mon_command.mock_calls

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.module.CephadmOrchestrator.get_unique_name")
    @patch("cephadm.services.iscsi.get_trusted_ips")
    def test_iscsi_config(self, _get_trusted_ips, _get_name, _run_cephadm, cephadm_module: CephadmOrchestrator):

        iscsi_daemon_id = 'testpool.test.qwert'
        trusted_ips = '1.1.1.1,2.2.2.2'
        api_port = 3456
        api_user = 'test-user'
        api_password = 'test-password'
        pool = 'testpool'
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        _get_name.return_value = iscsi_daemon_id
        _get_trusted_ips.return_value = trusted_ips

        iscsi_gateway_conf = f"""# This file is generated by cephadm.
[config]
cluster_client_name = client.iscsi.{iscsi_daemon_id}
pool = {pool}
trusted_ip_list = {trusted_ips}
minimum_gateways = 1
api_port = {api_port}
api_user = {api_user}
api_password = {api_password}
api_secure = False
log_to_stderr = True
log_to_stderr_prefix = debug
log_to_file = False"""

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, IscsiServiceSpec(service_id=pool,
                                                               api_port=api_port,
                                                               api_user=api_user,
                                                               api_password=api_password,
                                                               pool=pool,
                                                               trusted_ip_list=trusted_ips)):
                _run_cephadm.assert_called_with(
                    'test',
                    f'iscsi.{iscsi_daemon_id}',
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": f'iscsi.{iscsi_daemon_id}',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [api_port],
                        },
                        "meta": {
                            'service_name': f'iscsi.{pool}',
                            'ports': [api_port],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": {
                            "config": "",
                            "keyring": f"[client.iscsi.{iscsi_daemon_id}]\nkey = None\n",
                            "files": {
                                "iscsi-gateway.cfg": iscsi_gateway_conf,
                            },
                        }
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.module.CephadmOrchestrator.get_unique_name")
    @patch("cephadm.services.iscsi.get_trusted_ips")
    @patch("cephadm.services.cephadmservice.CephadmService.get_certificates",
           lambda instance, dspec, ips=None: TLSCredentials(ceph_generated_cert, ceph_generated_key))
    def test_iscsi_config_with_security_enabled(self, _get_trusted_ips, _get_name, _run_cephadm, cephadm_module: CephadmOrchestrator):

        iscsi_daemon_id = 'testpool.test.qwert'
        trusted_ips = '1.1.1.1,2.2.2.2'
        api_port = 3456
        api_user = 'test-user'
        api_password = 'test-password'
        pool = 'testpool'
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        _get_name.return_value = iscsi_daemon_id
        _get_trusted_ips.return_value = trusted_ips

        cephadm_module.check_mon_command = MagicMock()
        cephadm_module.check_mon_command.return_value = (0, '', '')

        iscsi_gateway_conf = f"""# This file is generated by cephadm.
[config]
cluster_client_name = client.iscsi.{iscsi_daemon_id}
pool = {pool}
trusted_ip_list = {trusted_ips}
minimum_gateways = 1
api_port = {api_port}
api_user = {api_user}
api_password = {api_password}
api_secure = True
log_to_stderr = True
log_to_stderr_prefix = debug
log_to_file = False"""

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, IscsiServiceSpec(service_id=pool,
                                                               api_port=api_port,
                                                               api_user=api_user,
                                                               api_password=api_password,
                                                               pool=pool,
                                                               api_secure=True,
                                                               ssl_cert=ceph_generated_cert,
                                                               ssl_key=ceph_generated_key,
                                                               trusted_ip_list=trusted_ips)):
                _run_cephadm.assert_called_with(
                    'test',
                    f'iscsi.{iscsi_daemon_id}',
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": f'iscsi.{iscsi_daemon_id}',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [api_port],
                        },
                        "meta": {
                            'service_name': f'iscsi.{pool}',
                            'ports': [api_port],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": {
                            "config": "",
                            "keyring": f"[client.iscsi.{iscsi_daemon_id}]\nkey = None\n",
                            "files": {
                                "iscsi-gateway.cfg": iscsi_gateway_conf,
                            },
                        }
                    }),
                    error_ok=True,
                    use_current_daemon_image=False,
                )

                expected_cert_call = call({
                    'prefix': 'config-key set',
                    'key': f'iscsi/client.iscsi.{iscsi_daemon_id}/iscsi-gateway.crt',
                    'val': ceph_generated_cert,
                })
                expected_key_call = call({
                    'prefix': 'config-key set',
                    'key': f'iscsi/client.iscsi.{iscsi_daemon_id}/iscsi-gateway.key',
                    'val': ceph_generated_key,
                })

                cephadm_module.check_mon_command.assert_has_calls(
                    [expected_cert_call, expected_key_call],
                    any_order=True
                )
