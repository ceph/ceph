from textwrap import dedent
import json
import urllib.parse
import yaml
from mgr_util import build_url

import pytest

from unittest.mock import MagicMock, call, patch, ANY

from cephadm.serve import CephadmServe
from cephadm.services.cephadmservice import MonService, MgrService, MdsService, RgwService, \
    RbdMirrorService, CrashService, CephadmDaemonDeploySpec
from cephadm.services.iscsi import IscsiService
from cephadm.services.nfs import NFSService
from cephadm.services.nvmeof import NvmeofService
from cephadm.services.osd import OSDService
from cephadm.services.monitoring import GrafanaService, AlertmanagerService, PrometheusService, \
    NodeExporterService, LokiService, PromtailService
from cephadm.module import CephadmOrchestrator
from ceph.deployment.service_spec import (
    AlertManagerSpec,
    CephExporterSpec,
    CustomContainerSpec,
    GrafanaSpec,
    IngressSpec,
    IscsiServiceSpec,
    MonitoringSpec,
    NFSServiceSpec,
    NvmeofServiceSpec,
    PlacementSpec,
    PrometheusSpec,
    RGWSpec,
    SNMPGatewaySpec,
    ServiceSpec,
    TracingSpec,
)
from cephadm.tests.fixtures import with_host, with_service, _run_cephadm, async_side_effect

from ceph.utils import datetime_now

from orchestrator import OrchestratorError
from orchestrator._interface import DaemonDescription

from typing import Dict, List

grafana_cert = """-----BEGIN CERTIFICATE-----\nMIICxjCCAa4CEQDIZSujNBlKaLJzmvntjukjMA0GCSqGSIb3DQEBDQUAMCExDTAL\nBgNVBAoMBENlcGgxEDAOBgNVBAMMB2NlcGhhZG0wHhcNMjIwNzEzMTE0NzA3WhcN\nMzIwNzEwMTE0NzA3WjAhMQ0wCwYDVQQKDARDZXBoMRAwDgYDVQQDDAdjZXBoYWRt\nMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyyMe4DMA+MeYK7BHZMHB\nq7zjliEOcNgxomjU8qbf5USF7Mqrf6+/87XWqj4pCyAW8x0WXEr6A56a+cmBVmt+\nqtWDzl020aoId6lL5EgLLn6/kMDCCJLq++Lg9cEofMSvcZh+lY2f+1p+C+00xent\nrLXvXGOilAZWaQfojT2BpRnNWWIFbpFwlcKrlg2G0cFjV5c1m6a0wpsQ9JHOieq0\nSvwCixajwq3CwAYuuiU1wjI4oJO4Io1+g8yB3nH2Mo/25SApCxMXuXh4kHLQr/T4\n4hqisvG4uJYgKMcSIrWj5o25mclByGi1UI/kZkCUES94i7Z/3ihx4Bad0AMs/9tw\nFwIDAQABMA0GCSqGSIb3DQEBDQUAA4IBAQAf+pwz7Gd7mDwU2LY0TQXsK6/8KGzh\nHuX+ErOb8h5cOAbvCnHjyJFWf6gCITG98k9nxU9NToG0WYuNm/max1y/54f0dtxZ\npUo6KSNl3w6iYCfGOeUIj8isi06xMmeTgMNzv8DYhDt+P2igN6LenqWTVztogkiV\nxQ5ZJFFLEw4sN0CXnrZX3t5ruakxLXLTLKeE0I91YJvjClSBGkVJq26wOKQNHMhx\npWxeydQ5EgPZY+Aviz5Dnxe8aB7oSSovpXByzxURSabOuCK21awW5WJCGNpmqhWK\nZzACBDEstccj57c4OGV0eayHJRsluVr2e9NHRINZA3qdB37e6gsI1xHo\n-----END CERTIFICATE-----\n"""

grafana_key = """-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDLIx7gMwD4x5gr\nsEdkwcGrvOOWIQ5w2DGiaNTypt/lRIXsyqt/r7/ztdaqPikLIBbzHRZcSvoDnpr5\nyYFWa36q1YPOXTbRqgh3qUvkSAsufr+QwMIIkur74uD1wSh8xK9xmH6VjZ/7Wn4L\n7TTF6e2ste9cY6KUBlZpB+iNPYGlGc1ZYgVukXCVwquWDYbRwWNXlzWbprTCmxD0\nkc6J6rRK/AKLFqPCrcLABi66JTXCMjigk7gijX6DzIHecfYyj/blICkLExe5eHiQ\nctCv9PjiGqKy8bi4liAoxxIitaPmjbmZyUHIaLVQj+RmQJQRL3iLtn/eKHHgFp3Q\nAyz/23AXAgMBAAECggEAVoTB3Mm8azlPlaQB9GcV3tiXslSn+uYJ1duCf0sV52dV\nBzKW8s5fGiTjpiTNhGCJhchowqxoaew+o47wmGc2TvqbpeRLuecKrjScD0GkCYyQ\neM2wlshEbz4FhIZdgS6gbuh9WaM1dW/oaZoBNR5aTYo7xYTmNNeyLA/jO2zr7+4W\n5yES1lMSBXpKk7bDGKYY4bsX2b5RLr2Grh2u2bp7hoLABCEvuu8tSQdWXLEXWpXo\njwmV3hc6tabypIa0mj2Dmn2Dmt1ppSO0AZWG/WAizN3f4Z0r/u9HnbVrVmh0IEDw\n3uf2LP5o3msG9qKCbzv3lMgt9mMr70HOKnJ8ohMSKQKBgQDLkNb+0nr152HU9AeJ\nvdz8BeMxcwxCG77iwZphZ1HprmYKvvXgedqWtS6FRU+nV6UuQoPUbQxJBQzrN1Qv\nwKSlOAPCrTJgNgF/RbfxZTrIgCPuK2KM8I89VZv92TSGi362oQA4MazXC8RAWjoJ\nSu1/PHzK3aXOfVNSLrOWvIYeZQKBgQD/dgT6RUXKg0UhmXj7ExevV+c7oOJTDlMl\nvLngrmbjRgPO9VxLnZQGdyaBJeRngU/UXfNgajT/MU8B5fSKInnTMawv/tW7634B\nw3v6n5kNIMIjJmENRsXBVMllDTkT9S7ApV+VoGnXRccbTiDapBThSGd0wri/CuwK\nNWK1YFOeywKBgEDyI/XG114PBUJ43NLQVWm+wx5qszWAPqV/2S5MVXD1qC6zgCSv\nG9NLWN1CIMimCNg6dm7Wn73IM7fzvhNCJgVkWqbItTLG6DFf3/DPODLx1wTMqLOI\nqFqMLqmNm9l1Nec0dKp5BsjRQzq4zp1aX21hsfrTPmwjxeqJZdioqy2VAoGAXR5X\nCCdSHlSlUW8RE2xNOOQw7KJjfWT+WAYoN0c7R+MQplL31rRU7dpm1bLLRBN11vJ8\nMYvlT5RYuVdqQSP6BkrX+hLJNBvOLbRlL+EXOBrVyVxHCkDe+u7+DnC4epbn+N8P\nLYpwqkDMKB7diPVAizIKTBxinXjMu5fkKDs5n+sCgYBbZheYKk5M0sIxiDfZuXGB\nkf4mJdEkTI1KUGRdCwO/O7hXbroGoUVJTwqBLi1tKqLLarwCITje2T200BYOzj82\nqwRkCXGtXPKnxYEEUOiFx9OeDrzsZV00cxsEnX0Zdj+PucQ/J3Cvd0dWUspJfLHJ\n39gnaegswnz9KMQAvzKFdg==\n-----END PRIVATE KEY-----\n"""


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


class TestCephadmService:
    def test_set_service_url_on_dashboard(self):
        # pylint: disable=protected-access
        mgr = FakeMgr()
        service_url = 'http://svc:1000'
        service = GrafanaService(mgr)
        service._set_service_url_on_dashboard('svc', 'get-cmd', 'set-cmd', service_url)
        assert mgr.config == service_url

        # set-cmd should not be called if value doesn't change
        mgr.check_mon_command.reset_mock()
        service._set_service_url_on_dashboard('svc', 'get-cmd', 'set-cmd', service_url)
        mgr.check_mon_command.assert_called_once_with({'prefix': 'get-cmd'})

    def _get_services(self, mgr):
        # services:
        osd_service = OSDService(mgr)
        nfs_service = NFSService(mgr)
        mon_service = MonService(mgr)
        mgr_service = MgrService(mgr)
        mds_service = MdsService(mgr)
        rgw_service = RgwService(mgr)
        rbd_mirror_service = RbdMirrorService(mgr)
        grafana_service = GrafanaService(mgr)
        alertmanager_service = AlertmanagerService(mgr)
        prometheus_service = PrometheusService(mgr)
        node_exporter_service = NodeExporterService(mgr)
        loki_service = LokiService(mgr)
        promtail_service = PromtailService(mgr)
        crash_service = CrashService(mgr)
        iscsi_service = IscsiService(mgr)
        nvmeof_service = NvmeofService(mgr)
        cephadm_services = {
            'mon': mon_service,
            'mgr': mgr_service,
            'osd': osd_service,
            'mds': mds_service,
            'rgw': rgw_service,
            'rbd-mirror': rbd_mirror_service,
            'nfs': nfs_service,
            'grafana': grafana_service,
            'alertmanager': alertmanager_service,
            'prometheus': prometheus_service,
            'node-exporter': node_exporter_service,
            'loki': loki_service,
            'promtail': promtail_service,
            'crash': crash_service,
            'iscsi': iscsi_service,
            'nvmeof': nvmeof_service,
        }
        return cephadm_services

    def test_get_auth_entity(self):
        mgr = FakeMgr()
        cephadm_services = self._get_services(mgr)

        for daemon_type in ['rgw', 'rbd-mirror', 'nfs', "iscsi"]:
            assert "client.%s.id1" % (daemon_type) == \
                cephadm_services[daemon_type].get_auth_entity("id1", "host")
            assert "client.%s.id1" % (daemon_type) == \
                cephadm_services[daemon_type].get_auth_entity("id1", "")
            assert "client.%s.id1" % (daemon_type) == \
                cephadm_services[daemon_type].get_auth_entity("id1")

        assert "client.crash.host" == \
            cephadm_services["crash"].get_auth_entity("id1", "host")
        with pytest.raises(OrchestratorError):
            cephadm_services["crash"].get_auth_entity("id1", "")
            cephadm_services["crash"].get_auth_entity("id1")

        assert "mon." == cephadm_services["mon"].get_auth_entity("id1", "host")
        assert "mon." == cephadm_services["mon"].get_auth_entity("id1", "")
        assert "mon." == cephadm_services["mon"].get_auth_entity("id1")

        assert "mgr.id1" == cephadm_services["mgr"].get_auth_entity("id1", "host")
        assert "mgr.id1" == cephadm_services["mgr"].get_auth_entity("id1", "")
        assert "mgr.id1" == cephadm_services["mgr"].get_auth_entity("id1")

        for daemon_type in ["osd", "mds"]:
            assert "%s.id1" % daemon_type == \
                cephadm_services[daemon_type].get_auth_entity("id1", "host")
            assert "%s.id1" % daemon_type == \
                cephadm_services[daemon_type].get_auth_entity("id1", "")
            assert "%s.id1" % daemon_type == \
                cephadm_services[daemon_type].get_auth_entity("id1")

        # services based on CephadmService shouldn't have get_auth_entity
        with pytest.raises(AttributeError):
            for daemon_type in ['grafana', 'alertmanager', 'prometheus', 'node-exporter', 'loki', 'promtail']:
                cephadm_services[daemon_type].get_auth_entity("id1", "host")
                cephadm_services[daemon_type].get_auth_entity("id1", "")
                cephadm_services[daemon_type].get_auth_entity("id1")


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
    @patch("cephadm.services.iscsi.IscsiService.get_trusted_ips")
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
                )


class TestNVMEOFService:

    mgr = FakeMgr()
    nvmeof_service = NvmeofService(mgr)

    nvmeof_spec = NvmeofServiceSpec(service_type='nvmeof', service_id="a")
    nvmeof_spec.daemon_type = 'nvmeof'
    nvmeof_spec.daemon_id = "a"
    nvmeof_spec.spec = MagicMock()
    nvmeof_spec.spec.daemon_type = 'nvmeof'

    mgr.spec_store = MagicMock()
    mgr.spec_store.all_specs.get.return_value = nvmeof_spec

    def test_nvmeof_client_caps(self):
        pass

    @patch('cephadm.utils.resolve_ip')
    def test_nvmeof_dashboard_config(self, mock_resolve_ip):
        pass

    @patch("cephadm.inventory.Inventory.get_addr", lambda _, __: '192.168.100.100')
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.module.CephadmOrchestrator.get_unique_name")
    def test_nvmeof_config(self, _get_name, _run_cephadm, cephadm_module: CephadmOrchestrator):

        nvmeof_daemon_id = 'testpool.test.qwert'
        pool = 'testpool'
        tgt_cmd_extra_args = '--cpumask=0xFF --msg-mempool-size=524288'
        default_port = 5500
        group = 'mygroup'
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        _get_name.return_value = nvmeof_daemon_id

        nvmeof_gateway_conf = f"""# This file is generated by cephadm.
[gateway]
name = client.nvmeof.{nvmeof_daemon_id}
group = {group}
addr = 192.168.100.100
port = {default_port}
enable_auth = False
state_update_notify = True
state_update_interval_sec = 5
min_controller_id = 1
max_controller_id = 65519
enable_spdk_discovery_controller = False
enable_prometheus_exporter = True
prometheus_exporter_ssl = False
prometheus_port = 10008

[ceph]
pool = {pool}
config_file = /etc/ceph/ceph.conf
id = nvmeof.{nvmeof_daemon_id}

[mtls]
server_key = ./server.key
client_key = ./client.key
server_cert = ./server.crt
client_cert = ./client.crt

[spdk]
tgt_path = /usr/local/bin/nvmf_tgt
rpc_socket = /var/tmp/spdk.sock
timeout = 60
log_level = WARN
conn_retries = 10
transports = tcp
transport_tcp_options = {{"in_capsule_data_size": 8192, "max_io_qpairs_per_ctrlr": 7}}
tgt_cmd_extra_args = {tgt_cmd_extra_args}\n"""

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, NvmeofServiceSpec(service_id=pool,
                                                                tgt_cmd_extra_args=tgt_cmd_extra_args,
                                                                group=group,
                                                                pool=pool)):
                _run_cephadm.assert_called_with(
                    'test',
                    f'nvmeof.{nvmeof_daemon_id}',
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": "nvmeof.testpool.test.qwert",
                        "image": "",
                        "deploy_arguments": [],
                        "params": {
                            "tcp_ports": [5500, 4420, 8009]
                        },
                        "meta": {
                            "service_name": "nvmeof.testpool",
                            "ports": [5500, 4420, 8009],
                            "ip": None,
                            "deployed_by": [],
                            "rank": None,
                            "rank_generation": None,
                            "extra_container_args": None,
                            "extra_entrypoint_args": None
                        },
                        "config_blobs": {
                            "config": "",
                            "keyring": "[client.nvmeof.testpool.test.qwert]\nkey = None\n",
                            "files": {
                                "ceph-nvmeof.conf": nvmeof_gateway_conf
                            }
                        }
                    }),
                )


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
            with with_service(cephadm_module, AlertManagerSpec()):
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
                            "peers": [],
                        }
                    }),
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("socket.getfqdn")
    @patch("cephadm.module.CephadmOrchestrator.get_mgr_ip", lambda _: '::1')
    @patch("cephadm.services.monitoring.password_hash", lambda password: 'alertmanager_password_hash')
    def test_alertmanager_config_security_enabled(self, _get_fqdn, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        fqdn = 'host1.test'
        _get_fqdn.return_value = fqdn

        def gen_cert(host, addr):
            return ('mycert', 'mykey')

        def get_root_cert():
            return 'my_root_cert'

        with with_host(cephadm_module, 'test'):
            cephadm_module.secure_monitoring_stack = True
            cephadm_module.set_store(AlertmanagerService.USER_CFG_KEY, 'alertmanager_user')
            cephadm_module.set_store(AlertmanagerService.PASS_CFG_KEY, 'alertmanager_plain_password')
            cephadm_module.http_server.service_discovery.ssl_certs.generate_cert = MagicMock(side_effect=gen_cert)
            cephadm_module.http_server.service_discovery.ssl_certs.get_root_cert = MagicMock(side_effect=get_root_cert)
            with with_service(cephadm_module, AlertManagerSpec()):

                y = dedent(f"""
                # This file is generated by cephadm.
                # See https://prometheus.io/docs/alerting/configuration/ for documentation.

                global:
                  resolve_timeout: 5m
                  http_config:
                    tls_config:
                      ca_file: root_cert.pem

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
                - name: 'ceph-dashboard'
                  webhook_configs:
                  - url: 'http://{fqdn}:8080/api/prometheus_receiver'
                """).lstrip()

                web_config = dedent("""
                tls_server_config:
                  cert_file: alertmanager.crt
                  key_file: alertmanager.key
                basic_auth_users:
                    alertmanager_user: alertmanager_password_hash""").lstrip()

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
                                'root_cert.pem': 'my_root_cert'
                            },
                            'peers': [],
                            'web_config': '/etc/alertmanager/web.yml',
                        }
                    }),
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.module.CephadmOrchestrator.get_mgr_ip", lambda _: '::1')
    def test_prometheus_config_security_disabled(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
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
                                                             backend_service='rgw.foo')) as _, \
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
                        - url: http://[::1]:8765/sd/prometheus/sd-config?service=alertmanager

                scrape_configs:
                  - job_name: 'ceph'
                    honor_labels: true
                    relabel_configs:
                    - source_labels: [__address__]
                      target_label: cluster
                      replacement: fsid
                    http_sd_configs:
                    - url: http://[::1]:8765/sd/prometheus/sd-config?service=mgr-prometheus

                  - job_name: 'node'
                    http_sd_configs:
                    - url: http://[::1]:8765/sd/prometheus/sd-config?service=node-exporter
                    relabel_configs:
                    - source_labels: [__address__]
                      target_label: cluster
                      replacement: fsid

                  - job_name: 'haproxy'
                    http_sd_configs:
                    - url: http://[::1]:8765/sd/prometheus/sd-config?service=haproxy
                    relabel_configs:
                    - source_labels: [__address__]
                      target_label: cluster
                      replacement: fsid

                  - job_name: 'ceph-exporter'
                    honor_labels: true
                    relabel_configs:
                    - source_labels: [__address__]
                      target_label: cluster
                      replacement: fsid
                    http_sd_configs:
                    - url: http://[::1]:8765/sd/prometheus/sd-config?service=ceph-exporter

                  - job_name: 'nvmeof'
                    http_sd_configs:
                    - url: http://[::1]:8765/sd/prometheus/sd-config?service=nvmeof

                  - job_name: 'federate'
                    scrape_interval: 15s
                    honor_labels: true
                    metrics_path: '/federate'
                    params:
                      'match[]':
                        - '{job="ceph"}'
                        - '{job="node"}'
                        - '{job="haproxy"}'
                        - '{job="ceph-exporter"}'
                    static_configs:
                    - targets: []
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
                            'enable_admin_api': False,
                        },
                    }),
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.module.CephadmOrchestrator.get_mgr_ip", lambda _: '::1')
    @patch("cephadm.services.monitoring.password_hash", lambda password: 'prometheus_password_hash')
    def test_prometheus_config_security_enabled(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        s = RGWSpec(service_id="foo", placement=PlacementSpec(count=1), rgw_frontend_type='beast')

        def gen_cert(host, addr):
            return ('mycert', 'mykey')

        with with_host(cephadm_module, 'test'):
            cephadm_module.secure_monitoring_stack = True
            cephadm_module.set_store(PrometheusService.USER_CFG_KEY, 'prometheus_user')
            cephadm_module.set_store(PrometheusService.PASS_CFG_KEY, 'prometheus_plain_password')
            cephadm_module.set_store(AlertmanagerService.USER_CFG_KEY, 'alertmanager_user')
            cephadm_module.set_store(AlertmanagerService.PASS_CFG_KEY, 'alertmanager_plain_password')
            cephadm_module.http_server.service_discovery.username = 'sd_user'
            cephadm_module.http_server.service_discovery.password = 'sd_password'
            cephadm_module.http_server.service_discovery.ssl_certs.generate_cert = MagicMock(
                side_effect=gen_cert)
            # host "test" needs to have networks for keepalive to be placed
            cephadm_module.cache.update_host_networks('test', {
                '1.2.3.0/24': {
                    'if0': ['1.2.3.1']
                },
            })
            with with_service(cephadm_module, MonitoringSpec('node-exporter')) as _, \
                    with_service(cephadm_module, s) as _, \
                    with_service(cephadm_module, AlertManagerSpec('alertmanager')) as _, \
                    with_service(cephadm_module, IngressSpec(service_id='ingress',
                                                             frontend_port=8089,
                                                             monitor_port=8999,
                                                             monitor_user='admin',
                                                             monitor_password='12345',
                                                             keepalived_password='12345',
                                                             virtual_ip="1.2.3.4/32",
                                                             backend_service='rgw.foo')) as _, \
                    with_service(cephadm_module, PrometheusSpec('prometheus')) as _:

                web_config = dedent("""
                tls_server_config:
                  cert_file: prometheus.crt
                  key_file: prometheus.key
                basic_auth_users:
                    prometheus_user: prometheus_password_hash""").lstrip()

                y = dedent("""
                # This file is generated by cephadm.
                global:
                  scrape_interval: 10s
                  evaluation_interval: 10s

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
                      http_sd_configs:
                        - url: https://[::1]:8765/sd/prometheus/sd-config?service=alertmanager
                          basic_auth:
                            username: sd_user
                            password: sd_password
                          tls_config:
                            ca_file: root_cert.pem

                scrape_configs:
                  - job_name: 'ceph'
                    scheme: https
                    tls_config:
                      ca_file: mgr_prometheus_cert.pem
                    honor_labels: true
                    http_sd_configs:
                    - url: https://[::1]:8765/sd/prometheus/sd-config?service=mgr-prometheus
                      basic_auth:
                        username: sd_user
                        password: sd_password
                      tls_config:
                        ca_file: root_cert.pem

                  - job_name: 'node'
                    scheme: https
                    tls_config:
                      ca_file: root_cert.pem
                    http_sd_configs:
                    - url: https://[::1]:8765/sd/prometheus/sd-config?service=node-exporter
                      basic_auth:
                        username: sd_user
                        password: sd_password
                      tls_config:
                        ca_file: root_cert.pem

                  - job_name: 'haproxy'
                    scheme: https
                    tls_config:
                      ca_file: root_cert.pem
                    http_sd_configs:
                    - url: https://[::1]:8765/sd/prometheus/sd-config?service=haproxy
                      basic_auth:
                        username: sd_user
                        password: sd_password
                      tls_config:
                        ca_file: root_cert.pem

                  - job_name: 'ceph-exporter'
                    honor_labels: true
                    scheme: https
                    tls_config:
                      ca_file: root_cert.pem
                    http_sd_configs:
                    - url: https://[::1]:8765/sd/prometheus/sd-config?service=ceph-exporter
                      basic_auth:
                        username: sd_user
                        password: sd_password
                      tls_config:
                        ca_file: root_cert.pem

                  - job_name: 'nvmeof'
                    honor_labels: true
                    scheme: https
                    tls_config:
                      ca_file: root_cert.pem
                    http_sd_configs:
                    - url: https://[::1]:8765/sd/prometheus/sd-config?service=nvmeof
                      basic_auth:
                        username: sd_user
                        password: sd_password
                      tls_config:
                        ca_file: root_cert.pem

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
                                'root_cert.pem': '',
                                'mgr_prometheus_cert.pem': '',
                                'web.yml': web_config,
                                'prometheus.crt': 'mycert',
                                'prometheus.key': 'mykey',
                                "/etc/prometheus/alerting/custom_alerts.yml": "",
                            },
                            'retention_time': '15d',
                            'retention_size': '0',
                            'ip_to_bind_to': '',
                            'enable_admin_api': False,
                            'web_config': '/etc/prometheus/web.yml',
                        },
                    }),
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
                  path_prefix: /tmp/loki
                  storage:
                    filesystem:
                      chunks_directory: /tmp/loki/chunks
                      rules_directory: /tmp/loki/rules
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
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.module.CephadmOrchestrator.get_mgr_ip", lambda _: '1::4')
    @patch("cephadm.services.monitoring.verify_tls", lambda *_: None)
    def test_grafana_config(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(("{}", "", 0))

        with with_host(cephadm_module, "test"):
            cephadm_module.set_store("test/grafana_crt", grafana_cert)
            cephadm_module.set_store("test/grafana_key", grafana_key)
            with with_service(
                cephadm_module, PrometheusSpec("prometheus")
            ) as _, with_service(cephadm_module, ServiceSpec("mgr")) as _, with_service(
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
                          domain = 'bootstrap.storage.lab'
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
                          allow_embedding = true""").lstrip(),  # noqa: W291
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
                            url: 'http://[1::4]:9095'
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
                        # generated by cephadm\n{grafana_cert}""").lstrip(),
                    'certs/cert_key': dedent(f"""
                        # generated by cephadm\n{grafana_key}""").lstrip(),
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
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_grafana_initial_admin_pw(self, cephadm_module: CephadmOrchestrator):
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, ServiceSpec('mgr')) as _, \
                    with_service(cephadm_module, GrafanaSpec(initial_admin_password='secure')):
                out = cephadm_module.cephadm_services['grafana'].generate_config(
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
                                    "  domain = 'bootstrap.storage.lab'\n"
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
                                    '  allow_embedding = true',
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
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, ServiceSpec('mgr')) as _, \
                    with_service(cephadm_module, GrafanaSpec(anonymous_access=False, initial_admin_password='secure')):
                out = cephadm_module.cephadm_services['grafana'].generate_config(
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
                                    "  domain = 'bootstrap.storage.lab'\n"
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
                                    '  allow_embedding = true',
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
                                'reconfig': True,
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
                    )


class TestRGWService:

    @pytest.mark.parametrize(
        "frontend, ssl, extra_args, expected",
        [
            ('beast', False, ['tcp_nodelay=1'],
             'beast endpoint=[fd00:fd00:fd00:3000::1]:80 tcp_nodelay=1'),
            ('beast', True, ['tcp_nodelay=0', 'max_header_size=65536'],
             'beast ssl_endpoint=[fd00:fd00:fd00:3000::1]:443 ssl_certificate=config://rgw/cert/rgw.foo tcp_nodelay=0 max_header_size=65536'),
            ('civetweb', False, [], 'civetweb port=[fd00:fd00:fd00:3000::1]:80'),
            ('civetweb', True, None,
             'civetweb port=[fd00:fd00:fd00:3000::1]:443s ssl_certificate=config://rgw/cert/rgw.foo'),
        ]
    )
    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_rgw_update(self, frontend, ssl, extra_args, expected, cephadm_module: CephadmOrchestrator):
        with with_host(cephadm_module, 'host1'):
            cephadm_module.cache.update_host_networks('host1', {
                'fd00:fd00:fd00:3000::/64': {
                    'if0': ['fd00:fd00:fd00:3000::1']
                }
            })
            s = RGWSpec(service_id="foo",
                        networks=['fd00:fd00:fd00:3000::/64'],
                        ssl=ssl,
                        rgw_frontend_type=frontend,
                        rgw_frontend_extra_args=extra_args)
            with with_service(cephadm_module, s) as dds:
                _, f, _ = cephadm_module.check_mon_command({
                    'prefix': 'config get',
                    'who': f'client.{dds[0]}',
                    'key': 'rgw_frontends',
                })
                assert f == expected


class TestMonService:

    def test_set_crush_locations(self, cephadm_module: CephadmOrchestrator):
        mgr = FakeMgr()
        mon_service = MonService(mgr)
        mon_spec = ServiceSpec(service_type='mon', crush_locations={'vm-00': ['datacenter=a', 'rack=1'], 'vm-01': ['datacenter=a'], 'vm-02': ['datacenter=b', 'rack=3']})

        mon_daemons = [
            DaemonDescription(daemon_type='mon', daemon_id='vm-00', hostname='vm-00'),
            DaemonDescription(daemon_type='mon', daemon_id='vm-01', hostname='vm-01'),
            DaemonDescription(daemon_type='mon', daemon_id='vm-02', hostname='vm-02')
        ]
        mon_service.set_crush_locations(mon_daemons, mon_spec)
        assert 'vm-00' in mgr.set_mon_crush_locations
        assert mgr.set_mon_crush_locations['vm-00'] == ['datacenter=a', 'rack=1']
        assert 'vm-01' in mgr.set_mon_crush_locations
        assert mgr.set_mon_crush_locations['vm-01'] == ['datacenter=a']
        assert 'vm-02' in mgr.set_mon_crush_locations
        assert mgr.set_mon_crush_locations['vm-02'] == ['datacenter=b', 'rack=3']


class TestSNMPGateway:

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_snmp_v2c_deployment(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        spec = SNMPGatewaySpec(
            snmp_version='V2c',
            snmp_destination='192.168.1.1:162',
            credentials={
                'snmp_community': 'public'
            })

        config = {
            "destination": spec.snmp_destination,
            "snmp_version": spec.snmp_version,
            "snmp_community": spec.credentials.get('snmp_community')
        }

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, spec):
                _run_cephadm.assert_called_with(
                    'test',
                    "snmp-gateway.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'snmp-gateway.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [9464],
                        },
                        "meta": {
                            'service_name': 'snmp-gateway',
                            'ports': [9464],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": config,
                    }),
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_snmp_v2c_with_port(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        spec = SNMPGatewaySpec(
            snmp_version='V2c',
            snmp_destination='192.168.1.1:162',
            credentials={
                'snmp_community': 'public'
            },
            port=9465)

        config = {
            "destination": spec.snmp_destination,
            "snmp_version": spec.snmp_version,
            "snmp_community": spec.credentials.get('snmp_community')
        }

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, spec):
                _run_cephadm.assert_called_with(
                    'test',
                    "snmp-gateway.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'snmp-gateway.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [9465],
                        },
                        "meta": {
                            'service_name': 'snmp-gateway',
                            'ports': [9465],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": config,
                    }),
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_snmp_v3nopriv_deployment(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        spec = SNMPGatewaySpec(
            snmp_version='V3',
            snmp_destination='192.168.1.1:162',
            engine_id='8000C53F00000000',
            credentials={
                'snmp_v3_auth_username': 'myuser',
                'snmp_v3_auth_password': 'mypassword'
            })

        config = {
            'destination': spec.snmp_destination,
            'snmp_version': spec.snmp_version,
            'snmp_v3_auth_protocol': 'SHA',
            'snmp_v3_auth_username': 'myuser',
            'snmp_v3_auth_password': 'mypassword',
            'snmp_v3_engine_id': '8000C53F00000000'
        }

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, spec):
                _run_cephadm.assert_called_with(
                    'test',
                    "snmp-gateway.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'snmp-gateway.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [9464],
                        },
                        "meta": {
                            'service_name': 'snmp-gateway',
                            'ports': [9464],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": config,
                    }),
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_snmp_v3priv_deployment(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        spec = SNMPGatewaySpec(
            snmp_version='V3',
            snmp_destination='192.168.1.1:162',
            engine_id='8000C53F00000000',
            auth_protocol='MD5',
            privacy_protocol='AES',
            credentials={
                'snmp_v3_auth_username': 'myuser',
                'snmp_v3_auth_password': 'mypassword',
                'snmp_v3_priv_password': 'mysecret',
            })

        config = {
            'destination': spec.snmp_destination,
            'snmp_version': spec.snmp_version,
            'snmp_v3_auth_protocol': 'MD5',
            'snmp_v3_auth_username': spec.credentials.get('snmp_v3_auth_username'),
            'snmp_v3_auth_password': spec.credentials.get('snmp_v3_auth_password'),
            'snmp_v3_engine_id': '8000C53F00000000',
            'snmp_v3_priv_protocol': spec.privacy_protocol,
            'snmp_v3_priv_password': spec.credentials.get('snmp_v3_priv_password'),
        }

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, spec):
                _run_cephadm.assert_called_with(
                    'test',
                    "snmp-gateway.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'snmp-gateway.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [9464],
                        },
                        "meta": {
                            'service_name': 'snmp-gateway',
                            'ports': [9464],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": config,
                    }),
                )


class TestIngressService:

    @pytest.mark.parametrize(
        "enable_haproxy_protocol",
        [False, True],
    )
    @patch("cephadm.inventory.Inventory.get_addr")
    @patch("cephadm.utils.resolve_ip")
    @patch("cephadm.inventory.HostCache.get_daemons_by_service")
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_ingress_config_nfs_multiple_nfs_same_rank(
        self,
        _run_cephadm,
        _get_daemons_by_service,
        _resolve_ip, _get_addr,
        cephadm_module: CephadmOrchestrator,
        enable_haproxy_protocol: bool,
    ):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        def fake_resolve_ip(hostname: str) -> str:
            if hostname == 'host1':
                return '192.168.122.111'
            elif hostname == 'host2':
                return '192.168.122.222'
            else:
                return 'xxx.xxx.xxx.xxx'
        _resolve_ip.side_effect = fake_resolve_ip

        def fake_get_addr(hostname: str) -> str:
            return hostname
        _get_addr.side_effect = fake_get_addr

        nfs_service = NFSServiceSpec(
            service_id="foo",
            placement=PlacementSpec(
                count=1,
                hosts=['host1', 'host2']),
            port=12049,
            enable_haproxy_protocol=enable_haproxy_protocol,
        )

        ispec = IngressSpec(
            service_type='ingress',
            service_id='nfs.foo',
            backend_service='nfs.foo',
            frontend_port=2049,
            monitor_port=9049,
            virtual_ip='192.168.122.100/24',
            monitor_user='admin',
            monitor_password='12345',
            keepalived_password='12345',
            enable_haproxy_protocol=enable_haproxy_protocol,
        )

        cephadm_module.spec_store._specs = {
            'nfs.foo': nfs_service,
            'ingress.nfs.foo': ispec
        }
        cephadm_module.spec_store.spec_created = {
            'nfs.foo': datetime_now(),
            'ingress.nfs.foo': datetime_now()
        }

        # in both test cases we'll do here, we want only the ip
        # for the host1 nfs daemon as we'll end up giving that
        # one higher rank_generation but the same rank as the one
        # on host2
        haproxy_txt = (
            '# This file is generated by cephadm.\n'
            'global\n'
            '    log         127.0.0.1 local2\n'
            '    chroot      /var/lib/haproxy\n'
            '    pidfile     /var/lib/haproxy/haproxy.pid\n'
            '    maxconn     8000\n'
            '    daemon\n'
            '    stats socket /var/lib/haproxy/stats\n\n'
            'defaults\n'
            '    mode                    tcp\n'
            '    log                     global\n'
            '    timeout queue           1m\n'
            '    timeout connect         10s\n'
            '    timeout client          1m\n'
            '    timeout server          1m\n'
            '    timeout check           10s\n'
            '    maxconn                 8000\n\n'
            'frontend stats\n'
            '    mode http\n'
            '    bind 192.168.122.100:9049\n'
            '    bind host1:9049\n'
            '    stats enable\n'
            '    stats uri /stats\n'
            '    stats refresh 10s\n'
            '    stats auth admin:12345\n'
            '    http-request use-service prometheus-exporter if { path /metrics }\n'
            '    monitor-uri /health\n\n'
            'frontend frontend\n'
            '    bind 192.168.122.100:2049\n'
            '    default_backend backend\n\n'
            'backend backend\n'
            '    mode        tcp\n'
            '    balance     source\n'
            '    hash-type   consistent\n'
        )
        if enable_haproxy_protocol:
            haproxy_txt += '    default-server send-proxy-v2\n'
        haproxy_txt += '    server nfs.foo.0 192.168.122.111:12049 check\n'
        haproxy_expected_conf = {
            'files': {'haproxy.cfg': haproxy_txt}
        }

        # verify we get the same cfg regardless of the order in which the nfs daemons are returned
        # in this case both nfs are rank 0, so it should only take the one with rank_generation 1 a.k.a
        # the one on host1
        nfs_daemons = [
            DaemonDescription(daemon_type='nfs', daemon_id='foo.0.1.host1.qwerty', hostname='host1', rank=0, rank_generation=1, ports=[12049]),
            DaemonDescription(daemon_type='nfs', daemon_id='foo.0.0.host2.abcdef', hostname='host2', rank=0, rank_generation=0, ports=[12049])
        ]
        _get_daemons_by_service.return_value = nfs_daemons

        haproxy_generated_conf = cephadm_module.cephadm_services['ingress'].haproxy_generate_config(
            CephadmDaemonDeploySpec(host='host1', daemon_id='ingress', service_name=ispec.service_name()))

        assert haproxy_generated_conf[0] == haproxy_expected_conf

        # swapping order now, should still pick out the one with the higher rank_generation
        # in this case both nfs are rank 0, so it should only take the one with rank_generation 1 a.k.a
        # the one on host1
        nfs_daemons = [
            DaemonDescription(daemon_type='nfs', daemon_id='foo.0.0.host2.abcdef', hostname='host2', rank=0, rank_generation=0, ports=[12049]),
            DaemonDescription(daemon_type='nfs', daemon_id='foo.0.1.host1.qwerty', hostname='host1', rank=0, rank_generation=1, ports=[12049])
        ]
        _get_daemons_by_service.return_value = nfs_daemons

        haproxy_generated_conf = cephadm_module.cephadm_services['ingress'].haproxy_generate_config(
            CephadmDaemonDeploySpec(host='host1', daemon_id='ingress', service_name=ispec.service_name()))

        assert haproxy_generated_conf[0] == haproxy_expected_conf

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_ingress_config(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test', addr='1.2.3.7'):
            cephadm_module.cache.update_host_networks('test', {
                '1.2.3.0/24': {
                    'if0': ['1.2.3.4']
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
                # generate the keepalived conf based on the specified spec
                keepalived_generated_conf = cephadm_module.cephadm_services['ingress'].keepalived_generate_config(
                    CephadmDaemonDeploySpec(host='test', daemon_id='ingress', service_name=ispec.service_name()))

                keepalived_expected_conf = {
                    'files':
                        {
                            'keepalived.conf':
                                '# This file is generated by cephadm.\n'
                                'vrrp_script check_backend {\n    '
                                'script "/usr/bin/curl http://1.2.3.7:8999/health"\n    '
                                'weight -20\n    '
                                'interval 2\n    '
                                'rise 2\n    '
                                'fall 2\n}\n\n'
                                'vrrp_instance VI_0 {\n  '
                                'state MASTER\n  '
                                'priority 100\n  '
                                'interface if0\n  '
                                'virtual_router_id 50\n  '
                                'advert_int 1\n  '
                                'authentication {\n      '
                                'auth_type PASS\n      '
                                'auth_pass 12345\n  '
                                '}\n  '
                                'unicast_src_ip 1.2.3.4\n  '
                                'unicast_peer {\n  '
                                '}\n  '
                                'virtual_ipaddress {\n    '
                                '1.2.3.4/32 dev if0\n  '
                                '}\n  '
                                'track_script {\n      '
                                'check_backend\n  }\n'
                                '}\n'
                        }
                }

                # check keepalived config
                assert keepalived_generated_conf[0] == keepalived_expected_conf

                # generate the haproxy conf based on the specified spec
                haproxy_generated_conf = cephadm_module.cephadm_services['ingress'].haproxy_generate_config(
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
                                'bind 1.2.3.4:8999\n    '
                                'bind 1.2.3.7:8999\n    '
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

                assert haproxy_generated_conf[0] == haproxy_expected_conf

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_ingress_config_ssl_rgw(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test'):
            cephadm_module.cache.update_host_networks('test', {
                '1.2.3.0/24': {
                    'if0': ['1.2.3.1']
                }
            })

            # the ingress backend
            s = RGWSpec(service_id="foo", placement=PlacementSpec(count=1),
                        rgw_frontend_type='beast', rgw_frontend_port=443, ssl=True)

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
                # generate the keepalived conf based on the specified spec
                keepalived_generated_conf = cephadm_module.cephadm_services['ingress'].keepalived_generate_config(
                    CephadmDaemonDeploySpec(host='test', daemon_id='ingress', service_name=ispec.service_name()))

                keepalived_expected_conf = {
                    'files':
                        {
                            'keepalived.conf':
                                '# This file is generated by cephadm.\n'
                                'vrrp_script check_backend {\n    '
                                'script "/usr/bin/curl http://[1::4]:8999/health"\n    '
                                'weight -20\n    '
                                'interval 2\n    '
                                'rise 2\n    '
                                'fall 2\n}\n\n'
                                'vrrp_instance VI_0 {\n  '
                                'state MASTER\n  '
                                'priority 100\n  '
                                'interface if0\n  '
                                'virtual_router_id 50\n  '
                                'advert_int 1\n  '
                                'authentication {\n      '
                                'auth_type PASS\n      '
                                'auth_pass 12345\n  '
                                '}\n  '
                                'unicast_src_ip 1.2.3.1\n  '
                                'unicast_peer {\n  '
                                '}\n  '
                                'virtual_ipaddress {\n    '
                                '1.2.3.4/32 dev if0\n  '
                                '}\n  '
                                'track_script {\n      '
                                'check_backend\n  }\n'
                                '}\n'
                        }
                }

                # check keepalived config
                assert keepalived_generated_conf[0] == keepalived_expected_conf

                # generate the haproxy conf based on the specified spec
                haproxy_generated_conf = cephadm_module.cephadm_services['ingress'].haproxy_generate_config(
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
                                'bind 1.2.3.4:8999\n    '
                                'bind 1::4:8999\n    '
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
                                'default-server ssl\n    '
                                'default-server verify none\n    '
                                'balance static-rr\n    '
                                'option httpchk HEAD / HTTP/1.0\n    '
                                'server '
                                + haproxy_generated_conf[1][0] + ' 1::4:443 check weight 100 inter 2s\n'
                        }
                }

                assert haproxy_generated_conf[0] == haproxy_expected_conf

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_ingress_config_multi_vips(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test', addr='1.2.3.7'):
            cephadm_module.cache.update_host_networks('test', {
                '1.2.3.0/24': {
                    'if0': ['1.2.3.1']
                }
            })

            # Check the ingress with multiple VIPs
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
                                virtual_ips_list=["1.2.3.4/32"])
            with with_service(cephadm_module, s) as _, with_service(cephadm_module, ispec) as _:
                # generate the keepalived conf based on the specified spec
                # Test with only 1 IP on the list, as it will fail with more VIPS but only one host.
                keepalived_generated_conf = cephadm_module.cephadm_services['ingress'].keepalived_generate_config(
                    CephadmDaemonDeploySpec(host='test', daemon_id='ingress', service_name=ispec.service_name()))

                keepalived_expected_conf = {
                    'files':
                        {
                            'keepalived.conf':
                                '# This file is generated by cephadm.\n'
                                'vrrp_script check_backend {\n    '
                                'script "/usr/bin/curl http://1.2.3.7:8999/health"\n    '
                                'weight -20\n    '
                                'interval 2\n    '
                                'rise 2\n    '
                                'fall 2\n}\n\n'
                                'vrrp_instance VI_0 {\n  '
                                'state MASTER\n  '
                                'priority 100\n  '
                                'interface if0\n  '
                                'virtual_router_id 50\n  '
                                'advert_int 1\n  '
                                'authentication {\n      '
                                'auth_type PASS\n      '
                                'auth_pass 12345\n  '
                                '}\n  '
                                'unicast_src_ip 1.2.3.1\n  '
                                'unicast_peer {\n  '
                                '}\n  '
                                'virtual_ipaddress {\n    '
                                '1.2.3.4/32 dev if0\n  '
                                '}\n  '
                                'track_script {\n      '
                                'check_backend\n  }\n'
                                '}\n'
                        }
                }

                # check keepalived config
                assert keepalived_generated_conf[0] == keepalived_expected_conf

                # generate the haproxy conf based on the specified spec
                haproxy_generated_conf = cephadm_module.cephadm_services['ingress'].haproxy_generate_config(
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
                                'bind [..]:8999\n    '
                                'bind 1.2.3.7:8999\n    '
                                'stats enable\n    '
                                'stats uri /stats\n    '
                                'stats refresh 10s\n    '
                                'stats auth admin:12345\n    '
                                'http-request use-service prometheus-exporter if { path /metrics }\n    '
                                'monitor-uri /health\n'
                                '\nfrontend frontend\n    '
                                'bind [..]:8089\n    '
                                'default_backend backend\n\n'
                                'backend backend\n    '
                                'option forwardfor\n    '
                                'balance static-rr\n    '
                                'option httpchk HEAD / HTTP/1.0\n    '
                                'server '
                                + haproxy_generated_conf[1][0] + ' 1.2.3.7:80 check weight 100 inter 2s\n'
                        }
                }

                assert haproxy_generated_conf[0] == haproxy_expected_conf

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_keepalive_config_multi_interface_vips(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test', addr='1.2.3.1'):
            with with_host(cephadm_module, 'test2', addr='1.2.3.2'):
                cephadm_module.cache.update_host_networks('test', {
                    '1.2.3.0/24': {
                        'if0': ['1.2.3.1']
                    },
                    '100.100.100.0/24': {
                        'if1': ['100.100.100.1']
                    }
                })
                cephadm_module.cache.update_host_networks('test2', {
                    '1.2.3.0/24': {
                        'if0': ['1.2.3.2']
                    },
                    '100.100.100.0/24': {
                        'if1': ['100.100.100.2']
                    }
                })

                # Check the ingress with multiple VIPs
                s = RGWSpec(service_id="foo", placement=PlacementSpec(count=1),
                            rgw_frontend_type='beast')

                ispec = IngressSpec(service_type='ingress',
                                    service_id='test',
                                    placement=PlacementSpec(hosts=['test', 'test2']),
                                    backend_service='rgw.foo',
                                    frontend_port=8089,
                                    monitor_port=8999,
                                    monitor_user='admin',
                                    monitor_password='12345',
                                    keepalived_password='12345',
                                    virtual_ips_list=["1.2.3.100/24", "100.100.100.100/24"])
                with with_service(cephadm_module, s) as _, with_service(cephadm_module, ispec) as _:
                    keepalived_generated_conf = cephadm_module.cephadm_services['ingress'].keepalived_generate_config(
                        CephadmDaemonDeploySpec(host='test', daemon_id='ingress', service_name=ispec.service_name()))

                    keepalived_expected_conf = {
                        'files':
                            {
                                'keepalived.conf':
                                    '# This file is generated by cephadm.\n'
                                    'vrrp_script check_backend {\n    '
                                    'script "/usr/bin/curl http://1.2.3.1:8999/health"\n    '
                                    'weight -20\n    '
                                    'interval 2\n    '
                                    'rise 2\n    '
                                    'fall 2\n}\n\n'
                                    'vrrp_instance VI_0 {\n  '
                                    'state MASTER\n  '
                                    'priority 100\n  '
                                    'interface if0\n  '
                                    'virtual_router_id 50\n  '
                                    'advert_int 1\n  '
                                    'authentication {\n      '
                                    'auth_type PASS\n      '
                                    'auth_pass 12345\n  '
                                    '}\n  '
                                    'unicast_src_ip 1.2.3.1\n  '
                                    'unicast_peer {\n    '
                                    '1.2.3.2\n  '
                                    '}\n  '
                                    'virtual_ipaddress {\n    '
                                    '1.2.3.100/24 dev if0\n  '
                                    '}\n  '
                                    'track_script {\n      '
                                    'check_backend\n  }\n'
                                    '}\n'
                                    'vrrp_instance VI_1 {\n  '
                                    'state BACKUP\n  '
                                    'priority 90\n  '
                                    'interface if1\n  '
                                    'virtual_router_id 51\n  '
                                    'advert_int 1\n  '
                                    'authentication {\n      '
                                    'auth_type PASS\n      '
                                    'auth_pass 12345\n  '
                                    '}\n  '
                                    'unicast_src_ip 100.100.100.1\n  '
                                    'unicast_peer {\n    '
                                    '100.100.100.2\n  '
                                    '}\n  '
                                    'virtual_ipaddress {\n    '
                                    '100.100.100.100/24 dev if1\n  '
                                    '}\n  '
                                    'track_script {\n      '
                                    'check_backend\n  }\n'
                                    '}\n'
                            }
                    }

                    # check keepalived config
                    assert keepalived_generated_conf[0] == keepalived_expected_conf

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_keepalive_interface_host_filtering(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        # we need to make sure keepalive daemons will have an interface
        # on the hosts we deploy them on in order to set up their VIP.
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test', addr='1.2.3.1'):
            with with_host(cephadm_module, 'test2', addr='1.2.3.2'):
                with with_host(cephadm_module, 'test3', addr='1.2.3.3'):
                    with with_host(cephadm_module, 'test4', addr='1.2.3.3'):
                        # setup "test" and "test4" to have all the necessary interfaces,
                        # "test2" to have one of them (should still be filtered)
                        # and "test3" to have none of them
                        cephadm_module.cache.update_host_networks('test', {
                            '1.2.3.0/24': {
                                'if0': ['1.2.3.1']
                            },
                            '100.100.100.0/24': {
                                'if1': ['100.100.100.1']
                            }
                        })
                        cephadm_module.cache.update_host_networks('test2', {
                            '1.2.3.0/24': {
                                'if0': ['1.2.3.2']
                            },
                        })
                        cephadm_module.cache.update_host_networks('test4', {
                            '1.2.3.0/24': {
                                'if0': ['1.2.3.4']
                            },
                            '100.100.100.0/24': {
                                'if1': ['100.100.100.4']
                            }
                        })

                        s = RGWSpec(service_id="foo", placement=PlacementSpec(count=1),
                                    rgw_frontend_type='beast')

                        ispec = IngressSpec(service_type='ingress',
                                            service_id='test',
                                            placement=PlacementSpec(hosts=['test', 'test2', 'test3', 'test4']),
                                            backend_service='rgw.foo',
                                            frontend_port=8089,
                                            monitor_port=8999,
                                            monitor_user='admin',
                                            monitor_password='12345',
                                            keepalived_password='12345',
                                            virtual_ips_list=["1.2.3.100/24", "100.100.100.100/24"])
                        with with_service(cephadm_module, s) as _, with_service(cephadm_module, ispec) as _:
                            # since we're never actually going to refresh the host here,
                            # check the tmp daemons to see what was placed during the apply
                            daemons = cephadm_module.cache._get_tmp_daemons()
                            keepalive_daemons = [d for d in daemons if d.daemon_type == 'keepalived']
                            hosts_deployed_on = [d.hostname for d in keepalive_daemons]
                            assert 'test' in hosts_deployed_on
                            assert 'test2' not in hosts_deployed_on
                            assert 'test3' not in hosts_deployed_on
                            assert 'test4' in hosts_deployed_on

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_haproxy_port_ips(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test', addr='1.2.3.7'):
            cephadm_module.cache.update_host_networks('test', {
                '1.2.3.0/24': {
                    'if0': ['1.2.3.4/32']
                }
            })

            # Check the ingress with multiple VIPs
            s = RGWSpec(service_id="foo", placement=PlacementSpec(count=1),
                        rgw_frontend_type='beast')

            ip = '1.2.3.100'
            frontend_port = 8089

            ispec = IngressSpec(service_type='ingress',
                                service_id='test',
                                backend_service='rgw.foo',
                                frontend_port=frontend_port,
                                monitor_port=8999,
                                monitor_user='admin',
                                monitor_password='12345',
                                keepalived_password='12345',
                                virtual_ip=f"{ip}/24")
            with with_service(cephadm_module, s) as _, with_service(cephadm_module, ispec) as _:
                # generate the haproxy conf based on the specified spec
                haproxy_daemon_spec = cephadm_module.cephadm_services['ingress'].prepare_create(
                    CephadmDaemonDeploySpec(
                        host='test',
                        daemon_type='haproxy',
                        daemon_id='ingress',
                        service_name=ispec.service_name()))

                assert haproxy_daemon_spec.port_ips == {str(frontend_port): ip}

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.services.nfs.NFSService.fence_old_ranks", MagicMock())
    @patch("cephadm.services.nfs.NFSService.run_grace_tool", MagicMock())
    @patch("cephadm.services.nfs.NFSService.purge", MagicMock())
    @patch("cephadm.services.nfs.NFSService.create_rados_config_obj", MagicMock())
    def test_keepalive_only_nfs_config(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test', addr='1.2.3.7'):
            cephadm_module.cache.update_host_networks('test', {
                '1.2.3.0/24': {
                    'if0': ['1.2.3.1']
                }
            })

            # Check the ingress with multiple VIPs
            s = NFSServiceSpec(service_id="foo", placement=PlacementSpec(count=1),
                               virtual_ip='1.2.3.0/24')

            ispec = IngressSpec(service_type='ingress',
                                service_id='test',
                                backend_service='nfs.foo',
                                monitor_port=8999,
                                monitor_user='admin',
                                monitor_password='12345',
                                keepalived_password='12345',
                                virtual_ip='1.2.3.0/24',
                                keepalive_only=True)
            with with_service(cephadm_module, s) as _, with_service(cephadm_module, ispec) as _:
                nfs_generated_conf, _ = cephadm_module.cephadm_services['nfs'].generate_config(
                    CephadmDaemonDeploySpec(host='test', daemon_id='foo.test.0.0', service_name=s.service_name()))
                ganesha_conf = nfs_generated_conf['files']['ganesha.conf']
                assert "Bind_addr = 1.2.3.0/24" in ganesha_conf

                keepalived_generated_conf = cephadm_module.cephadm_services['ingress'].keepalived_generate_config(
                    CephadmDaemonDeploySpec(host='test', daemon_id='ingress', service_name=ispec.service_name()))

                keepalived_expected_conf = {
                    'files':
                        {
                            'keepalived.conf':
                                '# This file is generated by cephadm.\n'
                                'vrrp_script check_backend {\n    '
                                'script "/usr/bin/false"\n    '
                                'weight -20\n    '
                                'interval 2\n    '
                                'rise 2\n    '
                                'fall 2\n}\n\n'
                                'vrrp_instance VI_0 {\n  '
                                'state MASTER\n  '
                                'priority 100\n  '
                                'interface if0\n  '
                                'virtual_router_id 50\n  '
                                'advert_int 1\n  '
                                'authentication {\n      '
                                'auth_type PASS\n      '
                                'auth_pass 12345\n  '
                                '}\n  '
                                'unicast_src_ip 1.2.3.1\n  '
                                'unicast_peer {\n  '
                                '}\n  '
                                'virtual_ipaddress {\n    '
                                '1.2.3.0/24 dev if0\n  '
                                '}\n  '
                                'track_script {\n      '
                                'check_backend\n  }\n'
                                '}\n'
                        }
                }

                # check keepalived config
                assert keepalived_generated_conf[0] == keepalived_expected_conf

    @patch("cephadm.services.nfs.NFSService.fence_old_ranks", MagicMock())
    @patch("cephadm.services.nfs.NFSService.run_grace_tool", MagicMock())
    @patch("cephadm.services.nfs.NFSService.purge", MagicMock())
    @patch("cephadm.services.nfs.NFSService.create_rados_config_obj", MagicMock())
    @patch("cephadm.inventory.Inventory.keys")
    @patch("cephadm.inventory.Inventory.get_addr")
    @patch("cephadm.utils.resolve_ip")
    @patch("cephadm.inventory.HostCache.get_daemons_by_service")
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_ingress_config_nfs_proxy_protocol(
        self,
        _run_cephadm,
        _get_daemons_by_service,
        _resolve_ip,
        _get_addr,
        _inventory_keys,
        cephadm_module: CephadmOrchestrator,
    ):
        """Verify that setting enable_haproxy_protocol for both ingress and
        nfs services sets the desired configuration parameters in both
        the haproxy config and nfs ganesha config.
        """
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        def fake_resolve_ip(hostname: str) -> str:
            if hostname in ('host1', "192.168.122.111"):
                return '192.168.122.111'
            elif hostname in ('host2', '192.168.122.222'):
                return '192.168.122.222'
            else:
                raise KeyError(hostname)
        _resolve_ip.side_effect = fake_resolve_ip
        _get_addr.side_effect = fake_resolve_ip

        def fake_keys():
            return ['host1', 'host2']
        _inventory_keys.side_effect = fake_keys

        nfs_service = NFSServiceSpec(
            service_id="foo",
            placement=PlacementSpec(
                count=1,
                hosts=['host1', 'host2']),
            port=12049,
            enable_haproxy_protocol=True,
        )

        ispec = IngressSpec(
            service_type='ingress',
            service_id='nfs.foo',
            backend_service='nfs.foo',
            frontend_port=2049,
            monitor_port=9049,
            virtual_ip='192.168.122.100/24',
            monitor_user='admin',
            monitor_password='12345',
            keepalived_password='12345',
            enable_haproxy_protocol=True,
        )

        cephadm_module.spec_store._specs = {
            'nfs.foo': nfs_service,
            'ingress.nfs.foo': ispec
        }
        cephadm_module.spec_store.spec_created = {
            'nfs.foo': datetime_now(),
            'ingress.nfs.foo': datetime_now()
        }

        haproxy_txt = (
            '# This file is generated by cephadm.\n'
            'global\n'
            '    log         127.0.0.1 local2\n'
            '    chroot      /var/lib/haproxy\n'
            '    pidfile     /var/lib/haproxy/haproxy.pid\n'
            '    maxconn     8000\n'
            '    daemon\n'
            '    stats socket /var/lib/haproxy/stats\n\n'
            'defaults\n'
            '    mode                    tcp\n'
            '    log                     global\n'
            '    timeout queue           1m\n'
            '    timeout connect         10s\n'
            '    timeout client          1m\n'
            '    timeout server          1m\n'
            '    timeout check           10s\n'
            '    maxconn                 8000\n\n'
            'frontend stats\n'
            '    mode http\n'
            '    bind 192.168.122.100:9049\n'
            '    bind 192.168.122.111:9049\n'
            '    stats enable\n'
            '    stats uri /stats\n'
            '    stats refresh 10s\n'
            '    stats auth admin:12345\n'
            '    http-request use-service prometheus-exporter if { path /metrics }\n'
            '    monitor-uri /health\n\n'
            'frontend frontend\n'
            '    bind 192.168.122.100:2049\n'
            '    default_backend backend\n\n'
            'backend backend\n'
            '    mode        tcp\n'
            '    balance     source\n'
            '    hash-type   consistent\n'
            '    default-server send-proxy-v2\n'
            '    server nfs.foo.0 192.168.122.111:12049 check\n'
        )
        haproxy_expected_conf = {
            'files': {'haproxy.cfg': haproxy_txt}
        }

        nfs_ganesha_txt = (
            "# This file is generated by cephadm.\n"
            'NFS_CORE_PARAM {\n'
            '        Enable_NLM = false;\n'
            '        Enable_RQUOTA = false;\n'
            '        Protocols = 4;\n'
            '        NFS_Port = 2049;\n'
            '        HAProxy_Hosts = 192.168.122.111, 10.10.2.20, 192.168.122.222;\n'
            '}\n'
            '\n'
            'NFSv4 {\n'
            '        Delegations = false;\n'
            "        RecoveryBackend = 'rados_cluster';\n"
            '        Minor_Versions = 1, 2;\n'
            '        IdmapConf = "/etc/ganesha/idmap.conf";\n'
            '}\n'
            '\n'
            'RADOS_KV {\n'
            '        UserId = "nfs.foo.test.0.0";\n'
            '        nodeid = "nfs.foo.None";\n'
            '        pool = ".nfs";\n'
            '        namespace = "foo";\n'
            '}\n'
            '\n'
            'RADOS_URLS {\n'
            '        UserId = "nfs.foo.test.0.0";\n'
            '        watch_url = '
            '"rados://.nfs/foo/conf-nfs.foo";\n'
            '}\n'
            '\n'
            'RGW {\n'
            '        cluster = "ceph";\n'
            '        name = "client.nfs.foo.test.0.0-rgw";\n'
            '}\n'
            '\n'
            "%url    rados://.nfs/foo/conf-nfs.foo"
        )
        nfs_expected_conf = {
            'files': {'ganesha.conf': nfs_ganesha_txt, 'idmap.conf': ''},
            'config': '',
            'extra_args': ['-N', 'NIV_EVENT'],
            'keyring': (
                '[client.nfs.foo.test.0.0]\n'
                'key = None\n'
            ),
            'namespace': 'foo',
            'pool': '.nfs',
            'rgw': {
                'cluster': 'ceph',
                'keyring': (
                    '[client.nfs.foo.test.0.0-rgw]\n'
                    'key = None\n'
                ),
                'user': 'nfs.foo.test.0.0-rgw',
            },
            'userid': 'nfs.foo.test.0.0',
        }

        nfs_daemons = [
            DaemonDescription(
                daemon_type='nfs',
                daemon_id='foo.0.1.host1.qwerty',
                hostname='host1',
                rank=0,
                rank_generation=1,
                ports=[12049],
            ),
            DaemonDescription(
                daemon_type='nfs',
                daemon_id='foo.0.0.host2.abcdef',
                hostname='host2',
                rank=0,
                rank_generation=0,
                ports=[12049],
            ),
        ]
        _get_daemons_by_service.return_value = nfs_daemons

        ingress_svc = cephadm_module.cephadm_services['ingress']
        nfs_svc = cephadm_module.cephadm_services['nfs']

        # add host network info to one host to test the behavior of
        # adding all known-good addresses of the host to the list.
        cephadm_module.cache.update_host_networks('host1', {
            # this one is additional
            '10.10.2.0/24': {
                'eth1': ['10.10.2.20']
            },
            # this is redundant and will be skipped
            '192.168.122.0/24': {
                'eth0': ['192.168.122.111']
            },
            # this is a link-local address and will be ignored
            "fe80::/64": {
                "veth0": [
                    "fe80::8cf5:25ff:fe1c:d963"
                ],
                "eth0": [
                    "fe80::c7b:cbff:fef6:7370"
                ],
                "eth1": [
                    "fe80::7201:25a7:390b:d9a7"
                ]
            },
        })

        haproxy_generated_conf, _ = ingress_svc.haproxy_generate_config(
            CephadmDaemonDeploySpec(
                host='host1',
                daemon_id='ingress',
                service_name=ispec.service_name(),
            ),
        )
        assert haproxy_generated_conf == haproxy_expected_conf

        nfs_generated_conf, _ = nfs_svc.generate_config(
            CephadmDaemonDeploySpec(
                host='test',
                daemon_id='foo.test.0.0',
                service_name=nfs_service.service_name(),
            ),
        )
        assert nfs_generated_conf == nfs_expected_conf


class TestCephFsMirror:
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_config(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, ServiceSpec('cephfs-mirror')):
                cephadm_module.assert_issued_mon_command({
                    'prefix': 'mgr module enable',
                    'module': 'mirroring'
                })


class TestJaeger:
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_jaeger_query(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        spec = TracingSpec(es_nodes="192.168.0.1:9200",
                           service_type="jaeger-query")

        config = {"elasticsearch_nodes": "http://192.168.0.1:9200"}

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, spec):
                _run_cephadm.assert_called_with(
                    'test',
                    "jaeger-query.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'jaeger-query.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [16686],
                        },
                        "meta": {
                            'service_name': 'jaeger-query',
                            'ports': [16686],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": config,
                    }),
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_jaeger_collector_es_deploy(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        collector_spec = TracingSpec(service_type="jaeger-collector")
        es_spec = TracingSpec(service_type="elasticsearch")
        es_config = {}

        with with_host(cephadm_module, 'test'):
            collector_config = {
                "elasticsearch_nodes": f'http://{build_url(host=cephadm_module.inventory.get_addr("test"), port=9200).lstrip("/")}'}
            with with_service(cephadm_module, es_spec):
                _run_cephadm.assert_called_with(
                    "test",
                    "elasticsearch.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'elasticsearch.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [9200],
                        },
                        "meta": {
                            'service_name': 'elasticsearch',
                            'ports': [9200],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": es_config,
                    }),
                )
                with with_service(cephadm_module, collector_spec):
                    _run_cephadm.assert_called_with(
                        "test",
                        "jaeger-collector.test",
                        ['_orch', 'deploy'],
                        [],
                        stdin=json.dumps({
                            "fsid": "fsid",
                            "name": 'jaeger-collector.test',
                            "image": '',
                            "deploy_arguments": [],
                            "params": {
                                'tcp_ports': [14250],
                            },
                            "meta": {
                                'service_name': 'jaeger-collector',
                                'ports': [14250],
                                'ip': None,
                                'deployed_by': [],
                                'rank': None,
                                'rank_generation': None,
                                'extra_container_args': None,
                                'extra_entrypoint_args': None,
                            },
                            "config_blobs": collector_config,
                        }),
                    )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_jaeger_agent(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        collector_spec = TracingSpec(service_type="jaeger-collector", es_nodes="192.168.0.1:9200")
        collector_config = {"elasticsearch_nodes": "http://192.168.0.1:9200"}

        agent_spec = TracingSpec(service_type="jaeger-agent")
        agent_config = {"collector_nodes": "test:14250"}

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, collector_spec):
                _run_cephadm.assert_called_with(
                    "test",
                    "jaeger-collector.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps({
                        "fsid": "fsid",
                        "name": 'jaeger-collector.test',
                        "image": '',
                        "deploy_arguments": [],
                        "params": {
                            'tcp_ports': [14250],
                        },
                        "meta": {
                            'service_name': 'jaeger-collector',
                            'ports': [14250],
                            'ip': None,
                            'deployed_by': [],
                            'rank': None,
                            'rank_generation': None,
                            'extra_container_args': None,
                            'extra_entrypoint_args': None,
                        },
                        "config_blobs": collector_config,
                    }),
                )
                with with_service(cephadm_module, agent_spec):
                    _run_cephadm.assert_called_with(
                        "test",
                        "jaeger-agent.test",
                        ['_orch', 'deploy'],
                        [],
                        stdin=json.dumps({
                            "fsid": "fsid",
                            "name": 'jaeger-agent.test',
                            "image": '',
                            "deploy_arguments": [],
                            "params": {
                                'tcp_ports': [6799],
                            },
                            "meta": {
                                'service_name': 'jaeger-agent',
                                'ports': [6799],
                                'ip': None,
                                'deployed_by': [],
                                'rank': None,
                                'rank_generation': None,
                                'extra_container_args': None,
                                'extra_entrypoint_args': None,
                            },
                            "config_blobs": agent_config,
                        }),
                    )


class TestCustomContainer:
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_deploy_custom_container(
        self, _run_cephadm, cephadm_module: CephadmOrchestrator
    ):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        spec = CustomContainerSpec(
            service_id='tsettinu',
            image='quay.io/foobar/barbaz:latest',
            entrypoint='/usr/local/bin/blat.sh',
            ports=[9090],
        )

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, spec):
                _run_cephadm.assert_called_with(
                    'test',
                    "container.tsettinu.test",
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps(
                        {
                            "fsid": "fsid",
                            "name": 'container.tsettinu.test',
                            "image": 'quay.io/foobar/barbaz:latest',
                            "deploy_arguments": [],
                            "params": {
                                'tcp_ports': [9090],
                            },
                            "meta": {
                                'service_name': 'container.tsettinu',
                                'ports': [],
                                'ip': None,
                                'deployed_by': [],
                                'rank': None,
                                'rank_generation': None,
                                'extra_container_args': None,
                                'extra_entrypoint_args': None,
                            },
                            "config_blobs": {
                                "image": "quay.io/foobar/barbaz:latest",
                                "entrypoint": "/usr/local/bin/blat.sh",
                                "args": [],
                                "envs": [],
                                "volume_mounts": {},
                                "privileged": False,
                                "ports": [9090],
                                "dirs": [],
                                "files": {},
                            },
                        }
                    ),
                )

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_deploy_custom_container_with_init_ctrs(
        self, _run_cephadm, cephadm_module: CephadmOrchestrator
    ):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        spec = CustomContainerSpec(
            service_id='tsettinu',
            image='quay.io/foobar/barbaz:latest',
            entrypoint='/usr/local/bin/blat.sh',
            ports=[9090],
            init_containers=[
                {'entrypoint': '/usr/local/bin/prepare.sh'},
                {
                    'entrypoint': '/usr/local/bin/optimize.sh',
                    'entrypoint_args': [
                        '--timeout=5m',
                        '--cores=8',
                        {'argument': '--title=Alpha One'},
                    ],
                },
            ],
        )

        expected = {
            'fsid': 'fsid',
            'name': 'container.tsettinu.test',
            'image': 'quay.io/foobar/barbaz:latest',
            'deploy_arguments': [],
            'params': {
                'tcp_ports': [9090],
                'init_containers': [
                    {'entrypoint': '/usr/local/bin/prepare.sh'},
                    {
                        'entrypoint': '/usr/local/bin/optimize.sh',
                        'entrypoint_args': [
                            '--timeout=5m',
                            '--cores=8',
                            '--title=Alpha One',
                        ],
                    },
                ],
            },
            'meta': {
                'service_name': 'container.tsettinu',
                'ports': [],
                'ip': None,
                'deployed_by': [],
                'rank': None,
                'rank_generation': None,
                'extra_container_args': None,
                'extra_entrypoint_args': None,
                'init_containers': [
                    {'entrypoint': '/usr/local/bin/prepare.sh'},
                    {
                        'entrypoint': '/usr/local/bin/optimize.sh',
                        'entrypoint_args': [
                            '--timeout=5m',
                            '--cores=8',
                            {'argument': '--title=Alpha One', 'split': False},
                        ],
                    },
                ],
            },
            'config_blobs': {
                'image': 'quay.io/foobar/barbaz:latest',
                'entrypoint': '/usr/local/bin/blat.sh',
                'args': [],
                'envs': [],
                'volume_mounts': {},
                'privileged': False,
                'ports': [9090],
                'dirs': [],
                'files': {},
            },
        }
        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, spec):
                _run_cephadm.assert_called_with(
                    'test',
                    'container.tsettinu.test',
                    ['_orch', 'deploy'],
                    [],
                    stdin=json.dumps(expected),
                )
