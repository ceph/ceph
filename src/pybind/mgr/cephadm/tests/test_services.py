from textwrap import dedent
import json
import yaml

import pytest

from unittest.mock import MagicMock, call, patch, ANY

from cephadm.serve import CephadmServe
from cephadm.services.cephadmservice import MonService, MgrService, MdsService, RgwService, \
    RbdMirrorService, CrashService, CephadmDaemonDeploySpec
from cephadm.services.iscsi import IscsiService
from cephadm.services.nfs import NFSService
from cephadm.services.osd import OSDService
from cephadm.services.monitoring import GrafanaService, AlertmanagerService, PrometheusService, \
    NodeExporterService, LokiService, PromtailService
from cephadm.module import CephadmOrchestrator
from ceph.deployment.service_spec import IscsiServiceSpec, MonitoringSpec, AlertManagerSpec, \
    ServiceSpec, RGWSpec, GrafanaSpec, SNMPGatewaySpec, IngressSpec, PlacementSpec, \
    PrometheusSpec, NFSServiceSpec
from cephadm.tests.fixtures import with_host, with_service, _run_cephadm, async_side_effect

from ceph.utils import datetime_now

from orchestrator import OrchestratorError
from orchestrator._interface import DaemonDescription

from typing import Dict, List


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
                    'deploy',
                    [
                        '--name', f'iscsi.{iscsi_daemon_id}',
                        '--meta-json', f'{"{"}"service_name": "iscsi.{pool}", "ports": [{api_port}], "ip": null, "deployed_by": [], "rank": null, "rank_generation": null, "extra_container_args": null, "extra_entrypoint_args": null{"}"}',
                        '--config-json', '-', '--tcp-ports', '3456'
                    ],
                    stdin=json.dumps({"config": "", "keyring": "[client.iscsi.testpool.test.qwert]\nkey = None\n", "files": {"iscsi-gateway.cfg": iscsi_gateway_conf}}),
                    image='')


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

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("mgr_module.MgrModule.get")
    def test_alertmanager_config(self, mock_get, _run_cephadm,
                                 cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        mock_get.return_value = {"services": {"dashboard": "http://[::1]:8080"}}

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, AlertManagerSpec()):
                y = dedent(self._get_config('http://localhost:8080')).lstrip()
                _run_cephadm.assert_called_with(
                    'test',
                    'alertmanager.test',
                    'deploy',
                    [
                        '--name', 'alertmanager.test',
                        '--meta-json', '{"service_name": "alertmanager", "ports": [9093, 9094], "ip": null, "deployed_by": [], "rank": null, "rank_generation": null, "extra_container_args": null, "extra_entrypoint_args": null}',
                        '--config-json', '-', '--tcp-ports', '9093 9094'
                    ],
                    stdin=json.dumps({"files": {"alertmanager.yml": y}, "peers": []}),
                    image='')

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("mgr_module.MgrModule.get")
    def test_alertmanager_config_v6(self, mock_get, _run_cephadm,
                                    cephadm_module: CephadmOrchestrator):
        dashboard_url = "http://[2001:db8:4321:0000:0000:0000:0000:0000]:8080"
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        mock_get.return_value = {"services": {"dashboard": dashboard_url}}

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, AlertManagerSpec()):
                y = dedent(self._get_config(dashboard_url)).lstrip()
                _run_cephadm.assert_called_with(
                    'test',
                    'alertmanager.test',
                    'deploy',
                    [
                        '--name', 'alertmanager.test',
                        '--meta-json',
                        '{"service_name": "alertmanager", "ports": [9093, 9094], "ip": null, "deployed_by": [], "rank": null, "rank_generation": null, "extra_container_args": null, "extra_entrypoint_args": null}',
                        '--config-json', '-', '--tcp-ports', '9093 9094'
                    ],
                    stdin=json.dumps({"files": {"alertmanager.yml": y}, "peers": []}),
                    image='')

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("mgr_module.MgrModule.get")
    @patch("socket.getfqdn")
    def test_alertmanager_config_v6_fqdn(self, mock_getfqdn, mock_get, _run_cephadm,
                                         cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        mock_getfqdn.return_value = "mgr.test.fqdn"
        mock_get.return_value = {"services": {
            "dashboard": "http://[2001:db8:4321:0000:0000:0000:0000:0000]:8080"}}

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, AlertManagerSpec()):
                y = dedent(self._get_config("http://mgr.test.fqdn:8080")).lstrip()
                _run_cephadm.assert_called_with(
                    'test',
                    'alertmanager.test',
                    'deploy',
                    [
                        '--name', 'alertmanager.test',
                        '--meta-json',
                        '{"service_name": "alertmanager", "ports": [9093, 9094], "ip": null, "deployed_by": [], "rank": null, "rank_generation": null, "extra_container_args": null, "extra_entrypoint_args": null}',
                        '--config-json', '-', '--tcp-ports', '9093 9094'
                    ],
                    stdin=json.dumps({"files": {"alertmanager.yml": y}, "peers": []}),
                    image='')

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("mgr_module.MgrModule.get")
    def test_alertmanager_config_v4(self, mock_get, _run_cephadm, cephadm_module: CephadmOrchestrator):
        dashboard_url = "http://192.168.0.123:8080"
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        mock_get.return_value = {"services": {"dashboard": dashboard_url}}

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, AlertManagerSpec()):
                y = dedent(self._get_config(dashboard_url)).lstrip()
                _run_cephadm.assert_called_with(
                    'test',
                    'alertmanager.test',
                    'deploy',
                    [
                        '--name', 'alertmanager.test',
                        '--meta-json', '{"service_name": "alertmanager", "ports": [9093, 9094], "ip": null, "deployed_by": [], "rank": null, "rank_generation": null, "extra_container_args": null, "extra_entrypoint_args": null}',
                        '--config-json', '-', '--tcp-ports', '9093 9094'
                    ],
                    stdin=json.dumps({"files": {"alertmanager.yml": y}, "peers": []}),
                    image='')

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("mgr_module.MgrModule.get")
    @patch("socket.getfqdn")
    def test_alertmanager_config_v4_fqdn(self, mock_getfqdn, mock_get, _run_cephadm,
                                         cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))
        mock_getfqdn.return_value = "mgr.test.fqdn"
        mock_get.return_value = {"services": {"dashboard": "http://192.168.0.123:8080"}}

        with with_host(cephadm_module, 'test'):
            with with_service(cephadm_module, AlertManagerSpec()):
                y = dedent(self._get_config("http://mgr.test.fqdn:8080")).lstrip()
                _run_cephadm.assert_called_with(
                    'test',
                    'alertmanager.test',
                    'deploy',
                    [
                        '--name', 'alertmanager.test',
                        '--meta-json',
                        '{"service_name": "alertmanager", "ports": [9093, 9094], "ip": null, "deployed_by": [], "rank": null, "rank_generation": null, "extra_container_args": null, "extra_entrypoint_args": null}',
                        '--config-json', '-', '--tcp-ports', '9093 9094'
                    ],
                    stdin=json.dumps({"files": {"alertmanager.yml": y}, "peers": []}),
                    image='')

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_prometheus_config(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(('{}', '', 0))

        with with_host(cephadm_module, 'test'):
            # host "test" needs to have networks for keepalive to be placed
            cephadm_module.cache.update_host_networks('test', {
                '1.2.3.0/24': {
                    'if0': ['1.2.3.1']
                },
            })
            with with_service(cephadm_module, MonitoringSpec('node-exporter')) as _, \
                    with_service(cephadm_module, PrometheusSpec('prometheus')) as _:

                y = dedent("""
                # This file is generated by cephadm.
                global:
                  scrape_interval: 10s
                  evaluation_interval: 10s
                rule_files:
                  - /etc/prometheus/alerting/*
                scrape_configs:
                  - job_name: 'ceph'
                    honor_labels: true
                    static_configs:
                    - targets:
                      - '[::1]:9283'

                  - job_name: 'node'
                    static_configs:
                    - targets: ['[1::4]:9100']
                      labels:
                        instance: 'test'

                """).lstrip()

                _run_cephadm.assert_called_with(
                    'test',
                    'prometheus.test',
                    'deploy',
                    [
                        '--name', 'prometheus.test',
                        '--meta-json',
                        ('{"service_name": "prometheus", "ports": [9095], "ip": null, "deployed_by": [], "rank": null, '
                         '"rank_generation": null, "extra_container_args": null, "extra_entrypoint_args": null}'),
                        '--config-json', '-',
                        '--tcp-ports', '9095'
                    ],
                    stdin=json.dumps({"files": {"prometheus.yml": y,
                                                "/etc/prometheus/alerting/custom_alerts.yml": ""},
                                      'retention_time': '15d',
                                      'retention_size': '0'}),
                    image='')

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
                    'loki.test',
                    'deploy',
                    [
                        '--name', 'loki.test',
                        '--meta-json',
                        ('{"service_name": "loki", "ports": [3100], "ip": null, "deployed_by": [], "rank": null, '
                         '"rank_generation": null, "extra_container_args": null, "extra_entrypoint_args": null}'),
                        '--config-json', '-',
                        '--tcp-ports', '3100'
                    ],
                    stdin=json.dumps({"files": {"loki.yml": y}}),
                    image='')

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
                    'promtail.test',
                    'deploy',
                    [
                        '--name', 'promtail.test',
                        '--meta-json',
                        ('{"service_name": "promtail", "ports": [9080], "ip": null, "deployed_by": [], "rank": null, '
                         '"rank_generation": null, "extra_container_args": null, "extra_entrypoint_args": null}'),
                        '--config-json', '-',
                        '--tcp-ports', '9080'
                    ],
                    stdin=json.dumps({"files": {"promtail.yml": y}}),
                    image='')

    @patch("cephadm.serve.CephadmServe._run_cephadm")
    @patch("cephadm.module.CephadmOrchestrator.get_mgr_ip", lambda _: '1::4')
    @patch("cephadm.services.monitoring.verify_tls", lambda *_: None)
    def test_grafana_config(self, _run_cephadm, cephadm_module: CephadmOrchestrator):
        _run_cephadm.side_effect = async_side_effect(("{}", "", 0))

        with with_host(cephadm_module, "test"):
            cephadm_module.set_store("test/grafana_crt", "c")
            cephadm_module.set_store("test/grafana_key", "k")
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
                        deleteDatasources:
                          - name: 'Dashboard1'
                            orgId: 1

                          - name: 'Loki'
                            orgId: 2

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
                            orgId: 2
                            url: ''
                            basicAuth: false
                            isDefault: true
                            editable: false""").lstrip(),
                    'certs/cert_file': dedent("""
                        # generated by cephadm
                        c""").lstrip(),
                    'certs/cert_key': dedent("""
                        # generated by cephadm
                        k""").lstrip(),
                }

                _run_cephadm.assert_called_with(
                    'test',
                    'grafana.test',
                    'deploy',
                    [
                        '--name', 'grafana.test',
                        '--meta-json',
                        ('{"service_name": "grafana", "ports": [3000], "ip": null, "deployed_by": [], "rank": null, '
                         '"rank_generation": null, "extra_container_args": null, "extra_entrypoint_args": null}'),
                        '--config-json', '-', '--tcp-ports', '3000'],
                    stdin=json.dumps({"files": files}),
                    image='')

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
                                    'deleteDatasources:\n\n'
                                    "  - name: 'Loki'\n"
                                    '    orgId: 2\n\n'
                                    'datasources:\n\n'
                                    "  - name: 'Loki'\n"
                                    "    type: 'loki'\n"
                                    "    access: 'proxy'\n"
                                    '    orgId: 2\n'
                                    "    url: ''\n"
                                    '    basicAuth: false\n'
                                    '    isDefault: true\n'
                                    '    editable: false',
                                'certs/cert_file': ANY,
                                'certs/cert_key': ANY}}, [])

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
                                    'deleteDatasources:\n\n'
                                    "  - name: 'Loki'\n"
                                    '    orgId: 2\n\n'
                                    'datasources:\n\n'
                                    "  - name: 'Loki'\n"
                                    "    type: 'loki'\n"
                                    "    access: 'proxy'\n"
                                    '    orgId: 2\n'
                                    "    url: ''\n"
                                    '    basicAuth: false\n'
                                    '    isDefault: true\n'
                                    '    editable: false',
                                'certs/cert_file': ANY,
                                'certs/cert_key': ANY}}, [])

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
                        'test', 'alertmanager.test', 'deploy', [
                            '--name', 'alertmanager.test',
                            '--meta-json', ('{"service_name": "alertmanager", "ports": [4200, 9094], "ip": null, "deployed_by": [], "rank": null, '
                                            '"rank_generation": null, "extra_container_args": null, "extra_entrypoint_args": null}'),
                            '--config-json', '-',
                            '--tcp-ports', '4200 9094',
                            '--reconfig'
                        ],
                        stdin='{}',
                        image='')


class TestRGWService:

    @pytest.mark.parametrize(
        "frontend, ssl, expected",
        [
            ('beast', False, 'beast endpoint=[fd00:fd00:fd00:3000::1]:80'),
            ('beast', True,
             'beast ssl_endpoint=[fd00:fd00:fd00:3000::1]:443 ssl_certificate=config://rgw/cert/rgw.foo'),
            ('civetweb', False, 'civetweb port=[fd00:fd00:fd00:3000::1]:80'),
            ('civetweb', True,
             'civetweb port=[fd00:fd00:fd00:3000::1]:443s ssl_certificate=config://rgw/cert/rgw.foo'),
        ]
    )
    @patch("cephadm.serve.CephadmServe._run_cephadm", _run_cephadm('{}'))
    def test_rgw_update(self, frontend, ssl, expected, cephadm_module: CephadmOrchestrator):
        with with_host(cephadm_module, 'host1'):
            cephadm_module.cache.update_host_networks('host1', {
                'fd00:fd00:fd00:3000::/64': {
                    'if0': ['fd00:fd00:fd00:3000::1']
                }
            })
            s = RGWSpec(service_id="foo",
                        networks=['fd00:fd00:fd00:3000::/64'],
                        ssl=ssl,
                        rgw_frontend_type=frontend)
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
                    'snmp-gateway.test',
                    'deploy',
                    [
                        '--name', 'snmp-gateway.test',
                        '--meta-json',
                        ('{"service_name": "snmp-gateway", "ports": [9464], "ip": null, "deployed_by": [], "rank": null, '
                         '"rank_generation": null, "extra_container_args": null, "extra_entrypoint_args": null}'),
                        '--config-json', '-',
                        '--tcp-ports', '9464'
                    ],
                    stdin=json.dumps(config),
                    image=''
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
                    'snmp-gateway.test',
                    'deploy',
                    [
                        '--name', 'snmp-gateway.test',
                        '--meta-json',
                        ('{"service_name": "snmp-gateway", "ports": [9465], "ip": null, "deployed_by": [], "rank": null, '
                         '"rank_generation": null, "extra_container_args": null, "extra_entrypoint_args": null}'),
                        '--config-json', '-',
                        '--tcp-ports', '9465'
                    ],
                    stdin=json.dumps(config),
                    image=''
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
                    'snmp-gateway.test',
                    'deploy',
                    [
                        '--name', 'snmp-gateway.test',
                        '--meta-json',
                        ('{"service_name": "snmp-gateway", "ports": [9464], "ip": null, "deployed_by": [], "rank": null, '
                         '"rank_generation": null, "extra_container_args": null, "extra_entrypoint_args": null}'),
                        '--config-json', '-',
                        '--tcp-ports', '9464'
                    ],
                    stdin=json.dumps(config),
                    image=''
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
                    'snmp-gateway.test',
                    'deploy',
                    [
                        '--name', 'snmp-gateway.test',
                        '--meta-json',
                        ('{"service_name": "snmp-gateway", "ports": [9464], "ip": null, "deployed_by": [], "rank": null, '
                         '"rank_generation": null, "extra_container_args": null, "extra_entrypoint_args": null}'),
                        '--config-json', '-',
                        '--tcp-ports', '9464'
                    ],
                    stdin=json.dumps(config),
                    image=''
                )


class TestIngressService:

    @patch("cephadm.inventory.Inventory.get_addr")
    @patch("cephadm.utils.resolve_ip")
    @patch("cephadm.inventory.HostCache.get_daemons_by_service")
    @patch("cephadm.serve.CephadmServe._run_cephadm")
    def test_ingress_config_nfs_multiple_nfs_same_rank(self, _run_cephadm, _get_daemons_by_service, _resolve_ip, _get_addr, cephadm_module: CephadmOrchestrator):
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

        nfs_service = NFSServiceSpec(service_id="foo", placement=PlacementSpec(count=1, hosts=['host1', 'host2']),
                                     port=12049)

        ispec = IngressSpec(service_type='ingress',
                            service_id='nfs.foo',
                            backend_service='nfs.foo',
                            frontend_port=2049,
                            monitor_port=9049,
                            virtual_ip='192.168.122.100/24',
                            monitor_user='admin',
                            monitor_password='12345',
                            keepalived_password='12345')

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
        haproxy_expected_conf = {
            'files':
                {
                    'haproxy.cfg':
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
                        '    server nfs.foo.0 192.168.122.111:12049\n'
                }
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
                                + haproxy_generated_conf[1][0] + ' 1.2.3.7:80 check weight 100\n'
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
                                + haproxy_generated_conf[1][0] + ' 1::4:443 check weight 100\n'
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
                                'bind *:8999\n    '
                                'bind 1.2.3.7:8999\n    '
                                'stats enable\n    '
                                'stats uri /stats\n    '
                                'stats refresh 10s\n    '
                                'stats auth admin:12345\n    '
                                'http-request use-service prometheus-exporter if { path /metrics }\n    '
                                'monitor-uri /health\n'
                                '\nfrontend frontend\n    '
                                'bind *:8089\n    '
                                'default_backend backend\n\n'
                                'backend backend\n    '
                                'option forwardfor\n    '
                                'balance static-rr\n    '
                                'option httpchk HEAD / HTTP/1.0\n    '
                                'server '
                                + haproxy_generated_conf[1][0] + ' 1.2.3.7:80 check weight 100\n'
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
