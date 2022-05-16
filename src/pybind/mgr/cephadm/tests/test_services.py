import pytest
import json
from textwrap import dedent

from unittest.mock import MagicMock, call, patch

from cephadm.services.cephadmservice import MonService, MgrService, MdsService, RgwService, \
    RbdMirrorService, CrashService, CephadmService, AuthEntity
from cephadm.services.iscsi import IscsiService
from cephadm.services.nfs import NFSService
from cephadm.services.osd import RemoveUtil, OSDRemovalQueue, OSDService, OSD, NotFoundError
from cephadm.services.monitoring import GrafanaService, AlertmanagerService, PrometheusService, \
    NodeExporterService
from cephadm.module import CephadmOrchestrator
from ceph.deployment.service_spec import IscsiServiceSpec, AlertManagerSpec
from cephadm.tests.fixtures import cephadm_module, with_host, with_service

from orchestrator import OrchestratorError


class FakeMgr:
    def __init__(self):
        self.config = ''
        self.check_mon_command = MagicMock(side_effect=self._check_mon_command)
        self.template = MagicMock()

    def _check_mon_command(self, cmd_dict, inbuf=None):
        prefix = cmd_dict.get('prefix')
        if prefix == 'get-cmd':
            return 0, self.config, ''
        if prefix == 'set-cmd':
            self.config = cmd_dict.get('value')
            return 0, 'value set', ''
        return -1, '', 'error'


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
            'crash': crash_service,
            'iscsi': iscsi_service,
        }
        return cephadm_services

    def test_iscsi_client_caps(self):
        mgr = FakeMgr()
        iscsi_service = self._get_services(mgr)['iscsi']

        iscsi_spec = IscsiServiceSpec(service_type='iscsi', service_id="a")
        iscsi_spec.daemon_type = "iscsi"
        iscsi_spec.daemon_id = "a"
        iscsi_spec.spec = MagicMock()
        iscsi_spec.spec.daemon_type = "iscsi"
        iscsi_spec.spec.ssl_cert = ''

        iscsi_service.prepare_create(iscsi_spec)

        expected_caps = ['mon',
                         'profile rbd, allow command "osd blacklist", allow command "config-key get" with "key" prefix "iscsi/"',
                         'mgr', 'allow command "service status"',
                         'osd', 'allow rwx']

        expected_call = call({'prefix': 'auth get-or-create',
                              'entity': 'client.iscsi.a',
                              'caps': expected_caps})

        assert expected_call in mgr.check_mon_command.mock_calls

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
            t = cephadm_services["crash"].get_auth_entity("id1", "")
            t = cephadm_services["crash"].get_auth_entity("id1")

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

        with pytest.raises(AttributeError):
            for daemon_type in ['grafana', 'alertmanager', 'prometheus', 'node-exporter']:
                cephadm_services[daemon_type].get_auth_entity("id1", "host")
                cephadm_services[daemon_type].get_auth_entity("id1", "")
                cephadm_services[daemon_type].get_auth_entity("id1")


class TestMonitoring:
    def _get_config(self, url: str) -> str:
        return f"""
        # This file is generated by cephadm.
        # See https://prometheus.io/docs/alerting/configuration/ for documentation.

        global:
          resolve_timeout: 5m

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

    @patch("cephadm.module.CephadmOrchestrator._run_cephadm")
    @patch("mgr_module.MgrModule.get")
    def test_alertmanager_config(self, mock_get, _run_cephadm,
                                 cephadm_module: CephadmOrchestrator):
        _run_cephadm.return_value = ('{}', '', 0)
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
                        '--config-json', '-',
                    ],
                    stdin=json.dumps({"files": {"alertmanager.yml": y}, "peers": []}),
                    image='')

    @patch("cephadm.module.CephadmOrchestrator._run_cephadm")
    @patch("mgr_module.MgrModule.get")
    def test_alertmanager_config_v6(self, mock_get, _run_cephadm,
                                    cephadm_module: CephadmOrchestrator):
        dashboard_url = "http://[2001:db8:4321:0000:0000:0000:0000:0000]:8080"
        _run_cephadm.return_value = ('{}', '', 0)
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
                        '--config-json', '-',
                    ],
                    stdin=json.dumps({"files": {"alertmanager.yml": y}, "peers": []}),
                    image='')

    @patch("cephadm.module.CephadmOrchestrator._run_cephadm")
    @patch("mgr_module.MgrModule.get")
    @patch("socket.getfqdn")
    def test_alertmanager_config_v6_fqdn(self, mock_getfqdn, mock_get, _run_cephadm,
                                         cephadm_module: CephadmOrchestrator):
        _run_cephadm.return_value = ('{}', '', 0)
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
                        '--config-json', '-',
                    ],
                    stdin=json.dumps({"files": {"alertmanager.yml": y}, "peers": []}),
                    image='')

    @patch("cephadm.module.CephadmOrchestrator._run_cephadm")
    @patch("mgr_module.MgrModule.get")
    def test_alertmanager_config_v4(self, mock_get, _run_cephadm, cephadm_module: CephadmOrchestrator):
        dashboard_url = "http://192.168.0.123:8080"
        _run_cephadm.return_value = ('{}', '', 0)
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
                        '--config-json', '-',
                    ],
                    stdin=json.dumps({"files": {"alertmanager.yml": y}, "peers": []}),
                    image='')

    @patch("cephadm.module.CephadmOrchestrator._run_cephadm")
    @patch("mgr_module.MgrModule.get")
    @patch("socket.getfqdn")
    def test_alertmanager_config_v4_fqdn(self, mock_getfqdn, mock_get, _run_cephadm,
                                         cephadm_module: CephadmOrchestrator):
        _run_cephadm.return_value = ('{}', '', 0)
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
                        '--config-json', '-',
                    ],
                    stdin=json.dumps({"files": {"alertmanager.yml": y}, "peers": []}),
                    image='')
