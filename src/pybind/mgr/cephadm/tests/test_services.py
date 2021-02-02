import pytest

from unittest.mock import MagicMock, call

from cephadm.services.cephadmservice import MonService, MgrService, MdsService, RgwService, \
    RbdMirrorService, CrashService, CephadmService, AuthEntity, CephadmExporter, CephadmDaemonSpec
from cephadm.services.iscsi import IscsiService
from cephadm.services.nfs import NFSService
from cephadm.services.osd import RemoveUtil, OSDRemovalQueue, OSDService, OSD, NotFoundError
from cephadm.services.monitoring import GrafanaService, AlertmanagerService, PrometheusService, \
    NodeExporterService
from ceph.deployment.service_spec import IscsiServiceSpec, ServiceType

from orchestrator import OrchestratorError, DaemonType

from .fixtures import cephadm_module


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

    def test_iscsi_client_caps(self, cephadm_module):
        cephadm_module.check_mon_command = MagicMock()
        cephadm_module.check_mon_command.return_value = 0, '', ''
        iscsi_service = cephadm_module._get_cephadm_service(ServiceType.iscsi)

        iscsi_spec = IscsiServiceSpec(service_type=ServiceType.iscsi, service_id="a")
        daemon = CephadmDaemonSpec(
            spec=iscsi_spec,
            daemon_type=DaemonType.iscsi,
            daemon_id='a',
            host='host'
        )

        iscsi_service.prepare_create(daemon)

        expected_caps = ['mon',
                         'profile rbd, allow command "osd blocklist", allow command "config-key get" with "key" prefix "iscsi/"',
                         'mgr', 'allow command "service status"',
                         'osd', 'allow rwx']

        expected_call = call({'prefix': 'auth get-or-create',
                              'entity': 'client.iscsi.a',
                              'caps': expected_caps})

        assert expected_call in cephadm_module.check_mon_command.mock_calls

    def test_get_auth_entity(self, cephadm_module):
        cephadm_services = cephadm_module.cephadm_services

        for daemon_type_s in ['rgw', 'rbd-mirror', 'nfs', "iscsi"]:
            service_type = ServiceType(daemon_type_s)
            assert "client.%s.id1" % (daemon_type_s) == \
                cephadm_services[service_type].get_auth_entity("id1", "host")
            assert "client.%s.id1" % (daemon_type_s) == \
                cephadm_services[service_type].get_auth_entity("id1", "")
            assert "client.%s.id1" % (daemon_type_s) == \
                cephadm_services[service_type].get_auth_entity("id1")

        assert "client.crash.host" == \
            cephadm_services[ServiceType.crash].get_auth_entity("id1", "host")
        with pytest.raises(OrchestratorError):
            t = cephadm_services[ServiceType.crash].get_auth_entity("id1", "")
            t = cephadm_services[ServiceType.crash].get_auth_entity("id1")

        assert "mon." == cephadm_services[ServiceType.mon].get_auth_entity("id1", "host")
        assert "mon." == cephadm_services[ServiceType.mon].get_auth_entity("id1", "")
        assert "mon." == cephadm_services[ServiceType.mon].get_auth_entity("id1")

        assert "mgr.id1" == cephadm_services[ServiceType.mgr].get_auth_entity("id1", "host")
        assert "mgr.id1" == cephadm_services[ServiceType.mgr].get_auth_entity("id1", "")
        assert "mgr.id1" == cephadm_services[ServiceType.mgr].get_auth_entity("id1")

        for daemon_type_s in ["osd", "mds"]:
            service_type = ServiceType(daemon_type_s)
            assert "%s.id1" % daemon_type_s == \
                cephadm_services[service_type].get_auth_entity("id1", "host")
            assert "%s.id1" % daemon_type_s == \
                cephadm_services[service_type].get_auth_entity("id1", "")
            assert "%s.id1" % daemon_type_s == \
                cephadm_services[service_type].get_auth_entity("id1")

        # services based on CephadmService shouldn't have get_auth_entity
        with pytest.raises(AttributeError):
            for daemon_type_s in ['grafana', 'alertmanager', 'prometheus', 'node-exporter', 'cephadm-exporter']:
                service_type = ServiceType(daemon_type_s)
                cephadm_services[service_type].get_auth_entity("id1", "host")
                cephadm_services[service_type].get_auth_entity("id1", "")
                cephadm_services[service_type].get_auth_entity("id1")
