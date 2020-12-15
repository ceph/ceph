import pytest

from unittest.mock import MagicMock

from cephadm.services.cephadmservice import MonService, MgrService, MdsService, RgwService, \
    RbdMirrorService, CrashService, CephadmService, AuthEntity, CephadmExporter
from cephadm.services.iscsi import IscsiService
from cephadm.services.nfs import NFSService
from cephadm.services.osd import RemoveUtil, OSDQueue, OSDService, OSD, NotFoundError
from cephadm.services.monitoring import GrafanaService, AlertmanagerService, PrometheusService, \
    NodeExporterService

from orchestrator import OrchestratorError


class FakeMgr:
    def __init__(self):
        self.config = ''
        self.check_mon_command = MagicMock(side_effect=self._check_mon_command)

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
        cephadm_exporter_service = CephadmExporter(mgr)
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
            'cephadm-exporter': cephadm_exporter_service,
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

        # services based on CephadmService shouldn't have get_auth_entity
        with pytest.raises(AttributeError):
            for daemon_type in ['grafana', 'alertmanager', 'prometheus', 'node-exporter', 'cephadm-exporter']:
                cephadm_services[daemon_type].get_auth_entity("id1", "host")
                cephadm_services[daemon_type].get_auth_entity("id1", "")
                cephadm_services[daemon_type].get_auth_entity("id1")
