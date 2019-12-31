from __future__ import absolute_import

import json
try:
    import mock
except ImportError:
    import unittest.mock as mock

from . import ControllerTestCase
from .. import mgr
from ..controllers.summary import Summary
from ..controllers.rbd_mirroring import RbdMirroring, RbdMirroringSummary, \
    RbdMirroringPoolBootstrap
from ..services import progress


mock_list_servers = [{
    'hostname': 'ceph-host',
    'services': [{'id': 3, 'type': 'rbd-mirror'}]
}]

mock_get_metadata = {
    'id': 1,
    'instance_id': 3,
    'ceph_version': 'ceph version 13.0.0-5719 mimic (dev)'
}

_status = {
    1: {
        'callouts': {},
        'image_local_count': 5,
        'image_remote_count': 6,
        'image_error_count': 7,
        'image_warning_count': 8,
        'name': 'pool_name'
    }
}

mock_get_daemon_status = {
    'json': json.dumps(_status)
}

mock_osd_map = {
    'pools': [{
        'pool_name': 'rbd',
        'application_metadata': {'rbd'}
    }]
}


class RbdMirroringControllerTest(ControllerTestCase):

    @classmethod
    def setup_server(cls):
        # pylint: disable=protected-access
        RbdMirroring._cp_config['tools.authenticate.on'] = False
        # pylint: enable=protected-access

        cls.setup_controllers([RbdMirroring])

    @mock.patch('dashboard.controllers.rbd_mirroring.rbd.RBD')
    def test_site_name(self, mock_rbd):
        result = {'site_name': 'fsid'}
        mock_rbd_instance = mock_rbd.return_value
        mock_rbd_instance.mirror_site_name_get.return_value = \
            result['site_name']

        self._get('/api/block/mirroring/site_name')
        self.assertStatus(200)
        self.assertJsonBody(result)

        result['site_name'] = 'site-a'
        mock_rbd_instance.mirror_site_name_get.return_value = \
            result['site_name']
        self._put('/api/block/mirroring/site_name', result)
        self.assertStatus(200)
        self.assertJsonBody(result)
        mock_rbd_instance.mirror_site_name_set.assert_called_with(
            mock.ANY, result['site_name'])


class RbdMirroringPoolBootstrapControllerTest(ControllerTestCase):

    @classmethod
    def setup_server(cls):
        # pylint: disable=protected-access
        RbdMirroringPoolBootstrap._cp_config['tools.authenticate.on'] = False
        # pylint: enable=protected-access

        cls.setup_controllers([RbdMirroringPoolBootstrap])

    @mock.patch('dashboard.controllers.rbd_mirroring.rbd.RBD')
    def test_token(self, mock_rbd):
        mock_rbd_instance = mock_rbd.return_value
        mock_rbd_instance.mirror_peer_bootstrap_create.return_value = "1234"

        self._post('/api/block/mirroring/pool/abc/bootstrap/token')
        self.assertStatus(200)
        self.assertJsonBody({"token": "1234"})
        mgr.rados.open_ioctx.assert_called_with("abc")

        mock_rbd_instance.mirror_peer_bootstrap_create.assert_called()

    @mock.patch('dashboard.controllers.rbd_mirroring.rbd')
    def test_peer(self, mock_rbd_module):
        mock_rbd_instance = mock_rbd_module.RBD.return_value

        values = {
            "direction": "invalid",
            "token": "1234"
        }
        self._post('/api/block/mirroring/pool/abc/bootstrap/peer', values)
        self.assertStatus(500)
        mgr.rados.open_ioctx.assert_called_with("abc")

        values["direction"] = "rx"
        self._post('/api/block/mirroring/pool/abc/bootstrap/peer', values)
        self.assertStatus(200)
        self.assertJsonBody({})
        mgr.rados.open_ioctx.assert_called_with("abc")

        mock_rbd_instance.mirror_peer_bootstrap_import.assert_called_with(
            mock.ANY, mock_rbd_module.RBD_MIRROR_PEER_DIRECTION_RX, '1234')


class RbdMirroringSummaryControllerTest(ControllerTestCase):

    @classmethod
    def setup_server(cls):
        mgr.list_servers.return_value = mock_list_servers
        mgr.get_metadata = mock.Mock(return_value=mock_get_metadata)
        mgr.get_daemon_status.return_value = mock_get_daemon_status
        mgr.get.side_effect = lambda key: {
            'osd_map': mock_osd_map,
            'health': {'json': '{"status": 1}'},
            'fs_map': {'filesystems': []},
            'mgr_map': {
                'services': {
                    'dashboard': 'https://ceph.dev:11000/'
                    },
            }
        }[key]
        mgr.url_prefix = ''
        mgr.get_mgr_id.return_value = 0
        mgr.have_mon_connection.return_value = True
        mgr.version = 'ceph version 13.1.0-534-g23d3751b89 ' \
                      '(23d3751b897b31d2bda57aeaf01acb5ff3c4a9cd) ' \
                      'nautilus (dev)'

        progress.get_progress_tasks = mock.MagicMock()
        progress.get_progress_tasks.return_value = ([], [])

        # pylint: disable=protected-access
        RbdMirroringSummary._cp_config['tools.authenticate.on'] = False
        Summary._cp_config['tools.authenticate.on'] = False
        # pylint: enable=protected-access

        cls.setup_controllers([RbdMirroringSummary, Summary], '/test')

    @mock.patch('dashboard.controllers.rbd_mirroring.rbd.RBD')
    def test_default(self, mock_rbd):
        mock_rbd_instance = mock_rbd.return_value
        mock_rbd_instance.mirror_site_name_get.return_value = 'site-a'

        self._get('/test/api/block/mirroring/summary')
        result = self.json_body()
        self.assertStatus(200)
        self.assertEqual(result['site_name'], 'site-a')
        self.assertEqual(result['status'], 0)
        for k in ['daemons', 'pools', 'image_error', 'image_syncing', 'image_ready']:
            self.assertIn(k, result['content_data'])

    @mock.patch('dashboard.controllers.BaseController._has_permissions')
    @mock.patch('dashboard.controllers.rbd_mirroring.rbd.RBD')
    def test_summary(self, mock_rbd, has_perms_mock):
        """We're also testing `summary`, as it also uses code from `rbd_mirroring.py`"""
        mock_rbd_instance = mock_rbd.return_value
        mock_rbd_instance.mirror_site_name_get.return_value = 'site-a'

        has_perms_mock.return_value = True
        self._get('/test/api/summary')
        self.assertStatus(200)

        summary = self.json_body()['rbd_mirroring']
        self.assertEqual(summary, {'errors': 0, 'warnings': 1})
