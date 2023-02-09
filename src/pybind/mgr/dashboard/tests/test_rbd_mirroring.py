
import json
import unittest

import rbd

try:
    import mock
except ImportError:
    import unittest.mock as mock

from .. import mgr
from ..controllers.orchestrator import Orchestrator
from ..controllers.rbd_mirroring import RbdMirroring, \
    RbdMirroringPoolBootstrap, RbdMirroringStatus, RbdMirroringSummary, \
    get_daemons, get_pools
from ..controllers.summary import Summary
from ..services import progress
from ..tests import ControllerTestCase

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
        'callouts': {
            'image': {
                'level': 'warning',
            }
        },
        'image_local_count': 5,
        'image_remote_count': 6,
        'image_error_count': 7,
        'image_warning_count': 8,
        'name': 'rbd'
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


class GetDaemonAndPoolsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
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

    @mock.patch('rbd.RBD')
    def test_get_pools_unknown(self, mock_rbd):
        mock_rbd_instance = mock_rbd.return_value
        mock_rbd_instance.mirror_mode_get.side_effect = Exception
        daemons = get_daemons()
        res = get_pools(daemons)
        self.assertTrue(res['rbd']['mirror_mode'] == "unknown")

    @mock.patch('rbd.RBD')
    def test_get_pools_mode(self, mock_rbd):

        daemons = get_daemons()
        mock_rbd_instance = mock_rbd.return_value
        testcases = [
            (rbd.RBD_MIRROR_MODE_DISABLED, "disabled"),
            (rbd.RBD_MIRROR_MODE_IMAGE, "image"),
            (rbd.RBD_MIRROR_MODE_POOL, "pool"),
        ]
        mock_rbd_instance.mirror_peer_list.return_value = []
        for mirror_mode, expected in testcases:
            mock_rbd_instance.mirror_mode_get.return_value = mirror_mode
            res = get_pools(daemons)
            self.assertTrue(res['rbd']['mirror_mode'] == expected)

    @mock.patch('rbd.RBD')
    def test_get_pools_health(self, mock_rbd):

        mock_rbd_instance = mock_rbd.return_value
        mock_rbd_instance.mirror_peer_list.return_value = []
        test_cases = self._get_pool_test_cases()
        for new_status, pool_mirror_mode, images_summary, expected_output in test_cases:
            _status[1].update(new_status)
            daemon_status = {
                'json': json.dumps(_status)
            }
            mgr.get_daemon_status.return_value = daemon_status
            daemons = get_daemons()
            mock_rbd_instance.mirror_mode_get.return_value = pool_mirror_mode
            mock_rbd_instance.mirror_image_status_summary.return_value = images_summary
            res = get_pools(daemons)
            for k, v in expected_output.items():
                self.assertTrue(v == res['rbd'][k])
        mgr.get_daemon_status.return_value = mock_get_daemon_status  # reset return value

    def _get_pool_test_cases(self):
        test_cases = [
            # 1. daemon status
            # 2. Pool mirror mock_get_daemon_status
            # 3. Image health summary
            # 4. Pool health output
            (
                {
                    'image_error_count': 7,
                },
                rbd.RBD_MIRROR_MODE_IMAGE,
                [(rbd.MIRROR_IMAGE_STATUS_STATE_UNKNOWN, None)],
                {
                    'health_color': 'warning',
                    'health': 'Warning'
                }
            ),
            (
                {
                    'image_error_count': 7,
                },
                rbd.RBD_MIRROR_MODE_POOL,
                [(rbd.MIRROR_IMAGE_STATUS_STATE_ERROR, None)],
                {
                    'health_color': 'error',
                    'health': 'Error'
                }
            ),
            (
                {
                    'image_error_count': 0,
                    'image_warning_count': 0,
                    'leader_id': 1
                },
                rbd.RBD_MIRROR_MODE_DISABLED,
                [],
                {
                    'health_color': 'info',
                    'health': 'Disabled'
                }
            ),
        ]
        return test_cases


class RbdMirroringControllerTest(ControllerTestCase):

    @classmethod
    def setup_server(cls):
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
        # 2 warnings: 1 for the daemon, 1 for the pool
        self.assertEqual(summary, {'errors': 0, 'warnings': 2})


class RbdMirroringStatusControllerTest(ControllerTestCase):

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([RbdMirroringStatus, Orchestrator])

    @mock.patch('dashboard.controllers.orchestrator.OrchClient.instance')
    def test_status(self, instance):
        status = {'available': False, 'description': ''}
        fake_client = mock.Mock()
        fake_client.status.return_value = status
        instance.return_value = fake_client

        self._get('/ui-api/block/mirroring/status')
        self.assertStatus(200)
        self.assertJsonBody({'available': True, 'message': None})

    def test_configure(self):
        self._post('/ui-api/block/mirroring/configure')
        self.assertStatus(200)
