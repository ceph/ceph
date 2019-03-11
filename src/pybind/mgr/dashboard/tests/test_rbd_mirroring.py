from __future__ import absolute_import

import json
import mock

from . import ControllerTestCase
from .. import mgr
from ..controllers.summary import Summary
from ..controllers.rbd_mirroring import RbdMirroringSummary


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


class RbdMirroringSummaryControllerTest(ControllerTestCase):

    @classmethod
    def setup_server(cls):
        mgr.list_servers.return_value = mock_list_servers
        mgr.get_metadata.return_value = mock_get_metadata
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

        # pylint: disable=protected-access
        RbdMirroringSummary._cp_config['tools.authenticate.on'] = False
        Summary._cp_config['tools.authenticate.on'] = False
        # pylint: enable=protected-access

        cls.setup_controllers([RbdMirroringSummary, Summary], '/test')

    @mock.patch('dashboard.controllers.rbd_mirroring.rbd')
    def test_default(self, rbd_mock):  # pylint: disable=W0613
        self._get('/test/api/block/mirroring/summary')
        result = self.jsonBody()
        self.assertStatus(200)
        self.assertEqual(result['status'], 0)
        for k in ['daemons', 'pools', 'image_error', 'image_syncing', 'image_ready']:
            self.assertIn(k, result['content_data'])

    @mock.patch('dashboard.controllers.BaseController._has_permissions')
    @mock.patch('dashboard.controllers.rbd_mirroring.rbd')
    def test_summary(self, rbd_mock, has_perms_mock):  # pylint: disable=W0613
        """We're also testing `summary`, as it also uses code from `rbd_mirroring.py`"""
        has_perms_mock.return_value = True
        self._get('/test/api/summary')
        self.assertStatus(200)

        summary = self.jsonBody()['rbd_mirroring']
        self.assertEqual(summary, {'errors': 0, 'warnings': 1})
