import json
import mock

import cherrypy
from cherrypy.test.helper import CPWebCase

from ..controllers.auth import Auth
from ..controllers.summary import Summary
from ..controllers.rbd_mirroring import RbdMirror
from ..services import Service
from ..tools import SessionExpireAtBrowserCloseTool
from .helper import ControllerTestCase


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


class RbdMirroringControllerTest(ControllerTestCase, CPWebCase):

    @classmethod
    def setup_server(cls):
        # Initialize custom handlers.
        cherrypy.tools.authenticate = cherrypy.Tool('before_handler', Auth.check_auth)
        cherrypy.tools.session_expire_at_browser_close = SessionExpireAtBrowserCloseTool()

        cls._mgr_module = mock.Mock()
        cls.setup_test()

    @classmethod
    def setup_test(cls):
        mgr_mock = mock.Mock()
        mgr_mock.list_servers.return_value = mock_list_servers
        mgr_mock.get_metadata.return_value = mock_get_metadata
        mgr_mock.get_daemon_status.return_value = mock_get_daemon_status
        mgr_mock.get.side_effect = lambda key: {
            'osd_map': mock_osd_map,
            'health': {'json': '{"status": 1}'},
            'fs_map': {'filesystems': []},

        }[key]
        mgr_mock.url_prefix = ''
        mgr_mock.get_mgr_id.return_value = 0
        mgr_mock.have_mon_connection.return_value = True

        Service.mgr = mgr_mock

        RbdMirror.mgr = mgr_mock
        RbdMirror._cp_config['tools.authenticate.on'] = False  # pylint: disable=protected-access

        Summary.mgr = mgr_mock
        Summary._cp_config['tools.authenticate.on'] = False  # pylint: disable=protected-access

        cherrypy.tree.mount(RbdMirror(), '/api/test/rbdmirror')
        cherrypy.tree.mount(Summary(), '/api/test/summary')

    def __init__(self, *args, **kwargs):
        super(RbdMirroringControllerTest, self).__init__(*args, dashboard_port=54583, **kwargs)

    @mock.patch('dashboard_v2.controllers.rbd_mirroring.rbd')
    def test_default(self, rbd_mock):  # pylint: disable=W0613
        self._get('/api/test/rbdmirror')
        result = self.jsonBody()
        self.assertStatus(200)
        self.assertEqual(result['status'], 0)
        for k in ['daemons', 'pools', 'image_error', 'image_syncing', 'image_ready']:
            self.assertIn(k, result['content_data'])

    @mock.patch('dashboard_v2.controllers.rbd_mirroring.rbd')
    def test_summary(self, rbd_mock):  # pylint: disable=W0613
        """We're also testing `summary`, as it also uses code from `rbd_mirroring.py`"""
        data = self._get('/api/test/summary')
        self.assertStatus(200)
        summary = data['rbd_mirroring']
        self.assertEqual(summary, {'errors': 0, 'warnings': 1})
