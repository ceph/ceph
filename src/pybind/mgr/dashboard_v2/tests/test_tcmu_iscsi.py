from cherrypy.test.helper import CPWebCase
import cherrypy
import mock

from ..controllers.auth import Auth
from ..services import Service
from ..tools import SessionExpireAtBrowserCloseTool
from ..controllers.tcmu_iscsi import TcmuIscsi
from .helper import ControllerTestCase

mocked_servers = [{
    'ceph_version': 'ceph version 13.0.0-5083- () mimic (dev)',
    'hostname': 'ceph-dev',
    'services': [{'id': 'a:b', 'type': 'tcmu-runner'}]
}]

mocked_metadata = {
    'ceph_version': 'ceph version 13.0.0-5083- () mimic (dev)',
    'pool_name': 'pool1',
    'image_name': 'image1',
    'image_id': '42',
    'optimized_since': 100.0,
}

mocked_get_daemon_status = {
    'lock_owner': 'true',
}

mocked_get_counter = {
    'librbd-42-pool1-image1.lock_acquired_time': [[10000.0, 10000.0]],
    'librbd-42-pool1-image1.rd': 43,
    'librbd-42-pool1-image1.wr': 44,
    'librbd-42-pool1-image1.rd_bytes': 45,
    'librbd-42-pool1-image1.wr_bytes': 46,
}

mocked_get_rate = 47


class TcmuIscsiControllerTest(ControllerTestCase, CPWebCase):

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
        mgr_mock.list_servers.return_value = mocked_servers
        mgr_mock.get_metadata.return_value = mocked_metadata
        mgr_mock.get_daemon_status.return_value = mocked_get_daemon_status
        mgr_mock.get_counter.return_value = mocked_get_counter
        mgr_mock.get_rate.return_value = mocked_get_rate
        mgr_mock.url_prefix = ''
        Service.mgr = mgr_mock
        TcmuIscsi.mgr = mgr_mock
        TcmuIscsi._cp_config['tools.authenticate.on'] = False  # pylint: disable=protected-access

        cherrypy.tree.mount(TcmuIscsi(), "/api/test/tcmu")

    def __init__(self, *args, **kwargs):
        super(TcmuIscsiControllerTest, self).__init__(*args, dashboard_port=54583, **kwargs)

    def test_list(self):
        self._post("/api/auth", {'username': 'admin', 'password': 'admin'})
        self._get('/api/test/tcmu')
        self.assertStatus(200)
        self.assertJsonBody({
            'daemons': [{
                'server_hostname': 'ceph-dev',
                'version': 'ceph version 13.0.0-5083- () mimic (dev)',
                'optimized_paths': 1, 'non_optimized_paths': 0}],
            'images': [{
                'device_id': 'b',
                'pool_name': 'pool1',
                'name': 'image1',
                'id': '42', 'optimized_paths': ['ceph-dev'],
                'non_optimized_paths': [],
                'optimized_since': 1e-05,
                'stats': {'rd': 47, 'rd_bytes': 47, 'wr': 47, 'wr_bytes': 47},
                'stats_history': {
                    'rd': 43, 'wr': 44, 'rd_bytes': 45, 'wr_bytes': 46}
            }]
        })
