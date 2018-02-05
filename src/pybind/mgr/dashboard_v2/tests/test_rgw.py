from mock import Mock
import cherrypy
from ..controllers.rgw import RgwDaemon
from .helper import ControllerTestCase

mocked_servers = [{
    'ceph_version': 'ceph version 13.0.0-5083-g8d1965af24 ' +
                    '(8d1965af241a5a5487e1b2e3684c676c47392be9) ' +
                    'mimic (dev)',
    'hostname': 'ceph-dev',
    'services': [{'id': 'a', 'type': 'mds'},
                 {'id': 'b', 'type': 'mds'},
                 {'id': 'c', 'type': 'mds'},
                 {'id': 'a', 'type': 'mon'},
                 {'id': 'b', 'type': 'mon'},
                 {'id': 'c', 'type': 'mon'},
                 {'id': '0', 'type': 'osd'},
                 {'id': '1', 'type': 'osd'},
                 {'id': '2', 'type': 'osd'},
                 {'id': 'rgw', 'type': 'rgw'}]}]

mocked_metadata = {
    'arch': 'x86_64',
    'ceph_version': 'ceph version 13.0.0-5083-g8d1965af24 ' +
                    '(8d1965af241a5a5487e1b2e3684c676c47392be9) mimic (dev)',
    'cpu': 'Intel(R) Core(TM)2 Quad  CPU   Q9550  @ 2.83GHz',
    'distro': 'opensuse',
    'distro_description': 'openSUSE Tumbleweed',
    'distro_version': '20180202',
    'frontend_config#0': 'civetweb port=8000',
    'frontend_type#0': 'civetweb',
    'hostname': 'ceph-dev',
    'kernel_description': '#135-Ubuntu SMP Fri Jan 19 11:48:36 UTC 2018',
    'kernel_version': '4.4.0-112-generic',
    'mem_swap_kb': '6287356',
    'mem_total_kb': '8173856',
    'num_handles': '1',
    'os': 'Linux',
    'pid': '979',
    'zone_id': '850fbec4-9238-481d-9044-c8048f4d582e',
    'zone_name': 'default',
    'zonegroup_id': '47e10f8e-5801-4d4d-802c-fc32a252c0ba',
    'zonegroup_name': 'default'}


class RgwControllerTest(ControllerTestCase):

    @classmethod
    def setup_test(cls):
        mgr_mock = Mock()
        mgr_mock.list_servers.return_value = mocked_servers
        mgr_mock.get_metadata.return_value = mocked_metadata
        mgr_mock.get_daemon_status.return_value = {'current_sync': []}
        mgr_mock.url_prefix = ''

        cherrypy.tree.mount(RgwDaemon(mgr_mock), "/api/test/rgw")

    def test_list(self):
        self._get('/api/test/rgw')
        self.assertStatus(200)
        self.assertJsonBody([{
            'id': 'rgw',
            'version': mocked_servers[0]['ceph_version'],
            'server_hostname': mocked_servers[0]['hostname']
        }])
