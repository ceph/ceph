from __future__ import absolute_import

import cherrypy

from .. import mgr
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
    'librbd-42-pool1-image1.rd': [[0, 0], [1, 43]],
    'librbd-42-pool1-image1.wr': [[0, 0], [1, 44]],
    'librbd-42-pool1-image1.rd_bytes': [[0, 0], [1, 45]],
    'librbd-42-pool1-image1.wr_bytes': [[0, 0], [1, 46]],
}


class TcmuIscsiControllerTest(ControllerTestCase):

    @classmethod
    def setup_server(cls):
        mgr.list_servers.return_value = mocked_servers
        mgr.get_metadata.return_value = mocked_metadata
        mgr.get_daemon_status.return_value = mocked_get_daemon_status
        mgr.get_counter.return_value = mocked_get_counter
        mgr.url_prefix = ''
        TcmuIscsi._cp_config['tools.authenticate.on'] = False  # pylint: disable=protected-access

        cherrypy.tree.mount(TcmuIscsi(), "/api/test/tcmu")

    def test_list(self):
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
                'stats': {
                    'rd': 43.0,
                    'wr': 44.0,
                    'rd_bytes': 45.0,
                    'wr_bytes': 46.0},
                'stats_history': {
                    'rd': [[0, 0], [1, 43]],
                    'wr': [[0, 0], [1, 44]],
                    'rd_bytes': [[0, 0], [1, 45]],
                    'wr_bytes': [[0, 0], [1, 46]]}
            }]
        })
