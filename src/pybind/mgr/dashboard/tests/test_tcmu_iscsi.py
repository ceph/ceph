from __future__ import absolute_import

from . import ControllerTestCase
from .. import mgr
from ..controllers.tcmu_iscsi import TcmuIscsi

mocked_servers = [{
    'ceph_version': 'ceph version 13.0.0-5083- () mimic (dev)',
    'hostname': 'ceph-dev1',
    'services': [{'id': 'ceph-dev1:pool1/image1', 'type': 'tcmu-runner'}]
}, {
    'ceph_version': 'ceph version 13.0.0-5083- () mimic (dev)',
    'hostname': 'ceph-dev2',
    'services': [{'id': 'ceph-dev2:pool1/image1', 'type': 'tcmu-runner'}]
}]

mocked_metadata1 = {
    'ceph_version': 'ceph version 13.0.0-5083- () mimic (dev)',
    'pool_name': 'pool1',
    'image_name': 'image1',
    'image_id': '42',
    'optimized_since': 1152121348,
}

mocked_metadata2 = {
    'ceph_version': 'ceph version 13.0.0-5083- () mimic (dev)',
    'pool_name': 'pool1',
    'image_name': 'image1',
    'image_id': '42',
    'optimized_since': 0,
}

mocked_get_daemon_status = {
    'lock_owner': 'true',
}

mocked_get_counter1 = {
    'librbd-42-pool1-image1.lock_acquired_time': [[1152121348 * 1000000000,
                                                   1152121348 * 1000000000]],
    'librbd-42-pool1-image1.rd': [[0, 0], [1, 43]],
    'librbd-42-pool1-image1.wr': [[0, 0], [1, 44]],
    'librbd-42-pool1-image1.rd_bytes': [[0, 0], [1, 45]],
    'librbd-42-pool1-image1.wr_bytes': [[0, 0], [1, 46]],
}

mocked_get_counter2 = {
    'librbd-42-pool1-image1.lock_acquired_time': [[0, 0]],
    'librbd-42-pool1-image1.rd': [],
    'librbd-42-pool1-image1.wr': [],
    'librbd-42-pool1-image1.rd_bytes': [],
    'librbd-42-pool1-image1.wr_bytes': [],
}


def _get_counter(_daemon_type, daemon_name, _stat):
    if daemon_name == 'ceph-dev1:pool1/image1':
        return mocked_get_counter1
    if daemon_name == 'ceph-dev2:pool1/image1':
        return mocked_get_counter2
    return Exception('invalid daemon name')


class TcmuIscsiControllerTest(ControllerTestCase):

    @classmethod
    def setup_server(cls):
        mgr.list_servers.return_value = mocked_servers
        mgr.get_metadata.side_effect = [mocked_metadata1, mocked_metadata2]
        mgr.get_daemon_status.return_value = mocked_get_daemon_status
        mgr.get_counter.side_effect = _get_counter
        mgr.url_prefix = ''
        TcmuIscsi._cp_config['tools.authenticate.on'] = False  # pylint: disable=protected-access

        cls.setup_controllers(TcmuIscsi, "/test")

    def test_list(self):
        self._get('/test/api/tcmuiscsi')
        self.assertStatus(200)
        self.assertJsonBody({
            'daemons': [
                {
                    'server_hostname': 'ceph-dev1',
                    'version': 'ceph version 13.0.0-5083- () mimic (dev)',
                    'optimized_paths': 1, 'non_optimized_paths': 0},
                {
                    'server_hostname': 'ceph-dev2',
                    'version': 'ceph version 13.0.0-5083- () mimic (dev)',
                    'optimized_paths': 0, 'non_optimized_paths': 1}],
            'images': [{
                'device_id': 'pool1/image1',
                'pool_name': 'pool1',
                'name': 'image1',
                'id': '42',
                'optimized_paths': ['ceph-dev1'],
                'non_optimized_paths': ['ceph-dev2'],
                'optimized_daemon': 'ceph-dev1',
                'optimized_since': 1152121348,
                'stats': {
                    'rd': 43.0,
                    'wr': 44.0,
                    'rd_bytes': 45.0,
                    'wr_bytes': 46.0},
                'stats_history': {
                    'rd': [[1, 43]],
                    'wr': [[1, 44]],
                    'rd_bytes': [[1, 45]],
                    'wr_bytes': [[1, 46]]}
            }]
        })
