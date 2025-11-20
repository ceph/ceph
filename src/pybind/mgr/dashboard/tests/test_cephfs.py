# -*- coding: utf-8 -*-
import json
from collections import defaultdict

try:
    from mock import Mock
except ImportError:
    from unittest.mock import patch, Mock

from .. import mgr
from ..controllers.cephfs import CephFS, CephFSMirror
from ..tests import ControllerTestCase


class MetaDataMock(object):
    def get(self, _x, _y):
        return 'bar'


def get_metadata_mock(key, meta_key):
    return {
        'mds': {
            None: None,  # Unknown key
            'foo': MetaDataMock()
        }[meta_key]
    }[key]


@patch('dashboard.mgr.get_metadata', Mock(side_effect=get_metadata_mock))
class CephFsTest(ControllerTestCase):
    cephFs = CephFS()

    def test_append_of_mds_metadata_if_key_is_not_found(self):
        mds_versions = defaultdict(list)
        # pylint: disable=protected-access
        self.cephFs._append_mds_metadata(mds_versions, None)
        self.assertEqual(len(mds_versions), 0)

    def test_append_of_mds_metadata_with_existing_metadata(self):
        mds_versions = defaultdict(list)
        # pylint: disable=protected-access
        self.cephFs._append_mds_metadata(mds_versions, 'foo')
        self.assertEqual(len(mds_versions), 1)
        self.assertEqual(mds_versions['bar'], ['foo'])


class CephFSMirrorTest(ControllerTestCase):

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([CephFSMirror])

    def test_list_success(self):
        fs_name = 'test_fs'
        expected_peers = [
            {
                'uuid': {
                    'client_name': 'client.mirror',
                    'site_name': 'remote-site',
                    'fs_name': 'test_fs'
                }
            }
        ]
        mock_output = json.dumps(expected_peers)
        mgr.remote = Mock(return_value=(0, mock_output, ''))

        self._get(f'/api/cephfs/mirror?fs_name={fs_name}')
        self.assertStatus(200)
        self.assertJsonBody(expected_peers)
        mgr.remote.assert_called_once_with('mirroring', 'snapshot_mirror_peer_list', fs_name)

    def test_list_error(self):
        fs_name = 'test_fs'
        error_message = 'Failed to connect to remote'
        mgr.remote = Mock(return_value=(1, '', error_message))

        self._get(f'/api/cephfs/mirror?fs_name={fs_name}')
        self.assertStatus(400)
        response = self.json_body()
        self.assertIn('Failed to get Cephfs mirror peers', response.get('detail', ''))
        self.assertIn(error_message, response.get('detail', ''))
        mgr.remote.assert_called_once_with('mirroring', 'snapshot_mirror_peer_list', fs_name)

    def test_daemon_status_success(self):
        expected_status = [
            {
                'daemon_id': 1,
                'filesystems': [
                    {
                        'filesystem_id': 1,
                        'name': 'test_fs',
                        'directory_count': 5,
                        'peers': [
                            {
                                'uuid': 'peer-uuid-123',
                                'remote': {
                                    'client_name': 'client.mirror',
                                    'cluster_name': 'remote-cluster',
                                    'fs_name': 'remote_fs'
                                },
                                'stats': {
                                    'failure_count': 0,
                                    'recovery_count': 1
                                }
                            }
                        ]
                    }
                ]
            }
        ]
        mock_output = json.dumps(expected_status)
        mgr.remote = Mock(return_value=(0, mock_output, ''))

        self._get('/api/cephfs/mirror/daemon-status')
        self.assertStatus(200)
        self.assertJsonBody(expected_status)
        mgr.remote.assert_called_once_with('mirroring', 'snapshot_mirror_daemon_status')

    def test_daemon_status_error(self):
        error_message = 'Daemon not available'
        mgr.remote = Mock(return_value=(1, '', error_message))

        self._get('/api/cephfs/mirror/daemon-status')
        self.assertStatus(400)
        response = self.json_body()
        self.assertIn('Failed to get Cephfs mirror daemon status', response.get('detail', ''))
        self.assertIn(error_message, response.get('detail', ''))
        mgr.remote.assert_called_once_with('mirroring', 'snapshot_mirror_daemon_status')
