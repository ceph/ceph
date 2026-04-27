# -*- coding: utf-8 -*-
import json
from collections import defaultdict

try:
    from mock import Mock
except ImportError:
    from unittest.mock import patch, Mock

from .. import mgr
from ..controllers.cephfs import CephFS, CephFSMirror, CephFSMirrorStatus
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

        self._get(f'/api/cephfs/mirror/{fs_name}')
        self.assertStatus(200)
        self.assertJsonBody(expected_peers)
        mgr.remote.assert_called_once_with('mirroring', 'snapshot_mirror_peer_list', fs_name)

    def test_list_error(self):
        fs_name = 'test_fs'
        error_message = 'Failed to connect to remote'
        mgr.remote = Mock(return_value=(1, '', error_message))

        self._get(f'/api/cephfs/mirror/{fs_name}')
        self.assertStatus(400)
        response = self.json_body()
        self.assertIn('Failed to get Cephfs mirror peers', response.get('detail', ''))
        self.assertIn(error_message, response.get('detail', ''))
        mgr.remote.assert_called_once_with('mirroring', 'snapshot_mirror_peer_list', fs_name)

    def test_token_success(self):
        fs_name = 'test_fs'
        client_name = 'client.mirror'
        site_name = 'remote-site'
        expected_token = {'token': 'bootstrap-token-12345'}
        mock_output = json.dumps(expected_token)
        mgr.remote = Mock(return_value=(0, mock_output, ''))

        self._post('/api/cephfs/mirror/token', {
            'fs_name': fs_name,
            'client_name': client_name,
            'site_name': site_name
        })
        self.assertStatus(200)
        self.assertJsonBody(expected_token)
        mgr.remote.assert_called_once_with('mirroring', 'snapshot_mirror_peer_bootstrap_create',
                                           fs_name, client_name, site_name)

    def test_token_error(self):
        fs_name = 'test_fs'
        client_name = 'client.mirror'
        site_name = 'remote-site'
        error_message = 'Failed to create bootstrap token'
        mgr.remote = Mock(return_value=(1, '', error_message))

        self._post('/api/cephfs/mirror/token', {
            'fs_name': fs_name,
            'client_name': client_name,
            'site_name': site_name
        })
        self.assertStatus(400)
        response = self.json_body()
        self.assertIn('Failed to create bootstrap token', response.get('detail', ''))
        self.assertIn(error_message, response.get('detail', ''))
        mgr.remote.assert_called_once_with('mirroring', 'snapshot_mirror_peer_bootstrap_create',
                                           fs_name, client_name, site_name)

    def test_create_success(self):
        fs_name = 'test_fs'
        token = 'bootstrap-token-12345'
        expected_result = {'peer_uuid': 'peer-uuid-123'}
        mock_output = json.dumps(expected_result)
        mgr.remote = Mock(return_value=(0, mock_output, ''))

        self._post('/api/cephfs/mirror', {
            'fs_name': fs_name,
            'token': token
        })
        self.assertStatus(201)
        self.assertJsonBody(expected_result)
        mgr.remote.assert_called_once_with('mirroring', 'snapshot_mirror_peer_bootstrap_import',
                                           fs_name, token)

    def test_import_token_error(self):
        fs_name = 'test_fs'
        token = 'invalid-token'
        error_message = 'Invalid bootstrap token'
        mgr.remote = Mock(return_value=(1, '', error_message))

        self._post('/api/cephfs/mirror', {
            'fs_name': fs_name,
            'token': token
        })
        self.assertStatus(400)
        response = self.json_body()
        self.assertIn('Failed to import the token to create bootstrap peer',
                      response.get('detail', ''))
        self.assertIn(error_message, response.get('detail', ''))
        mgr.remote.assert_called_once_with('mirroring', 'snapshot_mirror_peer_bootstrap_import',
                                           fs_name, token)

    def test_delete_success(self):
        fs_name = 'test_fs'
        peer_uuid = 'peer-uuid-123'
        mgr.remote = Mock(return_value=(0, '', ''))

        self._delete(f'/api/cephfs/mirror/{fs_name}/{peer_uuid}')
        self.assertStatus(204)
        mgr.remote.assert_called_once_with('mirroring', 'snapshot_mirror_peer_remove',
                                           fs_name, peer_uuid)

    def test_delete_error(self):
        fs_name = 'test_fs'
        peer_uuid = 'peer-uuid-123'
        error_message = 'Peer not found'
        mgr.remote = Mock(return_value=(1, '', error_message))

        self._delete(f'/api/cephfs/mirror/{fs_name}/{peer_uuid}')
        self.assertStatus(400)
        response = self.json_body()
        self.assertIn('Failed to delete peer', response.get('detail', ''))
        self.assertIn(error_message, response.get('detail', ''))
        mgr.remote.assert_called_once_with('mirroring', 'snapshot_mirror_peer_remove',
                                           fs_name, peer_uuid)

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


class CephFSMirrorStatusTest(ControllerTestCase):

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([CephFSMirrorStatus])

    def test_status_available(self):
        mgr.remote = Mock(return_value=(0, '', ''))

        self._get('/ui-api/cephfs/mirror/status')
        self.assertStatus(200)
        response = self.json_body()
        self.assertTrue(response.get('available'))
        self.assertIsNone(response.get('message'))
        mgr.remote.assert_called_once_with('mirroring', 'snapshot_mirror_daemon_status')

    def test_status_unavailable_import_error(self):
        with patch.object(mgr, 'remote', side_effect=ImportError('Module not found')):
            self._get('/ui-api/cephfs/mirror/status')
            self.assertStatus(200)
            response = self.json_body()
            self.assertFalse(response.get('available'))
            self.assertIn('Cephfs mirror module is not enabled', response.get('message', ''))

    def test_status_unavailable_runtime_error(self):
        with patch.object(mgr, 'remote', side_effect=RuntimeError('Module error')):
            self._get('/ui-api/cephfs/mirror/status')
            self.assertStatus(200)
            response = self.json_body()
            self.assertFalse(response.get('available'))
            self.assertIn('Cephfs mirror module is not enabled', response.get('message', ''))
