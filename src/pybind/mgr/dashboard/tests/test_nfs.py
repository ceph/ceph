# -*- coding: utf-8 -*-
# pylint: disable=too-many-lines
from copy import deepcopy
from unittest.mock import Mock, patch
from urllib.parse import urlencode

from nfs.export import AppliedExportResults

from .. import mgr
from ..controllers._version import APIVersion
from ..controllers.nfs import NFSGaneshaExports, NFSGaneshaUi
from ..tests import ControllerTestCase
from ..tools import NotificationQueue, TaskManager


class NFSGaneshaExportsTest(ControllerTestCase):
    _nfs_module_export = {
        "export_id": 1,
        "path": "bk1",
        "cluster_id": "myc",
        "pseudo": "/bk-ps",
        "access_type": "RO",
        "squash": "root_id_squash",
        "security_label": False,
        "protocols": [
            4
        ],
        "transports": [
            "TCP",
            "UDP"
        ],
        "fsal": {
            "name": "RGW",
            "user_id": "dashboard",
            "access_key_id": "UUU5YVVOQ2P5QTOPYNAN",
            "secret_access_key": "7z87tMUUsHr67ZWx12pCbWkp9UyOldxhDuPY8tVN"
        },
        "clients": []
    }

    _applied_export = AppliedExportResults()

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._expected_export = deepcopy(cls._nfs_module_export)
        del cls._expected_export['fsal']['access_key_id']
        del cls._expected_export['fsal']['secret_access_key']

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        NotificationQueue.stop()

    @classmethod
    def setup_server(cls):
        NotificationQueue.start_queue()
        TaskManager.init()
        cls.setup_controllers([NFSGaneshaExports])

    def test_list_exports(self):
        mgr.remote = Mock(return_value=[self._nfs_module_export])

        self._get('/api/nfs-ganesha/export')
        self.assertStatus(200)
        self.assertJsonBody([self._expected_export])

    def test_get_export(self):
        mgr.remote = Mock(return_value=self._nfs_module_export)

        self._get('/api/nfs-ganesha/export/myc/1')
        self.assertStatus(200)
        self.assertJsonBody(self._expected_export)

    def test_create_export(self):
        export_mgr = Mock()
        created_nfs_export = deepcopy(self._nfs_module_export)
        applied_nfs_export = deepcopy(self._applied_export)
        created_nfs_export['pseudo'] = 'new-pseudo'
        created_nfs_export['export_id'] = 2
        export_mgr.get_export_by_pseudo.side_effect = [None, created_nfs_export]
        export_mgr.apply_export.return_value = applied_nfs_export
        mgr.remote.return_value = export_mgr

        export_create_body = deepcopy(self._expected_export)
        del export_create_body['export_id']
        export_create_body['pseudo'] = created_nfs_export['pseudo']
        applied_nfs_export.append(export_create_body)

        self._post('/api/nfs-ganesha/export',
                   export_create_body,
                   version=APIVersion(2, 0))
        self.assertStatus(201)
        applied_nfs_export.changes[0]['export_id'] = created_nfs_export['export_id']
        self.assertJsonBody(applied_nfs_export.changes[0])

    def test_create_export_with_existing_pseudo_fails(self):
        export_mgr = Mock()
        export_mgr.get_export_by_pseudo.return_value = self._nfs_module_export
        mgr.remote.return_value = export_mgr

        export_create_body = deepcopy(self._expected_export)
        del export_create_body['export_id']

        self._post('/api/nfs-ganesha/export',
                   export_create_body,
                   version=APIVersion(2, 0))
        self.assertStatus(400)
        response = self.json_body()
        self.assertIn(f'Pseudo {export_create_body["pseudo"]} is already in use',
                      response['detail'])

    def test_set_export(self):
        export_mgr = Mock()
        updated_nfs_export = deepcopy(self._nfs_module_export)
        applied_nfs_export = deepcopy(self._applied_export)
        updated_nfs_export['pseudo'] = 'updated-pseudo'
        export_mgr.get_export_by_pseudo.return_value = updated_nfs_export
        export_mgr.apply_export.return_value = applied_nfs_export
        mgr.remote.return_value = export_mgr

        updated_export_body = deepcopy(self._expected_export)
        updated_export_body['pseudo'] = updated_nfs_export['pseudo']
        applied_nfs_export.append(updated_export_body)

        self._put('/api/nfs-ganesha/export/myc/2',
                  updated_export_body,
                  version=APIVersion(2, 0))
        self.assertStatus(200)
        self.assertJsonBody(applied_nfs_export.changes[0])

    def test_delete_export(self):
        mgr.remote = Mock(side_effect=[self._nfs_module_export, None])

        self._delete('/api/nfs-ganesha/export/myc/2',
                     version=APIVersion(2, 0))
        self.assertStatus(204)

    def test_delete_export_not_found(self):
        mgr.remote = Mock(return_value=None)

        self._delete('/api/nfs-ganesha/export/myc/3',
                     version=APIVersion(2, 0))
        self.assertStatus(404)


class NFSGaneshaUiControllerTest(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        cls.setup_controllers([NFSGaneshaUi])

    @classmethod
    def _create_ls_dir_url(cls, fs_name, query_params):
        api_url = '/ui-api/nfs-ganesha/lsdir/{}'.format(fs_name)
        if query_params is not None:
            return '{}?{}'.format(api_url, urlencode(query_params))
        return api_url

    @patch('dashboard.controllers.nfs.CephFS')
    def test_lsdir(self, cephfs_class):
        cephfs_class.return_value.ls_dir.return_value = [
            {'path': '/foo'},
            {'path': '/foo/bar'}
        ]
        mocked_ls_dir = cephfs_class.return_value.ls_dir

        reqs = [
            {
                'params': None,
                'cephfs_ls_dir_args': ['/', 1],
                'path0': '/',
                'status': 200
            },
            {
                'params': {'root_dir': '/', 'depth': '1'},
                'cephfs_ls_dir_args': ['/', 1],
                'path0': '/',
                'status': 200
            },
            {
                'params': {'root_dir': '', 'depth': '1'},
                'cephfs_ls_dir_args': ['/', 1],
                'path0': '/',
                'status': 200
            },
            {
                'params': {'root_dir': '/foo', 'depth': '3'},
                'cephfs_ls_dir_args': ['/foo', 3],
                'path0': '/foo',
                'status': 200
            },
            {
                'params': {'root_dir': 'foo', 'depth': '6'},
                'cephfs_ls_dir_args': ['/foo', 5],
                'path0': '/foo',
                'status': 200
            },
            {
                'params': {'root_dir': '/', 'depth': '-1'},
                'status': 400
            },
            {
                'params': {'root_dir': '/', 'depth': 'abc'},
                'status': 400
            }
        ]

        for req in reqs:
            self._get(self._create_ls_dir_url('a', req['params']))
            self.assertStatus(req['status'])

            # Returned paths should contain root_dir as first element
            if req['status'] == 200:
                paths = self.json_body()['paths']
                self.assertEqual(paths[0], req['path0'])
                cephfs_class.assert_called_once_with('a')

            # Check the arguments passed to `CephFS.ls_dir`.
            if req.get('cephfs_ls_dir_args'):
                mocked_ls_dir.assert_called_once_with(*req['cephfs_ls_dir_args'])
            else:
                mocked_ls_dir.assert_not_called()
            mocked_ls_dir.reset_mock()
            cephfs_class.reset_mock()

    @patch('dashboard.controllers.nfs.cephfs')
    @patch('dashboard.controllers.nfs.CephFS')
    def test_lsdir_non_existed_dir(self, cephfs_class, cephfs):
        cephfs.ObjectNotFound = Exception
        cephfs.PermissionError = Exception
        cephfs_class.return_value.ls_dir.side_effect = cephfs.ObjectNotFound()
        self._get(self._create_ls_dir_url('a', {'root_dir': '/foo', 'depth': '3'}))
        cephfs_class.assert_called_once_with('a')
        cephfs_class.return_value.ls_dir.assert_called_once_with('/foo', 3)
        self.assertStatus(200)
        self.assertJsonBody({'paths': []})

    def test_status_available(self):
        self._get('/ui-api/nfs-ganesha/status')
        self.assertStatus(200)
        self.assertJsonBody({'available': True, 'message': None})

    def test_status_not_available(self):
        mgr.remote = Mock(side_effect=RuntimeError('Test'))
        self._get('/ui-api/nfs-ganesha/status')
        self.assertStatus(200)
        self.assertJsonBody({'available': False, 'message': 'Test'})
