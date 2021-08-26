# -*- coding: utf-8 -*-
# pylint: disable=too-many-lines

from unittest.mock import patch
from urllib.parse import urlencode

from ..controllers.nfsganesha import NFSGaneshaExports, NFSGaneshaUi
from . import ControllerTestCase  # pylint: disable=no-name-in-module


class NFSGaneshaExportsTest(ControllerTestCase):

    def test_get_schema_export(self):
        export = {
            "export_id": 2,
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
        expected_schema_export = export
        del expected_schema_export['fsal']['access_key_id']
        del expected_schema_export['fsal']['secret_access_key']
        self.assertDictEqual(
            expected_schema_export,
            NFSGaneshaExports._get_schema_export(export))  # pylint: disable=protected-access


class NFSGaneshaUiControllerTest(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        # pylint: disable=protected-access
        NFSGaneshaUi._cp_config['tools.authenticate.on'] = False
        cls.setup_controllers([NFSGaneshaUi])

    @classmethod
    def _create_ls_dir_url(cls, fs_name, query_params):
        api_url = '/ui-api/nfs-ganesha/lsdir/{}'.format(fs_name)
        if query_params is not None:
            return '{}?{}'.format(api_url, urlencode(query_params))
        return api_url

    @patch('dashboard.controllers.nfsganesha.CephFS')
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

    @patch('dashboard.controllers.nfsganesha.cephfs')
    @patch('dashboard.controllers.nfsganesha.CephFS')
    def test_lsdir_non_existed_dir(self, cephfs_class, cephfs):
        cephfs.ObjectNotFound = Exception
        cephfs.PermissionError = Exception
        cephfs_class.return_value.ls_dir.side_effect = cephfs.ObjectNotFound()
        self._get(self._create_ls_dir_url('a', {'root_dir': '/foo', 'depth': '3'}))
        cephfs_class.assert_called_once_with('a')
        cephfs_class.return_value.ls_dir.assert_called_once_with('/foo', 3)
        self.assertStatus(200)
        self.assertJsonBody({'paths': []})
