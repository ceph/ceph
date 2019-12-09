# -*- coding: utf-8 -*-
from __future__ import absolute_import

import six

from .helper import DashboardTestCase, JObj, JList, JLeaf


class CephfsTest(DashboardTestCase):
    CEPHFS = True

    AUTH_ROLES = ['cephfs-manager']

    def assertToHave(self, data, key):
        self.assertIn(key, data)
        self.assertIsNotNone(data[key])

    def get_fs_id(self):
        return self.fs.get_namespace_id()

    def mk_dirs(self, path, expectedStatus=200):
        self._post("/api/cephfs/{}/mk_dirs".format(self.get_fs_id()),
                   params={'path': path})
        self.assertStatus(expectedStatus)

    def rm_dir(self, path, expectedStatus=200):
        self._post("/api/cephfs/{}/rm_dir".format(self.get_fs_id()),
                   params={'path': path})
        self.assertStatus(expectedStatus)

    def ls_dir(self, path, expectedLength, depth = None):
        params = {'path': path}
        if depth is not None:
            params['depth'] = depth
        data = self._get("/api/cephfs/{}/ls_dir".format(self.get_fs_id()),
                         params=params)
        self.assertStatus(200)
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), expectedLength)
        return data


    @DashboardTestCase.RunAs('test', 'test', ['block-manager'])
    def test_access_permissions(self):
        fs_id = self.get_fs_id()
        self._get("/api/cephfs/{}/clients".format(fs_id))
        self.assertStatus(403)
        self._get("/api/cephfs/{}".format(fs_id))
        self.assertStatus(403)
        self._get("/api/cephfs/{}/mds_counters".format(fs_id))
        self.assertStatus(403)
        self._get("/ui-api/cephfs/{}/tabs".format(fs_id))
        self.assertStatus(403)

    def test_cephfs_clients(self):
        fs_id = self.get_fs_id()
        data = self._get("/api/cephfs/{}/clients".format(fs_id))
        self.assertStatus(200)

        self.assertIn('status', data)
        self.assertIn('data', data)

    def test_cephfs_evict_client_does_not_exist(self):
        fs_id = self.get_fs_id()
        self._delete("/api/cephfs/{}/client/1234".format(fs_id))
        self.assertStatus(404)

    def test_cephfs_get(self):
        fs_id = self.get_fs_id()
        data = self._get("/api/cephfs/{}/".format(fs_id))
        self.assertStatus(200)

        self.assertToHave(data, 'cephfs')
        self.assertToHave(data, 'standbys')
        self.assertToHave(data, 'versions')

    def test_cephfs_mds_counters(self):
        fs_id = self.get_fs_id()
        data = self._get("/api/cephfs/{}/mds_counters".format(fs_id))
        self.assertStatus(200)

        self.assertIsInstance(data, dict)
        self.assertIsNotNone(data)

    def test_cephfs_mds_counters_wrong(self):
        self._get("/api/cephfs/baadbaad/mds_counters")
        self.assertStatus(400)
        self.assertJsonBody({
            "component": 'cephfs',
            "code": "invalid_cephfs_id",
            "detail": "Invalid cephfs ID baadbaad"
        })

    def test_cephfs_list(self):
        data = self._get("/api/cephfs/")
        self.assertStatus(200)

        self.assertIsInstance(data, list)
        cephfs = data[0]
        self.assertToHave(cephfs, 'id')
        self.assertToHave(cephfs, 'mdsmap')

    def test_cephfs_tabs(self):
        fs_id = self.get_fs_id()
        data = self._get("/ui-api/cephfs/{}/tabs".format(fs_id))
        self.assertStatus(200)
        self.assertIsInstance(data, dict)

        # Pools
        pools = data['pools']
        self.assertIsInstance(pools, list)
        self.assertGreater(len(pools), 0)
        for pool in pools:
            self.assertEqual(pool['size'], pool['used'] + pool['avail'])

        # Ranks
        self.assertToHave(data, 'ranks')
        self.assertIsInstance(data['ranks'], list)

        # Name
        self.assertToHave(data, 'name')
        self.assertIsInstance(data['name'], six.string_types)

        # Standbys
        self.assertToHave(data, 'standbys')
        self.assertIsInstance(data['standbys'], six.string_types)

        # MDS counters
        counters = data['mds_counters']
        self.assertIsInstance(counters, dict)
        self.assertGreater(len(counters.keys()), 0)
        for k, v in counters.items():
            self.assertEqual(v['name'], k)

        # Clients
        self.assertToHave(data, 'clients')
        clients = data['clients']
        self.assertToHave(clients, 'data')
        self.assertIsInstance(clients['data'], list)
        self.assertToHave(clients, 'status')
        self.assertIsInstance(clients['status'], int)

    def test_ls_mk_rm_dir(self):
        self.ls_dir('/', 0)

        self.mk_dirs('/pictures/birds')
        self.ls_dir('/', 2, 3)
        self.ls_dir('/pictures', 1)

        self.rm_dir('/pictures', 500)
        self.rm_dir('/pictures/birds')
        self.rm_dir('/pictures')

        self.ls_dir('/', 0)

    def test_snapshots(self):
        fs_id = self.get_fs_id()
        self.mk_dirs('/movies/dune/extended_version')

        self._post("/api/cephfs/{}/mk_snapshot".format(fs_id),
                   params={'path': '/movies/dune', 'name': 'test'})
        self.assertStatus(200)

        data = self.ls_dir('/movies', 1)
        self.assertSchema(data[0], JObj(sub_elems={
            'name': JLeaf(str),
            'path': JLeaf(str),
            'parent': JLeaf(str),
            'snapshots': JList(JObj(sub_elems={
                'name': JLeaf(str),
                'path': JLeaf(str),
                'created': JLeaf(str)
            })),
            'quotas': JObj(sub_elems={
                'max_bytes': JLeaf(int),
                'max_files': JLeaf(int)
            })
        }))
        snapshots = data[0]['snapshots']
        self.assertEqual(len(snapshots), 1)
        snapshot = snapshots[0]
        self.assertEqual(snapshot['name'], "test")
        self.assertEqual(snapshot['path'], "/movies/dune/.snap/test")

        # Should have filtered out "_test_$timestamp"
        data = self.ls_dir('/movies/dune', 1)
        snapshots = data[0]['snapshots']
        self.assertEqual(len(snapshots), 0)

        self._post("/api/cephfs/{}/rm_snapshot".format(fs_id),
                   params={'path': '/movies/dune', 'name': 'test'})
        self.assertStatus(200)

        data = self.ls_dir('/movies', 1)
        self.assertEqual(len(data[0]['snapshots']), 0)

        # Cleanup. Note, the CephFS Python extension (and therefor the Dashboard
        # REST API) does not support recursive deletion of a directory.
        self.rm_dir('/movies/dune/extended_version')
        self.rm_dir('/movies/dune')
        self.rm_dir('/movies')
