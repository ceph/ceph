# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods

from contextlib import contextmanager

from .helper import DashboardTestCase, JLeaf, JList, JObj


class CephfsTest(DashboardTestCase):
    CEPHFS = True

    AUTH_ROLES = ['cephfs-manager']

    QUOTA_PATH = '/quotas'

    def assertToHave(self, data, key):
        self.assertIn(key, data)
        self.assertIsNotNone(data[key])

    def get_fs_id(self):
        return self.fs.get_namespace_id()

    def mk_dirs(self, path, expectedStatus=200):
        self._post("/api/cephfs/{}/tree".format(self.get_fs_id()),
                   params={'path': path})
        self.assertStatus(expectedStatus)

    def rm_dir(self, path, expectedStatus=200):
        self._delete("/api/cephfs/{}/tree".format(self.get_fs_id()),
                     params={'path': path})
        self.assertStatus(expectedStatus)

    def get_root_directory(self, expectedStatus=200):
        data = self._get("/api/cephfs/{}/get_root_directory".format(self.get_fs_id()))
        self.assertStatus(expectedStatus)
        self.assertIsInstance(data, dict)
        return data

    def ls_dir(self, path, expectedLength, depth=None):
        return self._ls_dir(path, expectedLength, depth, "api")

    def ui_ls_dir(self, path, expectedLength, depth=None):
        return self._ls_dir(path, expectedLength, depth, "ui-api")

    def _ls_dir(self, path, expectedLength, depth, baseApiPath):
        params = {'path': path}
        if depth is not None:
            params['depth'] = depth
        data = self._get("/{}/cephfs/{}/ls_dir".format(baseApiPath, self.get_fs_id()),
                         params=params)
        self.assertStatus(200)
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), expectedLength)
        return data

    def set_quotas(self, max_bytes=None, max_files=None):
        quotas = {
            'max_bytes': max_bytes,
            'max_files': max_files
        }
        self._put("/api/cephfs/{}/quota".format(self.get_fs_id()), data=quotas,
                  params={'path': self.QUOTA_PATH})
        self.assertStatus(200)

    def assert_quotas(self, max_bytes, files):
        data = self.ls_dir('/', 1)[0]
        self.assertEqual(data['quotas']['max_bytes'], max_bytes)
        self.assertEqual(data['quotas']['max_files'], files)

    @contextmanager
    def new_quota_dir(self):
        self.mk_dirs(self.QUOTA_PATH)
        self.set_quotas(1024 ** 3, 1024)
        yield 1
        self.rm_dir(self.QUOTA_PATH)

    def write_to_file(self, path, buf):
        params = {'path': path, 'buf': buf}
        self._post(f"/api/cephfs/{self.get_fs_id()}/write_to_file",
                   params=params)
        self.assertStatus(200)

    def unlink(self, path, expectedStatus=200):
        params = {'path': path}
        self._delete(f"/api/cephfs/{self.get_fs_id()}/unlink",
                     params=params)
        self.assertStatus(expectedStatus)

    def statfs(self, path):
        params = {'path': path}
        data = self._get(f"/api/cephfs/{self.get_fs_id()}/statfs",
                         params=params)
        self.assertStatus(200)
        self.assertIsInstance(data, dict)
        return data

    def rename_path(self, src_path, dst_path):
        params = {'src_path': src_path, 'dst_path': dst_path}
        self._put(f"/api/cephfs/{self.get_fs_id()}/rename-path",
                  data=params)
        self.assertStatus(200)

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

    def test_cephfs_evict_invalid_client_id(self):
        fs_id = self.get_fs_id()
        self._delete("/api/cephfs/{}/client/xyz".format(fs_id))
        self.assertStatus(400)
        self.assertJsonBody({
            "component": 'cephfs',
            "code": "invalid_cephfs_client_id",
            "detail": "Invalid cephfs client ID xyz"
        })

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

    def test_cephfs_get_quotas(self):
        fs_id = self.get_fs_id()
        data = self._get("/api/cephfs/{}/quota?path=/".format(fs_id))
        self.assertStatus(200)
        self.assertSchema(data, JObj({
            'max_bytes': int,
            'max_files': int
        }))

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
        self.assertIsInstance(data['name'], str)

        # Standbys
        self.assertToHave(data, 'standbys')
        self.assertIsInstance(data['standbys'], str)

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

        self._post("/api/cephfs/{}/snapshot".format(fs_id),
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

        self._delete("/api/cephfs/{}/snapshot".format(fs_id),
                     params={'path': '/movies/dune', 'name': 'test'})
        self.assertStatus(200)

        data = self.ls_dir('/movies', 1)
        self.assertEqual(len(data[0]['snapshots']), 0)

        # Cleanup. Note, the CephFS Python extension (and therefor the Dashboard
        # REST API) does not support recursive deletion of a directory.
        self.rm_dir('/movies/dune/extended_version')
        self.rm_dir('/movies/dune')
        self.rm_dir('/movies')

    def test_quotas_default(self):
        self.mk_dirs(self.QUOTA_PATH)
        self.assert_quotas(0, 0)
        self.rm_dir(self.QUOTA_PATH)

    def test_quotas_set_both(self):
        with self.new_quota_dir():
            self.assert_quotas(1024 ** 3, 1024)

    def test_quotas_set_only_bytes(self):
        with self.new_quota_dir():
            self.set_quotas(2048 ** 3)
            self.assert_quotas(2048 ** 3, 1024)

    def test_quotas_set_only_files(self):
        with self.new_quota_dir():
            self.set_quotas(None, 2048)
            self.assert_quotas(1024 ** 3, 2048)

    def test_quotas_unset_both(self):
        with self.new_quota_dir():
            self.set_quotas(0, 0)
            self.assert_quotas(0, 0)

    def test_listing_of_root_dir(self):
        self.ls_dir('/', 0)  # Should not list root
        ui_root = self.ui_ls_dir('/', 1)[0]  # Should list root by default
        root = self.get_root_directory()
        self.assertEqual(ui_root, root)

    def test_listing_of_ui_api_ls_on_deeper_levels(self):
        # The UI-API and API ls_dir methods should behave the same way on deeper levels
        self.mk_dirs('/pictures')
        api_ls = self.ls_dir('/pictures', 0)
        ui_api_ls = self.ui_ls_dir('/pictures', 0)
        self.assertEqual(api_ls, ui_api_ls)
        self.rm_dir('/pictures')

    def test_statfs(self):
        self.statfs('/')

        self.mk_dirs('/animal')
        stats = self.statfs('/animal')
        self.assertEqual(stats['bytes'], 0)
        self.assertEqual(stats['files'], 0)
        self.assertEqual(stats['subdirs'], 1)

        buf = 'a' * 512
        self.write_to_file('/animal/lion', buf)
        stats = self.statfs('/animal')
        self.assertEqual(stats['bytes'], 512)
        self.assertEqual(stats['files'], 1)
        self.assertEqual(stats['subdirs'], 1)

        buf = 'b' * 512
        self.write_to_file('/animal/tiger', buf)
        stats = self.statfs('/animal')
        self.assertEqual(stats['bytes'], 1024)
        self.assertEqual(stats['files'], 2)
        self.assertEqual(stats['subdirs'], 1)

        self.unlink('/animal/tiger')
        stats = self.statfs('/animal')
        self.assertEqual(stats['bytes'], 512)
        self.assertEqual(stats['files'], 1)
        self.assertEqual(stats['subdirs'], 1)

        self.unlink('/animal/lion')
        stats = self.statfs('/animal')
        self.assertEqual(stats['bytes'], 0)
        self.assertEqual(stats['files'], 0)
        self.assertEqual(stats['subdirs'], 1)

        self.rm_dir('/animal')

    def test_cephfs_clients_get_after_mds_down(self):
        fs_id = self.get_fs_id()
        self._get(f"/api/cephfs/{fs_id}/clients")
        self.assertStatus(200)

        self.fs.fail()
        params = {'suppress_client_ls_errors': 'False'}
        self._get(f"/api/cephfs/{fs_id}/clients", params=params)
        self.assertStatus(500)

        self.fs.set_joinable()
        self._get(f"/api/cephfs/{fs_id}/clients")
        self.assertStatus(200)

    def test_rename_path(self):
        self.mk_dirs('/apple')
        self.rename_path('/apple', '/orange')
        self.ls_dir('/orange', 0)
        self.rm_dir('/orange')
