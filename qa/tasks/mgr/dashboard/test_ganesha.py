# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods

from __future__ import absolute_import

import time

from .helper import DashboardTestCase, JObj, JLeaf, JList


class GaneshaTest(DashboardTestCase):
    CEPHFS = True
    AUTH_ROLES = ['pool-manager', 'ganesha-manager']

    @classmethod
    def create_pool(cls, name, pg_num, pool_type, application='rbd'):
        data = {
            'pool': name,
            'pg_num': pg_num,
            'pool_type': pool_type,
            'application_metadata': [application]
        }
        if pool_type == 'erasure':
            data['flags'] = ['ec_overwrites']
        cls._task_post("/api/pool", data)

    @classmethod
    def setUpClass(cls):
        super(GaneshaTest, cls).setUpClass()
        cls.create_pool('ganesha', 3, 'replicated')
        cls._rados_cmd(['-p', 'ganesha', '-N', 'ganesha1', 'create', 'conf-node1'])
        cls._rados_cmd(['-p', 'ganesha', '-N', 'ganesha1', 'create', 'conf-node2'])
        cls._rados_cmd(['-p', 'ganesha', '-N', 'ganesha1', 'create', 'conf-node3'])
        cls._rados_cmd(['-p', 'ganesha', '-N', 'ganesha2', 'create', 'conf-node1'])
        cls._rados_cmd(['-p', 'ganesha', '-N', 'ganesha2', 'create', 'conf-node2'])
        cls._rados_cmd(['-p', 'ganesha', '-N', 'ganesha2', 'create', 'conf-node3'])
        cls._ceph_cmd(['dashboard', 'set-ganesha-clusters-rados-pool-namespace', 'cluster1:ganesha/ganesha1,cluster2:ganesha/ganesha2'])

        # RGW setup
        cls._radosgw_admin_cmd([
            'user', 'create', '--uid', 'admin', '--display-name', 'admin',
            '--system', '--access-key', 'admin', '--secret', 'admin'
        ])
        cls._ceph_cmd(['dashboard', 'set-rgw-api-secret-key', 'admin'])
        cls._ceph_cmd(['dashboard', 'set-rgw-api-access-key', 'admin'])

    @classmethod
    def tearDownClass(cls):
        super(GaneshaTest, cls).tearDownClass()
        cls._radosgw_admin_cmd(['user', 'rm', '--uid', 'admin', '--purge-data'])
        cls._ceph_cmd(['osd', 'pool', 'delete', 'ganesha', 'ganesha', '--yes-i-really-really-mean-it'])

    @DashboardTestCase.RunAs('test', 'test', [{'rbd-image': ['create', 'update', 'delete']}])
    def test_read_access_permissions(self):
        self._get('/api/nfs-ganesha/export')
        self.assertStatus(403)

    def test_list_daemons(self):
        daemons = self._get("/api/nfs-ganesha/daemon")
        self.assertEqual(len(daemons), 6)
        daemons = [(d['daemon_id'], d['cluster_id']) for d in daemons]
        self.assertIn(('node1', 'cluster1'), daemons)
        self.assertIn(('node2', 'cluster1'), daemons)
        self.assertIn(('node3', 'cluster1'), daemons)
        self.assertIn(('node1', 'cluster2'), daemons)
        self.assertIn(('node2', 'cluster2'), daemons)
        self.assertIn(('node3', 'cluster2'), daemons)

    @classmethod
    def create_export(cls, path, cluster_id, daemons, fsal, sec_label_xattr=None):
        if fsal == 'CEPH':
            fsal = {"name": "CEPH", "user_id":"admin", "fs_name": None, "sec_label_xattr": sec_label_xattr}
            pseudo = "/cephfs{}".format(path)
        else:
            fsal = {"name": "RGW", "rgw_user_id": "admin"}
            pseudo = "/rgw/{}".format(path if path[0] != '/' else "")
        ex_json = {
            "path": path,
            "fsal": fsal,
            "cluster_id": cluster_id,
            "daemons": ["node1", "node3"],
            "pseudo": pseudo,
            "tag": None,
            "access_type": "RW",
            "squash": "no_root_squash",
            "security_label": sec_label_xattr is not None,
            "protocols": [4],
            "transports": ["TCP"],
            "clients": [{
                "addresses":["10.0.0.0/8"],
                "access_type": "RO",
                "squash": "root"
            }]
        }
        return cls._task_post('/api/nfs-ganesha/export', ex_json)

    def tearDown(self):
        super(GaneshaTest, self).tearDown()
        exports = self._get("/api/nfs-ganesha/export")
        if self._resp.status_code != 200:
            return
        self.assertIsInstance(exports, list)
        for exp in exports:
            self._task_delete("/api/nfs-ganesha/export/{}/{}"
                              .format(exp['cluster_id'], exp['export_id']))

    def test_create_export(self):
        exports = self._get("/api/nfs-ganesha/export")
        self.assertEqual(len(exports), 0)

        data = self.create_export("/foo", 'cluster1', ['node1', 'node2'], 'CEPH', "security.selinux")

        exports = self._get("/api/nfs-ganesha/export")
        self.assertEqual(len(exports), 1)
        self.assertDictEqual(exports[0], data)
        return data

    def test_update_export(self):
        export = self.test_create_export()
        export['access_type'] = 'RO'
        export['daemons'] = ['node1', 'node3']
        export['security_label'] = True
        data = self._task_put('/api/nfs-ganesha/export/{}/{}'
                              .format(export['cluster_id'], export['export_id']),
                              export)
        exports = self._get("/api/nfs-ganesha/export")
        self.assertEqual(len(exports), 1)
        self.assertDictEqual(exports[0], data)
        self.assertEqual(exports[0]['daemons'], ['node1', 'node3'])
        self.assertEqual(exports[0]['security_label'], True)

    def test_delete_export(self):
        export = self.test_create_export()
        self._task_delete("/api/nfs-ganesha/export/{}/{}"
                          .format(export['cluster_id'], export['export_id']))
        self.assertStatus(204)

    def test_get_export(self):
        exports = self._get("/api/nfs-ganesha/export")
        self.assertEqual(len(exports), 0)

        data1 = self.create_export("/foo", 'cluster2', ['node1', 'node2'], 'CEPH')
        data2 = self.create_export("mybucket", 'cluster2', ['node2', 'node3'], 'RGW')

        export1 = self._get("/api/nfs-ganesha/export/cluster2/1")
        self.assertDictEqual(export1, data1)

        export2 = self._get("/api/nfs-ganesha/export/cluster2/2")
        self.assertDictEqual(export2, data2)
