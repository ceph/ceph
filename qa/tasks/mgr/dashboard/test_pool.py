# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

import six

from .helper import DashboardTestCase, JObj, JList

log = logging.getLogger(__name__)


class PoolTest(DashboardTestCase):
    AUTH_ROLES = ['pool-manager']

    @classmethod
    def tearDownClass(cls):
        super(PoolTest, cls).tearDownClass()
        for name in ['dashboard_pool1', 'dashboard_pool2', 'dashboard_pool3', 'dashboard_pool_update1']:
            cls._ceph_cmd(['osd', 'pool', 'delete', name, name, '--yes-i-really-really-mean-it'])
        cls._ceph_cmd(['osd', 'erasure-code-profile', 'rm', 'ecprofile'])

    pool_schema = JObj(sub_elems={
        'pool_name': str,
        'type': str,
        'application_metadata': JList(str),
        'flags': int,
        'flags_names': str,
    }, allow_unknown=True)

    @DashboardTestCase.RunAs('test', 'test', [{'pool': ['create', 'update', 'delete']}])
    def test_read_access_permissions(self):
        self._get('/api/pool')
        self.assertStatus(403)
        self._get('/api/pool/bla')
        self.assertStatus(403)

    @DashboardTestCase.RunAs('test', 'test', [{'pool': ['read', 'update', 'delete']}])
    def test_create_access_permissions(self):
        self._task_post('/api/pool/', {})
        self.assertStatus(403)

    @DashboardTestCase.RunAs('test', 'test', [{'pool': ['read', 'create', 'update']}])
    def test_delete_access_permissions(self):
        self._delete('/api/pool/ddd')
        self.assertStatus(403)

    def test_pool_list(self):
        data = self._get("/api/pool")
        self.assertStatus(200)

        cluster_pools = self.ceph_cluster.mon_manager.list_pools()
        self.assertEqual(len(cluster_pools), len(data))
        self.assertSchemaBody(JList(self.pool_schema))
        for pool in data:
            self.assertNotIn('stats', pool)
            self.assertIn(pool['pool_name'], cluster_pools)

    def test_pool_list_attrs(self):
        data = self._get("/api/pool?attrs=type,flags")
        self.assertStatus(200)

        cluster_pools = self.ceph_cluster.mon_manager.list_pools()
        self.assertEqual(len(cluster_pools), len(data))
        for pool in data:
            self.assertIn('pool_name', pool)
            self.assertIn('type', pool)
            self.assertIn('flags', pool)
            self.assertNotIn('flags_names', pool)
            self.assertNotIn('stats', pool)
            self.assertIn(pool['pool_name'], cluster_pools)

    def test_pool_list_stats(self):
        data = self._get("/api/pool?stats=true")
        self.assertStatus(200)

        cluster_pools = self.ceph_cluster.mon_manager.list_pools()
        self.assertEqual(len(cluster_pools), len(data))
        for pool in data:
            self.assertIn('pool_name', pool)
            self.assertIn('type', pool)
            self.assertIn('application_metadata', pool)
            self.assertIn('flags', pool)
            self.assertIn('stats', pool)
            self.assertIn('flags_names', pool)
            self.assertIn(pool['pool_name'], cluster_pools)

    def test_pool_get(self):
        cluster_pools = self.ceph_cluster.mon_manager.list_pools()
        pool = self._get("/api/pool/{}?stats=true&attrs=type,flags,stats"
                         .format(cluster_pools[0]))
        self.assertEqual(pool['pool_name'], cluster_pools[0])
        self.assertIn('type', pool)
        self.assertIn('flags', pool)
        self.assertIn('stats', pool)
        self.assertNotIn('flags_names', pool)

    def _pool_create(self, data):
        try:
            self._task_post('/api/pool/', data)
            self.assertStatus(201)

            self._check_pool_properties(data)

            self._task_delete("/api/pool/" + data['pool'])
            self.assertStatus(204)
        except Exception:
            log.exception("test_pool_create: data=%s", data)
            raise

    def _check_pool_properties(self, data, pool_name=None):
        if not pool_name:
            pool_name = data['pool']
        pool = self._get("/api/pool/" + pool_name)
        self.assertStatus(200)
        self.assertSchemaBody(self.pool_schema)
        try:
            for k, v in data.items():
                if k == 'pool_type':
                    self.assertEqual(pool['type'], data['pool_type'])
                elif k == 'pg_num':
                    self.assertEqual(pool[k], int(v), '{}: {} != {}'.format(k, pool[k], v))
                    k = 'pg_placement_num' # Should have the same value as pg_num
                    self.assertEqual(pool[k], int(v), '{}: {} != {}'.format(k, pool[k], v))
                elif k == 'application_metadata':
                    self.assertIsInstance(pool[k], list)
                    self.assertEqual(pool[k],
                                     data['application_metadata'])
                elif k == 'pool':
                    self.assertEqual(pool['pool_name'], v)
                elif k in ['compression_mode', 'compression_algorithm']:
                    self.assertEqual(pool['options'][k], data[k])
                elif k == 'compression_max_blob_size':
                    self.assertEqual(pool['options'][k], int(data[k]))
                elif k == 'compression_required_ratio':
                    self.assertEqual(pool['options'][k], float(data[k]))
                else:
                    self.assertEqual(pool[k], v, '{}: {} != {}'.format(k, pool[k], v))

        except Exception:
            log.exception("test_pool_create: pool=%s", pool)
            raise

        health = self._get('/api/dashboard/health')['health']
        self.assertEqual(health['status'], 'HEALTH_OK', msg='health={}'.format(health))

    def test_pool_create(self):
        self._ceph_cmd(['osd', 'crush', 'rule', 'create-erasure', 'ecrule'])
        self._ceph_cmd(
            ['osd', 'erasure-code-profile', 'set', 'ecprofile', 'crush-failure-domain=osd'])
        pools = [{
            'pool': 'dashboard_pool1',
            'pg_num': '10',
            'pool_type': 'replicated',
            'application_metadata': ['rbd', 'sth'],
        }, {
            'pool': 'dashboard_pool2',
            'pg_num': '10',
            'pool_type': 'erasure',
            'erasure_code_profile': 'ecprofile',
            'crush_rule': 'ecrule',
        }, {
            'pool': 'dashboard_pool3',
            'pg_num': '10',
            'pool_type': 'replicated',
            'compression_algorithm': 'zstd',
            'compression_mode': 'aggressive',
            'compression_max_blob_size': '10000000',
            'compression_required_ratio': '0.8',
        }]
        for data in pools:
            self._pool_create(data)

    def test_update(self):
        pool = [
            {
                'pool': 'dashboard_pool_update1',
                'pg_num': '10',
                'pool_type': 'replicated',
            },
            {
                'application_metadata': ['rbd', 'sth'],
            },
            # {
            #     'pg_num': '12',
            # },
            {
                'application_metadata': ['rgw'],
            },
            {
                'compression_algorithm': 'zstd',
                'compression_mode': 'aggressive',
                'compression_max_blob_size': '10000000',
                'compression_required_ratio': '0.8',
            }

        ]
        self._task_post('/api/pool/', pool[0])
        self.assertStatus(201)

        self._check_pool_properties(pool[0])

        for data in pool[1:]:
            self._task_put('/api/pool/' + pool[0]['pool'], data)
            self._check_pool_properties(data, pool_name=pool[0]['pool'])

        self._task_delete("/api/pool/" + pool[0]['pool'])
        self.assertStatus(204)

    def test_pool_create_fail(self):
        data = {'pool_type': u'replicated', 'rule_name': u'dnf', 'pg_num': u'8', 'pool': u'sadfs'}
        self._task_post('/api/pool/', data)
        self.assertStatus(400)
        self.assertJsonBody({
            'component': 'pool',
            'code': "2",
            'detail': "specified rule dnf doesn't exist"
        })

    def test_pool_info(self):
        self._get("/api/pool/_info")
        self.assertSchemaBody(JObj({
            'pool_names': JList(six.string_types),
            'compression_algorithms': JList(six.string_types),
            'compression_modes': JList(six.string_types),
            'is_all_bluestore': bool,
            'osd_count': int,
            'crush_rules_replicated': JList(JObj({}, allow_unknown=True)),
            'crush_rules_erasure': JList(JObj({}, allow_unknown=True)),
        }))
