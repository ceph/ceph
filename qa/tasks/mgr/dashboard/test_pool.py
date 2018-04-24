# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

import six

from .helper import DashboardTestCase, authenticate

log = logging.getLogger(__name__)


class PoolTest(DashboardTestCase):
    @classmethod
    def tearDownClass(cls):
        super(PoolTest, cls).tearDownClass()
        for name in ['dashboard_pool1', 'dashboard_pool2', 'dashboard_pool3']:
            cls._ceph_cmd(['osd', 'pool', 'delete', name, name, '--yes-i-really-really-mean-it'])
        cls._ceph_cmd(['osd', 'erasure-code-profile', 'rm', 'ecprofile'])

    @authenticate
    def test_pool_list(self):
        data = self._get("/api/pool")
        self.assertStatus(200)

        cluster_pools = self.ceph_cluster.mon_manager.list_pools()
        self.assertEqual(len(cluster_pools), len(data))
        for pool in data:
            self.assertIn('pool_name', pool)
            self.assertIn('type', pool)
            self.assertIn('application_metadata', pool)
            self.assertIsInstance(pool['application_metadata'], list)
            self.assertIn('flags', pool)
            self.assertIn('flags_names', pool)
            self.assertNotIn('stats', pool)
            self.assertIn(pool['pool_name'], cluster_pools)

    @authenticate
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

    @authenticate
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

    @authenticate
    def test_pool_get(self):
        cluster_pools = self.ceph_cluster.mon_manager.list_pools()
        pool = self._get("/api/pool/{}?stats=true&attrs=type,flags,stats"
                         .format(cluster_pools[0]))
        self.assertEqual(pool['pool_name'], cluster_pools[0])
        self.assertIn('type', pool)
        self.assertIn('flags', pool)
        self.assertIn('stats', pool)
        self.assertNotIn('flags_names', pool)

    @authenticate
    def _pool_create(self, data):
        try:
            self._post('/api/pool/', data)
            self.assertStatus(201)

            pool = self._get("/api/pool/" + data['pool'])
            self.assertStatus(200)
            try:
                for k, v in data.items():
                    if k == 'pool_type':
                        self.assertEqual(pool['type'], data['pool_type'])
                    elif k == 'pg_num':
                        self.assertEqual(pool[k], int(v), '{}: {} != {}'.format(k, pool[k], v))
                    elif k == 'application_metadata':
                        self.assertIsInstance(pool[k], list)
                        self.assertEqual(pool[k],
                                         data['application_metadata'].split(','))
                    elif k == 'pool':
                        self.assertEqual(pool['pool_name'], v)
                    elif k in ['compression_mode', 'compression_algorithm',
                               'compression_max_blob_size']:
                        self.assertEqual(pool['options'][k], data[k])
                    elif k == 'compression_required_ratio':
                        self.assertEqual(pool['options'][k], float(data[k]))
                    else:
                        self.assertEqual(pool[k], v, '{}: {} != {}'.format(k, pool[k], v))

            except Exception:
                log.exception("test_pool_create: pool=%s", pool)
                raise

            self._delete("/api/pool/" + data['pool'])
            self.assertStatus(204)
        except Exception:
            log.exception("test_pool_create: data=%s", data)
            raise

    def test_pool_create(self):
        self._ceph_cmd(['osd', 'crush', 'rule', 'create-erasure', 'ecrule'])
        self._ceph_cmd(
            ['osd', 'erasure-code-profile', 'set', 'ecprofile', 'crush-failure-domain=osd'])
        pools = [{
            'pool': 'dashboard_pool1',
            'pg_num': '10',
            'pool_type': 'replicated',
            'application_metadata': 'rbd',
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
            'compression_max_blob_size': 10000000,
            'compression_required_ratio': '0.8',
        }]
        for data in pools:
            self._pool_create(data)

    @authenticate
    def test_pool_create_fail(self):
        data = {'pool_type': u'replicated', 'rule_name': u'dnf', 'pg_num': u'8', 'pool': u'sadfs'}
        self._post('/api/pool/', data)
        self.assertStatus(400)
        self.assertJsonBody({
            'component': 'pool',
            'code': "2",
            'detail': "specified rule dnf doesn't exist"
        })

    @authenticate
    def test_pool_info(self):
        info_data = self._get("/api/pool/_info")
        self.assertEqual(set(info_data),
                         {'pool_names', 'crush_rules_replicated', 'crush_rules_erasure',
                          'is_all_bluestore', 'compression_algorithms', 'compression_modes',
                          'osd_count'})
        self.assertTrue(all(isinstance(n, six.string_types) for n in info_data['pool_names']))
        self.assertTrue(
            all(isinstance(n, dict) for n in info_data['crush_rules_replicated']))
        self.assertTrue(
            all(isinstance(n, dict) for n in info_data['crush_rules_erasure']))
        self.assertIsInstance(info_data['is_all_bluestore'], bool)
        self.assertIsInstance(info_data['osd_count'], int)
        self.assertTrue(
            all(isinstance(n, six.string_types) for n in info_data['compression_algorithms']))
        self.assertTrue(
            all(isinstance(n, six.string_types) for n in info_data['compression_modes']))


