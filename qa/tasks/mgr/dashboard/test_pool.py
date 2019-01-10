# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging

import six
import time

from .helper import DashboardTestCase, JAny, JList, JObj

log = logging.getLogger(__name__)


class PoolTest(DashboardTestCase):
    AUTH_ROLES = ['pool-manager']

    pool_schema = JObj(sub_elems={
        'pool_name': str,
        'type': str,
        'application_metadata': JList(str),
        'flags': int,
        'flags_names': str,
    }, allow_unknown=True)

    pool_list_stat_schema = JObj(sub_elems={
        'latest': int,
        'rate': float,
        'series': JList(JAny(none=False)),
    })

    pool_list_stats_schema = JObj(sub_elems={
        'bytes_used': pool_list_stat_schema,
        'max_avail': pool_list_stat_schema,
        'rd_bytes': pool_list_stat_schema,
        'wr_bytes': pool_list_stat_schema,
        'rd': pool_list_stat_schema,
        'wr': pool_list_stat_schema,
    }, allow_unknown=True)

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
        pool = self._get_pool(pool_name)
        try:
            for k, v in data.items():
                self._check_pool_property(k, v, pool)

        except Exception:
            log.exception("test_pool_create: pool=%s", pool)
            raise

        health = self._get('/api/health/minimal')['health']
        self.assertEqual(health['status'], 'HEALTH_OK', msg='health={}'.format(health))

    def _get_pool(self, pool_name):
        pool = self._get("/api/pool/" + pool_name)
        self.assertStatus(200)
        self.assertSchemaBody(self.pool_schema)
        return pool

    def _check_pool_property(self, prop, value, pool):
        if prop == 'pool_type':
            self.assertEqual(pool['type'], value)
        elif prop == 'size':
            self.assertEqual(pool[prop], int(value), '{}: {} != {}'.format(prop, pool[prop], value))
        elif prop == 'pg_num':
            self._check_pg_num(value, pool)
        elif prop == 'application_metadata':
            self.assertIsInstance(pool[prop], list)
            self.assertEqual(pool[prop], value)
        elif prop == 'pool':
            self.assertEqual(pool['pool_name'], value)
        elif prop.startswith('compression'):
            if value is not None:
                if prop.endswith('size'):
                    value = int(value)
                elif prop.endswith('ratio'):
                    value = float(value)
            self.assertEqual(pool['options'].get(prop), value)
        else:
            self.assertEqual(pool[prop], value, '{}: {} != {}'.format(prop, pool[prop], value))

    def _check_pg_num(self, value, pool):
        # If both properties have not the same value, the cluster goes into a warning state,
        # which will only happen during a pg update on a existing pool.
        # The test that does that is currently commented out because
        # our QA systems can't deal with the change.
        # Feel free to test it locally.
        prop = 'pg_num'
        pgp_prop = 'pg_placement_num'
        health = lambda: self._get('/api/health/minimal')['health']['status'] == 'HEALTH_OK'
        t = 0;
        while (int(value) != pool[pgp_prop] or not health()) and t < 180:
            time.sleep(2)
            t += 2
            pool = self._get_pool(pool['pool_name'])
        for p in [prop, pgp_prop]:  # Should have the same values
            self.assertEqual(pool[p], int(value), '{}: {} != {}'.format(p, pool[p], value))

    @classmethod
    def tearDownClass(cls):
        super(PoolTest, cls).tearDownClass()
        for name in ['dashboard_pool1', 'dashboard_pool2', 'dashboard_pool3', 'dashboard_pool_update1']:
            cls._ceph_cmd(['osd', 'pool', 'delete', name, name, '--yes-i-really-really-mean-it'])
        cls._ceph_cmd(['osd', 'erasure-code-profile', 'rm', 'ecprofile'])

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
            self.assertNotIn('pg_status', pool)
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
            self.assertNotIn('pg_status', pool)
            self.assertNotIn('stats', pool)
            self.assertIn(pool['pool_name'], cluster_pools)

    def test_pool_list_stats(self):
        data = self._get("/api/pool?stats=true")
        self.assertStatus(200)

        cluster_pools = self.ceph_cluster.mon_manager.list_pools()
        self.assertEqual(len(cluster_pools), len(data))
        self.assertSchemaBody(JList(self.pool_schema))
        for pool in data:
            self.assertIn('pool_name', pool)
            self.assertIn('type', pool)
            self.assertIn('application_metadata', pool)
            self.assertIn('flags', pool)
            self.assertIn('pg_status', pool)
            self.assertSchema(pool['stats'], self.pool_list_stats_schema)
            self.assertIn('flags_names', pool)
            self.assertIn(pool['pool_name'], cluster_pools)

    def test_pool_get(self):
        cluster_pools = self.ceph_cluster.mon_manager.list_pools()
        pool = self._get("/api/pool/{}?stats=true&attrs=type,flags,stats"
                         .format(cluster_pools[0]))
        self.assertEqual(pool['pool_name'], cluster_pools[0])
        self.assertIn('type', pool)
        self.assertIn('flags', pool)
        self.assertNotIn('pg_status', pool)
        self.assertSchema(pool['stats'], self.pool_list_stats_schema)
        self.assertNotIn('flags_names', pool)

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
        pool = {
            'pool': 'dashboard_pool_update1',
            'pg_num': '4',
            'pool_type': 'replicated',
            'compression_mode': 'passive',
            'compression_algorithm': 'snappy',
            'compression_max_blob_size': '131072',
            'compression_required_ratio': '0.875',
        }
        updates = [
            {
                'application_metadata': ['rbd', 'sth'],
            },
            # The following test case is currently commented out because
            # our QA systems can't deal with the change and will fail because
            # they can't recover from the resulting warning state.
            # Feel free to test it locally.
            # {
            #     'pg_num': '8',
            # },
            {
                'application_metadata': ['rgw'],
            },
            {
                'compression_algorithm': 'zstd',
                'compression_mode': 'aggressive',
                'compression_max_blob_size': '10000000',
                'compression_required_ratio': '0.8',
            },
            {
                'compression_mode': 'unset'
            }
        ]
        self._task_post('/api/pool/', pool)
        self.assertStatus(201)
        self._check_pool_properties(pool)

        for update in updates:
            self._task_put('/api/pool/' + pool['pool'], update)
            if update.get('compression_mode') == 'unset':
                update = {
                    'compression_mode': None,
                    'compression_algorithm': None,
                    'compression_mode': None,
                    'compression_max_blob_size': None,
                    'compression_required_ratio': None,
                }
            self._check_pool_properties(update, pool_name=pool['pool'])
        self._task_delete("/api/pool/" + pool['pool'])
        self.assertStatus(204)

    def test_pool_create_fail(self):
        data = {'pool_type': u'replicated', 'rule_name': u'dnf', 'pg_num': u'8', 'pool': u'sadfs'}
        self._task_post('/api/pool/', data)
        self.assertStatus(400)
        self.assertJsonBody({
            'component': 'pool',
            'code': "2",
            'detail': "[errno -2] specified rule dnf doesn't exist"
        })

    def test_pool_info(self):
        self._get("/api/pool/_info")
        self.assertSchemaBody(JObj({
            'pool_names': JList(six.string_types),
            'compression_algorithms': JList(six.string_types),
            'compression_modes': JList(six.string_types),
            'is_all_bluestore': bool,
            "bluestore_compression_algorithm": six.string_types,
            'osd_count': int,
            'crush_rules_replicated': JList(JObj({}, allow_unknown=True)),
            'crush_rules_erasure': JList(JObj({}, allow_unknown=True)),
        }))
