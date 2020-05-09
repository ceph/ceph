# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging
import six
import time
from contextlib import contextmanager

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
        'rates': JList(JAny(none=False)),
    })

    pool_list_stats_schema = JObj(sub_elems={
        'bytes_used': pool_list_stat_schema,
        'max_avail': pool_list_stat_schema,
        'rd_bytes': pool_list_stat_schema,
        'wr_bytes': pool_list_stat_schema,
        'rd': pool_list_stat_schema,
        'wr': pool_list_stat_schema,
    }, allow_unknown=True)

    pool_rbd_conf_schema = JList(JObj(sub_elems={
        'name': str,
        'value': str,
        'source': int
    }))

    @contextmanager
    def __yield_pool(self, name=None, data=None, deletion_name=None):
        """
        Use either just a name or whole description of a pool to create one.
        This also validates the correct creation and deletion after the pool was used.

        :param name: Name of the pool
        :param data: Describes the pool in full length
        :param deletion_name: Only needed if the pool was renamed
        :return:
        """
        data = self._create_pool(name, data)
        yield data
        self._delete_pool(deletion_name or data['pool'])

    def _create_pool(self, name, data):
        data = data or {
            'pool': name,
            'pg_num': '32',
            'pool_type': 'replicated',
            'compression_algorithm': 'snappy',
            'compression_mode': 'passive',
            'compression_max_blob_size': '131072',
            'compression_required_ratio': '0.875',
            'application_metadata': ['rbd'],
            'configuration': {
                'rbd_qos_bps_limit': 1024000,
                'rbd_qos_iops_limit': 5000,
            }
        }
        self._task_post('/api/pool/', data)
        self.assertStatus(201)
        self._validate_pool_properties(data, self._get_pool(data['pool']))
        return data

    def _delete_pool(self, name):
        self._task_delete('/api/pool/' + name)
        self.assertStatus(204)

    def _validate_pool_properties(self, data, pool):
        for prop, value in data.items():
            if prop == 'pool_type':
                self.assertEqual(pool['type'], value)
            elif prop == 'size':
                self.assertEqual(pool[prop], int(value),
                                 '{}: {} != {}'.format(prop, pool[prop], value))
            elif prop == 'pg_num':
                self._check_pg_num(value, pool)
            elif prop == 'application_metadata':
                self.assertIsInstance(pool[prop], list)
                self.assertEqual(value, pool[prop])
            elif prop == 'pool':
                self.assertEqual(pool['pool_name'], value)
            elif prop.startswith('compression'):
                if value is not None:
                    if prop.endswith('size'):
                        value = int(value)
                    elif prop.endswith('ratio'):
                        value = float(value)
                    self.assertEqual(pool['options'][prop], value)
                else:
                    self.assertEqual(pool['options'], {})
            elif prop == 'configuration':
                # configuration cannot really be checked here for two reasons:
                #   1.  The default value cannot be given to this method, which becomes relevant
                #       when resetting a value, because it's not always zero.
                #   2.  The expected `source` cannot be given to this method, and it cannot
                #       relibably be determined (see 1)
                pass
            else:
                self.assertEqual(pool[prop], value, '{}: {} != {}'.format(prop, pool[prop], value))

        health = self._get('/api/health/minimal')['health']
        self.assertEqual(health['status'], 'HEALTH_OK', msg='health={}'.format(health))

    def _get_pool(self, pool_name):
        pool = self._get("/api/pool/" + pool_name)
        self.assertStatus(200)
        self.assertSchemaBody(self.pool_schema)
        return pool

    def _check_pg_num(self, value, pool):
        """
        If both properties have not the same value, the cluster goes into a warning state, which
        will only happen during a pg update on an existing pool. The test that does that is
        currently commented out because our QA systems can't deal with the change. Feel free to test
        it locally.
        """
        pgp_prop = 'pg_placement_num'
        t = 0
        while (int(value) != pool[pgp_prop] or self._get('/api/health/minimal')['health']['status']
               != 'HEALTH_OK') and t < 180:
            time.sleep(2)
            t += 2
            pool = self._get_pool(pool['pool_name'])
        for p in ['pg_num', pgp_prop]:  # Should have the same values
            self.assertEqual(pool[p], int(value), '{}: {} != {}'.format(p, pool[p], value))

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
        self.assertSchema(pool['configuration'], self.pool_rbd_conf_schema)

    def test_pool_create_with_two_applications(self):
        self.__yield_pool(None, {
            'pool': 'dashboard_pool1',
            'pg_num': '32',
            'pool_type': 'replicated',
            'application_metadata': ['rbd', 'sth'],
        })

    def test_pool_create_with_ecp_and_rule(self):
        self._ceph_cmd(['osd', 'crush', 'rule', 'create-erasure', 'ecrule'])
        self._ceph_cmd(
            ['osd', 'erasure-code-profile', 'set', 'ecprofile', 'crush-failure-domain=osd'])
        self.__yield_pool(None, {
            'pool': 'dashboard_pool2',
            'pg_num': '32',
            'pool_type': 'erasure',
            'application_metadata': ['rbd'],
            'erasure_code_profile': 'ecprofile',
            'crush_rule': 'ecrule',
        })
        self._ceph_cmd(['osd', 'erasure-code-profile', 'rm', 'ecprofile'])

    def test_pool_create_with_compression(self):
        pool = {
            'pool': 'dashboard_pool3',
            'pg_num': '32',
            'pool_type': 'replicated',
            'compression_algorithm': 'zstd',
            'compression_mode': 'aggressive',
            'compression_max_blob_size': '10000000',
            'compression_required_ratio': '0.8',
            'application_metadata': ['rbd'],
            'configuration': {
                'rbd_qos_bps_limit': 2048,
                'rbd_qos_iops_limit': None,
            },
        }
        with self.__yield_pool(None, pool):
            expected_configuration = [{
                'name': 'rbd_qos_bps_limit',
                'source': 1,
                'value': '2048',
            }, {
                'name': 'rbd_qos_iops_limit',
                'source': 0,
                'value': '0',
            }]
            new_pool = self._get_pool(pool['pool'])
            for conf in expected_configuration:
                self.assertIn(conf, new_pool['configuration'])

    def test_pool_create_with_quotas(self):
        pools = [
            {
                'pool_data': {
                    'pool': 'dashboard_pool_quota1',
                    'pg_num': '32',
                    'pool_type': 'replicated',
                },
                'pool_quotas_to_check': {
                    'quota_max_objects': 0,
                    'quota_max_bytes': 0,
                }
            },
            {
                'pool_data': {
                    'pool': 'dashboard_pool_quota2',
                    'pg_num': '32',
                    'pool_type': 'replicated',
                    'quota_max_objects': 1024,
                    'quota_max_bytes': 1000,
                },
                'pool_quotas_to_check': {
                    'quota_max_objects': 1024,
                    'quota_max_bytes': 1000,
                }
            }
        ]

        for pool in pools:
            pool_name = pool['pool_data']['pool']
            with self.__yield_pool(pool_name, pool['pool_data']):
                self._validate_pool_properties(pool['pool_quotas_to_check'],
                                               self._get_pool(pool_name))

    def test_pool_update_name(self):
        name = 'pool_update'
        updated_name = 'pool_updated_name'
        with self.__yield_pool(name, None, updated_name):
            props = {'pool': updated_name}
            self._task_put('/api/pool/{}'.format(name), props)
            time.sleep(5)
            self.assertStatus(200)
            self._validate_pool_properties(props, self._get_pool(updated_name))

    def test_pool_update_metadata(self):
        pool_name = 'pool_update_metadata'
        with self.__yield_pool(pool_name):
            props = {'application_metadata': ['rbd', 'sth']}
            self._task_put('/api/pool/{}'.format(pool_name), props)
            time.sleep(5)
            self._validate_pool_properties(props, self._get_pool(pool_name))

            properties = {'application_metadata': ['rgw']}
            self._task_put('/api/pool/' + pool_name, properties)
            time.sleep(5)
            self._validate_pool_properties(properties, self._get_pool(pool_name))

            properties = {'application_metadata': ['rbd', 'sth']}
            self._task_put('/api/pool/' + pool_name, properties)
            time.sleep(5)
            self._validate_pool_properties(properties, self._get_pool(pool_name))

            properties = {'application_metadata': ['rgw']}
            self._task_put('/api/pool/' + pool_name, properties)
            time.sleep(5)
            self._validate_pool_properties(properties, self._get_pool(pool_name))

    def test_pool_update_configuration(self):
        pool_name = 'pool_update_configuration'
        with self.__yield_pool(pool_name):
            configuration = {
                'rbd_qos_bps_limit': 1024,
                'rbd_qos_iops_limit': None,
            }
            expected_configuration = [{
                'name': 'rbd_qos_bps_limit',
                'source': 1,
                'value': '1024',
            }, {
                'name': 'rbd_qos_iops_limit',
                'source': 0,
                'value': '0',
            }]
            self._task_put('/api/pool/' + pool_name, {'configuration': configuration})
            time.sleep(5)
            pool_config = self._get_pool(pool_name)['configuration']
            for conf in expected_configuration:
                self.assertIn(conf, pool_config)

    def test_pool_update_compression(self):
        pool_name = 'pool_update_compression'
        with self.__yield_pool(pool_name):
            properties = {
                'compression_algorithm': 'zstd',
                'compression_mode': 'aggressive',
                'compression_max_blob_size': '10000000',
                'compression_required_ratio': '0.8',
            }
            self._task_put('/api/pool/' + pool_name, properties)
            time.sleep(5)
            self._validate_pool_properties(properties, self._get_pool(pool_name))

    def test_pool_update_unset_compression(self):
        pool_name = 'pool_update_unset_compression'
        with self.__yield_pool(pool_name):
            self._task_put('/api/pool/' + pool_name, {'compression_mode': 'unset'})
            time.sleep(5)
            self._validate_pool_properties({
                'compression_algorithm': None,
                'compression_mode': None,
                'compression_max_blob_size': None,
                'compression_required_ratio': None,
            }, self._get_pool(pool_name))

    def test_pool_update_quotas(self):
        pool_name = 'pool_update_quotas'
        with self.__yield_pool(pool_name):
            properties = {
                'quota_max_objects': 1024,
                'quota_max_bytes': 1000,
            }
            self._task_put('/api/pool/' + pool_name, properties)
            time.sleep(5)
            self._validate_pool_properties(properties, self._get_pool(pool_name))

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
        self._get("/ui-api/pool/info")
        self.assertSchemaBody(JObj({
            'pool_names': JList(six.string_types),
            'compression_algorithms': JList(six.string_types),
            'compression_modes': JList(six.string_types),
            'is_all_bluestore': bool,
            'bluestore_compression_algorithm': six.string_types,
            'osd_count': int,
            'crush_rules_replicated': JList(JObj({}, allow_unknown=True)),
            'crush_rules_erasure': JList(JObj({}, allow_unknown=True)),
            'pg_autoscale_default_mode': six.string_types,
            'pg_autoscale_modes': JList(six.string_types),
            'erasure_code_profiles': JList(JObj({}, allow_unknown=True)),
            'used_rules': JObj({}, allow_unknown=True),
            'used_profiles': JObj({}, allow_unknown=True),
        }))
