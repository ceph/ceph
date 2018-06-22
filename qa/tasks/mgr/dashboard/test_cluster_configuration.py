from __future__ import absolute_import

import time

from .helper import DashboardTestCase


class ClusterConfigurationTest(DashboardTestCase):

    def test_list(self):
        data = self._get('/api/cluster_conf')
        self.assertStatus(200)
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 1000)
        for conf in data:
            self._validate_single(conf)

    def test_get(self):
        data = self._get('/api/cluster_conf/admin_socket')
        self.assertStatus(200)
        self._validate_single(data)
        self.assertIn('enum_values', data)

        data = self._get('/api/cluster_conf/fantasy_name')
        self.assertStatus(404)

    def test_get_specific_db_config_option(self):
        def _get_mon_allow_pool_delete_config():
            data = self._get('/api/cluster_conf/mon_allow_pool_delete')
            if 'value' in data:
                return data['value'][0]
            return None

        orig_value = _get_mon_allow_pool_delete_config()

        self._ceph_cmd(['config', 'set', 'mon', 'mon_allow_pool_delete', 'true'])
        result = self._wait_for_expected_get_result(_get_mon_allow_pool_delete_config,
                                                    {'section': 'mon', 'value': 'true'})
        self.assertEqual(result, {'section': 'mon', 'value': 'true'})

        self._ceph_cmd(['config', 'set', 'mon', 'mon_allow_pool_delete', 'false'])
        result = self._wait_for_expected_get_result(_get_mon_allow_pool_delete_config,
                                                    {'section': 'mon', 'value': 'false'})
        self.assertEqual(result, {'section': 'mon', 'value': 'false'})

        # restore value
        if orig_value:
            self._ceph_cmd(['config', 'set', 'mon', 'mon_allow_pool_delete',
                           orig_value['value']])

    def _validate_single(self, data):
        self.assertIn('name', data)
        self.assertIn('daemon_default', data)
        self.assertIn('long_desc', data)
        self.assertIn('level', data)
        self.assertIn('default', data)
        self.assertIn('see_also', data)
        self.assertIn('tags', data)
        self.assertIn('min', data)
        self.assertIn('max', data)
        self.assertIn('services', data)
        self.assertIn('type', data)
        self.assertIn('desc', data)

        if 'value' in data:
            self.assertIn('source', data)
            self.assertIsInstance(data['value'], list)

            for entry in data['value']:
                self.assertIsInstance(entry, dict)
                self.assertIn('section', entry)
                self.assertIn('value', entry)

    def _wait_for_expected_get_result(self, get_func, expected_result, max_attempts=30,
                                      sleep_time=1):
        attempts = 0
        while attempts < max_attempts:
            get_result = get_func()
            if get_result == expected_result:
                self.assertStatus(200)
                return get_result

            time.sleep(sleep_time)
            attempts += 1
