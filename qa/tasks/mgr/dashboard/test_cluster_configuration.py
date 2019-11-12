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
        config_name = 'mon_allow_pool_delete'

        orig_value = self._get_config_by_name(config_name)

        self._ceph_cmd(['config', 'set', 'mon', config_name, 'true'])
        result = self._wait_for_expected_get_result(self._get_config_by_name, config_name,
                                                    [{'section': 'mon', 'value': 'true'}])
        self.assertEqual(result, [{'section': 'mon', 'value': 'true'}])

        self._ceph_cmd(['config', 'set', 'mon', config_name, 'false'])
        result = self._wait_for_expected_get_result(self._get_config_by_name, config_name,
                                                    [{'section': 'mon', 'value': 'false'}])
        self.assertEqual(result, [{'section': 'mon', 'value': 'false'}])

        # restore value
        if orig_value:
            self._ceph_cmd(['config', 'set', 'mon', config_name, orig_value[0]['value']])

    def test_filter_config_options(self):
        config_names = ['osd_scrub_during_recovery', 'osd_scrub_begin_hour', 'osd_scrub_end_hour']
        data = self._get('/api/cluster_conf/filter?names={}'.format(','.join(config_names)))
        self.assertStatus(200)
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 3)
        for conf in data:
            self._validate_single(conf)
            self.assertIn(conf['name'], config_names)

    def test_filter_config_options_empty_names(self):
        self._get('/api/cluster_conf/filter?names=')
        self.assertStatus(404)
        self.assertEqual(self._resp.json()['detail'], 'Config options `` not found')

    def test_filter_config_options_unknown_name(self):
        self._get('/api/cluster_conf/filter?names=abc')
        self.assertStatus(404)
        self.assertEqual(self._resp.json()['detail'], 'Config options `abc` not found')

    def test_filter_config_options_contains_unknown_name(self):
        config_names = ['osd_scrub_during_recovery', 'osd_scrub_begin_hour', 'abc']
        data = self._get('/api/cluster_conf/filter?names={}'.format(','.join(config_names)))
        self.assertStatus(200)
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 2)
        for conf in data:
            self._validate_single(conf)
            self.assertIn(conf['name'], config_names)

    def test_create(self):
        config_name = 'debug_ms'
        orig_value = self._get_config_by_name(config_name)

        # remove all existing settings for equal preconditions
        self._clear_all_values_for_config_option(config_name)

        expected_result = [{'section': 'mon', 'value': '0/3'}]

        self._post('/api/cluster_conf', {
            'name': config_name,
            'value': expected_result
        })
        self.assertStatus(201)
        result = self._wait_for_expected_get_result(self._get_config_by_name, config_name,
                                                    expected_result)
        self.assertEqual(result, expected_result)

        # reset original value
        self._clear_all_values_for_config_option(config_name)
        self._reset_original_values(config_name, orig_value)

    def test_delete(self):
        config_name = 'debug_ms'
        orig_value = self._get_config_by_name(config_name)

        # set a config option
        expected_result = [{'section': 'mon', 'value': '0/3'}]
        self._post('/api/cluster_conf', {
            'name': config_name,
            'value': expected_result
        })
        self.assertStatus(201)
        self._wait_for_expected_get_result(self._get_config_by_name, config_name, expected_result)

        # delete it and check if it's deleted
        self._delete('/api/cluster_conf/{}?section={}'.format(config_name, 'mon'))
        self.assertStatus(204)
        result = self._wait_for_expected_get_result(self._get_config_by_name, config_name, None)
        self.assertEqual(result, None)

        # reset original value
        self._clear_all_values_for_config_option(config_name)
        self._reset_original_values(config_name, orig_value)

    def test_create_cant_update_at_runtime(self):
        config_name = 'public_bind_addr'  # not updatable
        config_value = [{'section': 'global', 'value': 'true'}]
        orig_value = self._get_config_by_name(config_name)

        # try to set config option and check if it fails
        self._post('/api/cluster_conf', {
            'name': config_name,
            'value': config_value
        })
        self.assertStatus(400)
        self.assertError(code='config_option_not_updatable_at_runtime',
                         component='cluster_configuration',
                         detail='Config option {} is/are not updatable at runtime'.format(
                             config_name))

        # check if config option value is still the original one
        result = self._wait_for_expected_get_result(self._get_config_by_name, config_name,
                                                    orig_value)
        self.assertEqual(result, orig_value)

    def test_create_two_values(self):
        config_name = 'debug_ms'
        orig_value = self._get_config_by_name(config_name)

        # remove all existing settings for equal preconditions
        self._clear_all_values_for_config_option(config_name)

        expected_result = [{'section': 'mon', 'value': '0/3'},
                           {'section': 'osd', 'value': '0/5'}]

        self._post('/api/cluster_conf', {
            'name': config_name,
            'value': expected_result
        })
        self.assertStatus(201)
        result = self._wait_for_expected_get_result(self._get_config_by_name, config_name,
                                                    expected_result)
        self.assertEqual(result, expected_result)

        # reset original value
        self._clear_all_values_for_config_option(config_name)
        self._reset_original_values(config_name, orig_value)

    def test_create_can_handle_none_values(self):
        config_name = 'debug_ms'
        orig_value = self._get_config_by_name(config_name)

        # remove all existing settings for equal preconditions
        self._clear_all_values_for_config_option(config_name)

        self._post('/api/cluster_conf', {
            'name': config_name,
            'value': [{'section': 'mon', 'value': '0/3'},
                      {'section': 'osd', 'value': None}]
        })
        self.assertStatus(201)

        expected_result = [{'section': 'mon', 'value': '0/3'}]
        result = self._wait_for_expected_get_result(self._get_config_by_name, config_name,
                                                    expected_result)
        self.assertEqual(result, expected_result)

        # reset original value
        self._clear_all_values_for_config_option(config_name)
        self._reset_original_values(config_name, orig_value)

    def test_create_can_handle_boolean_values(self):
        config_name = 'mon_allow_pool_delete'
        orig_value = self._get_config_by_name(config_name)

        # remove all existing settings for equal preconditions
        self._clear_all_values_for_config_option(config_name)

        expected_result = [{'section': 'mon', 'value': 'true'}]

        self._post('/api/cluster_conf', {
            'name': config_name,
            'value': [{'section': 'mon', 'value': True}]})
        self.assertStatus(201)

        result = self._wait_for_expected_get_result(self._get_config_by_name, config_name,
                                                    expected_result)
        self.assertEqual(result, expected_result)

        # reset original value
        self._clear_all_values_for_config_option(config_name)
        self._reset_original_values(config_name, orig_value)

    def test_bulk_set(self):
        expected_result = {
            'osd_max_backfills': {'section': 'osd', 'value': '1'},
            'osd_recovery_max_active': {'section': 'osd', 'value': '3'},
            'osd_recovery_max_single_start': {'section': 'osd', 'value': '1'},
            'osd_recovery_sleep': {'section': 'osd', 'value': '2.000000'}
        }
        orig_values = dict()

        for config_name in expected_result:
            orig_values[config_name] = self._get_config_by_name(config_name)

            # remove all existing settings for equal preconditions
            self._clear_all_values_for_config_option(config_name)

        self._put('/api/cluster_conf', {'options': expected_result})
        self.assertStatus(200)

        for config_name, value in expected_result.items():
            result = self._wait_for_expected_get_result(self._get_config_by_name, config_name,
                                                        [value])
            self.assertEqual(result, [value])

            # reset original value
            self._clear_all_values_for_config_option(config_name)
            self._reset_original_values(config_name, orig_values[config_name])

    def test_bulk_set_cant_update_at_runtime(self):
        config_options = {
            'public_bind_addr': {'section': 'global', 'value': '1.2.3.4:567'},  # not updatable
            'public_network': {'section': 'global', 'value': '10.0.0.0/8'}  # not updatable
        }
        orig_values = dict()

        for config_name in config_options:
            orig_values[config_name] = self._get_config_by_name(config_name)

        # try to set config options and see if it fails
        self._put('/api/cluster_conf', {'options': config_options})
        self.assertStatus(400)
        self.assertError(code='config_option_not_updatable_at_runtime',
                         component='cluster_configuration',
                         detail='Config option {} is/are not updatable at runtime'.format(
                             ', '.join(config_options.keys())))

        # check if config option values are still the original ones
        for config_name, value in orig_values.items():
            result = self._wait_for_expected_get_result(self._get_config_by_name, config_name,
                                                        value)
            self.assertEqual(result, value)

    def test_bulk_set_cant_update_at_runtime_partial(self):
        config_options = {
            'public_bind_addr': {'section': 'global', 'value': 'true'},  # not updatable
            'log_to_stderr': {'section': 'global', 'value': 'true'}  # updatable
        }
        orig_values = dict()

        for config_name in config_options:
            orig_values[config_name] = self._get_config_by_name(config_name)

        # try to set config options and see if it fails
        self._put('/api/cluster_conf', {'options': config_options})
        self.assertStatus(400)
        self.assertError(code='config_option_not_updatable_at_runtime',
                         component='cluster_configuration',
                         detail='Config option {} is/are not updatable at runtime'.format(
                             'public_bind_addr'))

        # check if config option values are still the original ones
        for config_name, value in orig_values.items():
            result = self._wait_for_expected_get_result(self._get_config_by_name, config_name,
                                                        value)
            self.assertEqual(result, value)

    def test_check_existence(self):
        """
        This test case is intended to check the existence of all hard coded config options used by
        the dashboard.
        If you include further hard coded options in the dashboard, feel free to add them to the
        list.
        """
        hard_coded_options = [
            'osd_max_backfills',  # osd-recv-speed
            'osd_recovery_max_active',  # osd-recv-speed
            'osd_recovery_max_single_start',  # osd-recv-speed
            'osd_recovery_sleep',  # osd-recv-speed
            'osd_scrub_during_recovery',  # osd-pg-scrub
            'osd_scrub_begin_hour',  # osd-pg-scrub
            'osd_scrub_end_hour',  # osd-pg-scrub
            'osd_scrub_begin_week_day',  # osd-pg-scrub
            'osd_scrub_end_week_day',  # osd-pg-scrub
            'osd_scrub_min_interval',  # osd-pg-scrub
            'osd_scrub_max_interval',  # osd-pg-scrub
            'osd_deep_scrub_interval',  # osd-pg-scrub
            'osd_scrub_auto_repair',  # osd-pg-scrub
            'osd_max_scrubs',  # osd-pg-scrub
            'osd_scrub_priority',  # osd-pg-scrub
            'osd_scrub_sleep',  # osd-pg-scrub
            'osd_scrub_auto_repair_num_errors',  # osd-pg-scrub
            'osd_debug_deep_scrub_sleep',  # osd-pg-scrub
            'osd_deep_scrub_keys',  # osd-pg-scrub
            'osd_deep_scrub_large_omap_object_key_threshold',  # osd-pg-scrub
            'osd_deep_scrub_large_omap_object_value_sum_threshold',  # osd-pg-scrub
            'osd_deep_scrub_randomize_ratio',  # osd-pg-scrub
            'osd_deep_scrub_stride',  # osd-pg-scrub
            'osd_deep_scrub_update_digest_min_age',  # osd-pg-scrub
            'osd_requested_scrub_priority',  # osd-pg-scrub
            'osd_scrub_backoff_ratio',  # osd-pg-scrub
            'osd_scrub_chunk_max',  # osd-pg-scrub
            'osd_scrub_chunk_min',  # osd-pg-scrub
            'osd_scrub_cost',  # osd-pg-scrub
            'osd_scrub_interval_randomize_ratio',  # osd-pg-scrub
            'osd_scrub_invalid_stats',  # osd-pg-scrub
            'osd_scrub_load_threshold',  # osd-pg-scrub
            'osd_scrub_max_preemptions',  # osd-pg-scrub
            'mon_allow_pool_delete'  # pool-list
        ]

        for config_option in hard_coded_options:
            self._get('/api/cluster_conf/{}'.format(config_option))
            self.assertStatus(200)

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
        self.assertIn(data['type'], ['str', 'bool', 'float', 'int', 'size', 'uint', 'addr', 'addrvec', 'uuid',
                                     'secs'])

        if 'value' in data:
            self.assertIn('source', data)
            self.assertIsInstance(data['value'], list)

            for entry in data['value']:
                self.assertIsInstance(entry, dict)
                self.assertIn('section', entry)
                self.assertIn('value', entry)

    def _wait_for_expected_get_result(self, get_func, get_params, expected_result, max_attempts=30,
                                      sleep_time=1):
        attempts = 0
        while attempts < max_attempts:
            get_result = get_func(get_params)
            if get_result == expected_result:
                self.assertStatus(200)
                return get_result

            time.sleep(sleep_time)
            attempts += 1

    def _get_config_by_name(self, conf_name):
        data = self._get('/api/cluster_conf/{}'.format(conf_name))
        if 'value' in data:
            return data['value']
        return None

    def _clear_all_values_for_config_option(self, config_name):
        values = self._get_config_by_name(config_name)
        if values:
            for value in values:
                self._ceph_cmd(['config', 'rm', value['section'], config_name])

    def _reset_original_values(self, config_name, orig_values):
        if orig_values:
            for value in orig_values:
                self._ceph_cmd(['config', 'set', value['section'], config_name, value['value']])
