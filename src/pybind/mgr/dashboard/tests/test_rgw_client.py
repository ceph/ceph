# -*- coding: utf-8 -*-
# pylint: disable=too-many-public-methods
import errno
from unittest import TestCase
from unittest.mock import Mock, patch

from .. import mgr
from ..exceptions import DashboardException
from ..services.rgw_client import NoRgwDaemonsException, RgwClient, \
    _determine_rgw_addr, _parse_frontend_config
from ..services.service import NoCredentialsException
from ..settings import Settings
from ..tests import CLICommandTestMixin, RgwStub


@patch('dashboard.services.rgw_client.RgwClient._get_user_id', Mock(
    return_value='dummy_admin'))
@patch('dashboard.services.ceph_service.CephService.send_command', Mock(
    return_value=''))
class RgwClientTest(TestCase, CLICommandTestMixin):
    _dashboard_user_realm1_access_key = 'VUOFXZFK24H81ISTVBTR'
    _dashboard_user_realm1_secret_key = '0PGsCvXPGWS3AGgibUZEcd9efLrbbshlUkY3jruR'
    _dashboard_user_realm2_access_key = 'OMDR282VYLBC1ZYMYDL0'
    _dashboard_user_realm2_secret_key = 'N3thf7jAiwQ90PsPrhC2DIcvCFOsBXtBvPJJMdC3'
    _radosgw_admin_result_error = (-errno.EINVAL, '', 'fake error')
    _radosgw_admin_result_no_realms = (0, {}, '')
    _radosgw_admin_result_realms = (0, {"realms": ["realm1", "realm2"]}, '')
    _radosgw_admin_result_user_realm1 = (
        0,
        {
            "keys": [
                {
                    "user": "dashboard",
                    "access_key": _dashboard_user_realm1_access_key,
                    "secret_key": _dashboard_user_realm1_secret_key
                }
            ],
            "system": "true"
        },
        '')
    _radosgw_admin_result_user_realm2 = (
        0,
        {
            "keys": [
                {
                    "user": "dashboard",
                    "access_key": _dashboard_user_realm2_access_key,
                    "secret_key": _dashboard_user_realm2_secret_key
                }
            ],
            "system": "true"
        },
        '')

    def setUp(self):
        RgwStub.get_daemons()
        self.mock_kv_store()
        self.CONFIG_KEY_DICT.update({
            'RGW_API_ACCESS_KEY': 'klausmustermann',
            'RGW_API_SECRET_KEY': 'supergeheim',
        })

    def test_configure_credentials_error(self):
        self.CONFIG_KEY_DICT.update({
            'RGW_API_ACCESS_KEY': '',
            'RGW_API_SECRET_KEY': '',
        })
        # Get no realms, get no user, user creation fails.
        mgr.send_rgwadmin_command.side_effect = [
            self._radosgw_admin_result_error,
            self._radosgw_admin_result_error,
            self._radosgw_admin_result_error,
        ]
        with self.assertRaises(NoCredentialsException) as cm:
            RgwClient.admin_instance()
        self.assertIn('No RGW credentials found', str(cm.exception))

    def test_configure_credentials_error_with_realms(self):
        self.CONFIG_KEY_DICT.update({
            'RGW_API_ACCESS_KEY': '',
            'RGW_API_SECRET_KEY': '',
        })
        # Get realms, get no user, user creation fails.
        mgr.send_rgwadmin_command.side_effect = [
            self._radosgw_admin_result_realms,
            self._radosgw_admin_result_error,
            self._radosgw_admin_result_error,
            self._radosgw_admin_result_error,
            self._radosgw_admin_result_error,
        ]
        with self.assertRaises(NoCredentialsException) as cm:
            RgwClient.admin_instance()
        self.assertIn('No RGW credentials found', str(cm.exception))

    def test_set_rgw_credentials_command(self):
        # Get no realms, get user.
        mgr.send_rgwadmin_command.side_effect = [
            self._radosgw_admin_result_error,
            self._radosgw_admin_result_user_realm1
        ]
        result = self.exec_cmd('set-rgw-credentials')
        self.assertEqual(result, 'RGW credentials configured')
        self.assertEqual(Settings.RGW_API_ACCESS_KEY, self._dashboard_user_realm1_access_key)
        self.assertEqual(Settings.RGW_API_SECRET_KEY, self._dashboard_user_realm1_secret_key)

        # Get no realms, get no user, user creation.
        mgr.send_rgwadmin_command.side_effect = [
            self._radosgw_admin_result_error,
            self._radosgw_admin_result_error,
            self._radosgw_admin_result_user_realm1
        ]
        result = self.exec_cmd('set-rgw-credentials')
        self.assertEqual(result, 'RGW credentials configured')
        self.assertEqual(Settings.RGW_API_ACCESS_KEY, self._dashboard_user_realm1_access_key)
        self.assertEqual(Settings.RGW_API_SECRET_KEY, self._dashboard_user_realm1_secret_key)

        # Get realms, get users.
        mgr.send_rgwadmin_command.side_effect = [
            self._radosgw_admin_result_realms,
            self._radosgw_admin_result_user_realm1,
            self._radosgw_admin_result_user_realm2
        ]
        result = self.exec_cmd('set-rgw-credentials')
        self.assertEqual(result, 'RGW credentials configured')
        self.assertEqual(Settings.RGW_API_ACCESS_KEY, {
            'realm1': self._dashboard_user_realm1_access_key,
            'realm2': self._dashboard_user_realm2_access_key
        })
        self.assertEqual(Settings.RGW_API_SECRET_KEY, {
            'realm1': self._dashboard_user_realm1_secret_key,
            'realm2': self._dashboard_user_realm2_secret_key
        })

        # Get realms, get no users, users' creation.
        mgr.send_rgwadmin_command.side_effect = [
            self._radosgw_admin_result_realms,
            self._radosgw_admin_result_error,
            self._radosgw_admin_result_user_realm1,
            self._radosgw_admin_result_error,
            self._radosgw_admin_result_user_realm2
        ]
        result = self.exec_cmd('set-rgw-credentials')
        self.assertEqual(result, 'RGW credentials configured')
        self.assertEqual(Settings.RGW_API_ACCESS_KEY, {
            'realm1': self._dashboard_user_realm1_access_key,
            'realm2': self._dashboard_user_realm2_access_key
        })
        self.assertEqual(Settings.RGW_API_SECRET_KEY, {
            'realm1': self._dashboard_user_realm1_secret_key,
            'realm2': self._dashboard_user_realm2_secret_key
        })

        # Get realms, get no users, realm 2 user creation fails.
        mgr.send_rgwadmin_command.side_effect = [
            self._radosgw_admin_result_realms,
            self._radosgw_admin_result_error,
            self._radosgw_admin_result_user_realm1,
            self._radosgw_admin_result_error,
            self._radosgw_admin_result_error,
        ]
        result = self.exec_cmd('set-rgw-credentials')
        self.assertEqual(result, 'RGW credentials configured')
        self.assertEqual(Settings.RGW_API_ACCESS_KEY, {
            'realm1': self._dashboard_user_realm1_access_key,
        })
        self.assertEqual(Settings.RGW_API_SECRET_KEY, {
            'realm1': self._dashboard_user_realm1_secret_key,
        })

    def test_ssl_verify(self):
        Settings.RGW_API_SSL_VERIFY = True
        instance = RgwClient.admin_instance()
        self.assertTrue(instance.session.verify)

    def test_no_ssl_verify(self):
        Settings.RGW_API_SSL_VERIFY = False
        instance = RgwClient.admin_instance()
        self.assertFalse(instance.session.verify)

    def test_no_daemons(self):
        RgwStub.get_mgr_no_services()
        with self.assertRaises(NoRgwDaemonsException) as cm:
            RgwClient.admin_instance()
        self.assertIn('No RGW service is running.', str(cm.exception))

    @patch.object(RgwClient, '_get_daemon_zone_info')
    def test_get_placement_targets_from_zone(self, zone_info):
        zone_info.return_value = {
            'id': 'a0df30ea-4b5b-4830-b143-2bedf684663d',
            'placement_pools': [
                {
                    'key': 'default-placement',
                    'val': {
                        'index_pool': 'default.rgw.buckets.index',
                        'storage_classes': {
                            'STANDARD': {
                                'data_pool': 'default.rgw.buckets.data'
                            }
                        }
                    }
                }
            ]
        }

        instance = RgwClient.admin_instance()
        expected_result = {
            'zonegroup': 'zonegroup1',
            'placement_targets': [
                {
                    'name': 'default-placement',
                    'data_pool': 'default.rgw.buckets.data'
                }
            ]
        }
        self.assertEqual(expected_result, instance.get_placement_targets())

    @patch.object(RgwClient, '_get_realms_info')
    def test_get_realms(self, realms_info):
        realms_info.side_effect = [
            {
                'default_info': '51de8373-bc24-4f74-a9b7-8e9ef4cb71f7',
                'realms': [
                    'realm1',
                    'realm2'
                ]
            },
            {}
        ]
        instance = RgwClient.admin_instance()

        self.assertEqual(['realm1', 'realm2'], instance.get_realms())
        self.assertEqual([], instance.get_realms())

    def test_set_bucket_locking_error(self):
        instance = RgwClient.admin_instance()
        test_params = [
            ('COMPLIANCE', 'null', None, 'must be a positive integer'),
            ('COMPLIANCE', None, 'null', 'must be a positive integer'),
            ('COMPLIANCE', -1, None, 'must be a positive integer'),
            ('COMPLIANCE', None, -1, 'must be a positive integer'),
            ('COMPLIANCE', 1, 1, 'You can\'t specify both at the same time'),
            ('COMPLIANCE', None, None, 'You must specify at least one'),
            ('COMPLIANCE', 0, 0, 'You must specify at least one'),
            (None, 1, 0, 'must be either COMPLIANCE or GOVERNANCE'),
            ('', 1, 0, 'must be either COMPLIANCE or GOVERNANCE'),
            ('FAKE_MODE', 1, 0, 'must be either COMPLIANCE or GOVERNANCE')
        ]
        for params in test_params:
            mode, days, years, error_msg = params
            with self.assertRaises(DashboardException) as cm:
                instance.set_bucket_locking(
                    bucket_name='test',
                    mode=mode,
                    retention_period_days=days,
                    retention_period_years=years
                )
            self.assertIn(error_msg, str(cm.exception))

    @patch('dashboard.rest_client._Request', Mock())
    def test_set_bucket_locking_success(self):
        instance = RgwClient.admin_instance()
        test_params = [
            ('Compliance', '1', None),
            ('Governance', 1, None),
            ('COMPLIANCE', None, '1'),
            ('GOVERNANCE', None, 1),
        ]
        for params in test_params:
            mode, days, years = params
            self.assertIsNone(instance.set_bucket_locking(
                bucket_name='test',
                mode=mode,
                retention_period_days=days,
                retention_period_years=years
            ))

    def test_set_rgw_hostname(self):
        result = self.exec_cmd(
            'set-rgw-hostname',
            daemon_name='test_daemon',
            hostname='example.hostname.com'
        )
        self.assertEqual(
            result,
            'RGW hostname for daemon test_daemon configured'
        )
        self.assertEqual(
            Settings.RGW_HOSTNAME_PER_DAEMON,
            {'test_daemon': 'example.hostname.com'}
        )

    @patch("dashboard.services.rgw_client.RgwDaemon")
    def test_hostname_when_rgw_hostname_config_is_set(self, mock_daemons):
        mock_instance = Mock()
        mock_daemons.return_value = mock_instance

        self.test_set_rgw_hostname()

        daemon_info = {
            'metadata': {
                'id': 'test_daemon',
                'hostname': 'my-hostname.com',
                'frontend_config#0': 'beast port=8000'
            },
            'addr': '192.0.2.1'
        }

        result = _determine_rgw_addr(daemon_info)
        self.assertEqual(result.host, "example.hostname.com")

    @patch("dashboard.services.rgw_client.RgwDaemon")
    def test_hostname_when_rgw_hostname_config_is_not_set(self, mock_daemons):
        mock_instance = Mock()
        mock_daemons.return_value = mock_instance

        daemon_info = {
            'metadata': {
                'id': 'test_daemon',
                'hostname': 'my.hostname.com',
                'frontend_config#0': 'beast port=8000'
            },
            'addr': '192.168.178.3:49774/1534999298'
        }

        result = _determine_rgw_addr(daemon_info)
        self.assertEqual(result.host, "192.168.178.3")


class RgwClientHelperTest(TestCase):
    def test_parse_frontend_config_1(self):
        self.assertEqual(_parse_frontend_config('beast port=8000'), (8000, False))

    def test_parse_frontend_config_2(self):
        self.assertEqual(_parse_frontend_config('beast port=80 port=8000'), (80, False))

    def test_parse_frontend_config_3(self):
        self.assertEqual(_parse_frontend_config('beast ssl_port=443 port=8000'), (443, True))

    def test_parse_frontend_config_4(self):
        self.assertEqual(_parse_frontend_config('beast endpoint=192.168.0.100:8000'), (8000, False))

    def test_parse_frontend_config_5(self):
        self.assertEqual(_parse_frontend_config('beast endpoint=[::1]'), (80, False))

    def test_parse_frontend_config_6(self):
        self.assertEqual(_parse_frontend_config(
            'beast ssl_endpoint=192.168.0.100:8443'), (8443, True))

    def test_parse_frontend_config_7(self):
        self.assertEqual(_parse_frontend_config('beast ssl_endpoint=192.168.0.100'), (443, True))

    def test_parse_frontend_config_8(self):
        self.assertEqual(_parse_frontend_config(
            'beast ssl_endpoint=[::1]:8443 endpoint=192.0.2.3:80'), (8443, True))

    def test_parse_frontend_config_9(self):
        self.assertEqual(_parse_frontend_config(
            'beast port=8080 endpoint=192.0.2.3:80'), (8080, False))

    def test_parse_frontend_config_10(self):
        self.assertEqual(_parse_frontend_config(
            'beast ssl_endpoint=192.0.2.3:8443 port=8080'), (8443, True))

    def test_parse_frontend_config_11(self):
        self.assertEqual(_parse_frontend_config('civetweb port=8000s'), (8000, True))

    def test_parse_frontend_config_12(self):
        self.assertEqual(_parse_frontend_config('civetweb port=443s port=8000'), (443, True))

    def test_parse_frontend_config_13(self):
        self.assertEqual(_parse_frontend_config('civetweb port=192.0.2.3:80'), (80, False))

    def test_parse_frontend_config_14(self):
        self.assertEqual(_parse_frontend_config('civetweb port=172.5.2.51:8080s'), (8080, True))

    def test_parse_frontend_config_15(self):
        self.assertEqual(_parse_frontend_config('civetweb port=[::]:8080'), (8080, False))

    def test_parse_frontend_config_16(self):
        self.assertEqual(_parse_frontend_config('civetweb port=ip6-localhost:80s'), (80, True))

    def test_parse_frontend_config_17(self):
        self.assertEqual(_parse_frontend_config('civetweb port=[2001:0db8::1234]:80'), (80, False))

    def test_parse_frontend_config_18(self):
        self.assertEqual(_parse_frontend_config('civetweb port=[::1]:8443s'), (8443, True))

    def test_parse_frontend_config_19(self):
        self.assertEqual(_parse_frontend_config('civetweb port=127.0.0.1:8443s+8000'), (8443, True))

    def test_parse_frontend_config_20(self):
        self.assertEqual(_parse_frontend_config('civetweb port=127.0.0.1:8080+443s'), (8080, False))

    def test_parse_frontend_config_21(self):
        with self.assertRaises(LookupError) as ctx:
            _parse_frontend_config('civetweb port=xyz')
        self.assertEqual(str(ctx.exception),
                         'Failed to determine RGW port from "civetweb port=xyz"')

    def test_parse_frontend_config_22(self):
        with self.assertRaises(LookupError) as ctx:
            _parse_frontend_config('civetweb')
        self.assertEqual(str(ctx.exception), 'Failed to determine RGW port from "civetweb"')

    def test_parse_frontend_config_23(self):
        with self.assertRaises(LookupError) as ctx:
            _parse_frontend_config('mongoose port=8080')
        self.assertEqual(str(ctx.exception),
                         'Failed to determine RGW port from "mongoose port=8080"')


class TestDictToXML(TestCase):
    def test_empty_dict(self):
        result = RgwClient.dict_to_xml({})
        self.assertEqual(result, '')

    def test_empty_string(self):
        result = RgwClient.dict_to_xml("")
        self.assertEqual(result, '')

    def test_invalid_json_string(self):
        with self.assertRaises(DashboardException):
            RgwClient.dict_to_xml("invalid json")

    def test_simple_dict(self):
        data = {"name": "Foo", "age": 30}
        expected_xml = "<name>Foo</name>\n<age>30</age>\n"
        result = RgwClient.dict_to_xml(data)
        self.assertEqual(result, expected_xml)

    def test_nested_dict(self):
        data = {"person": {"name": "Foo", "age": 30}}
        expected_xml = "<person>\n<name>Foo</name>\n<age>30</age>\n</person>\n"
        result = RgwClient.dict_to_xml(data)
        self.assertEqual(result, expected_xml)

    def test_list_in_dict(self):
        data = {"names": ["Foo", "Boo"]}
        expected_xml = "<names>\nFoo</names>\n<names>\nBoo</names>\n"
        result = RgwClient.dict_to_xml(data)
        self.assertEqual(result, expected_xml)

    def test_rules_list_in_dict(self):
        data = {"Rules": [{"id": 1}, {"id": 2}]}
        expected_xml = "<Rule>\n<id>1</id>\n</Rule>\n<Rule>\n<id>2</id>\n</Rule>\n"
        result = RgwClient.dict_to_xml(data)
        self.assertEqual(result, expected_xml)

    def test_json_string(self):
        data = '{"name": "Foo", "age": 30}'
        expected_xml = "<name>Foo</name>\n<age>30</age>\n"
        result = RgwClient.dict_to_xml(data)
        self.assertEqual(result, expected_xml)
