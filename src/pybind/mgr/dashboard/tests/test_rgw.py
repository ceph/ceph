
from unittest.mock import Mock, call, patch

from .. import mgr
from ..controllers.rgw import Rgw, RgwDaemon, RgwTopic, RgwUser
from ..rest_client import RequestException
from ..services.rgw_client import RgwClient, RgwMultisite
from ..tests import ControllerTestCase, RgwStub


class RgwControllerTestCase(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        cls.setup_controllers([Rgw], '/test')

    def setUp(self) -> None:
        RgwStub.get_daemons()
        RgwStub.get_settings()

    @patch.object(RgwClient, '_get_user_id', Mock(return_value='fake-user'))
    @patch.object(RgwClient, 'is_service_online', Mock(return_value=True))
    @patch.object(RgwClient, '_is_system_user', Mock(return_value=True))
    @patch('dashboard.services.ceph_service.CephService.send_command')
    def test_status_available(self, send_command):
        send_command.return_value = ''
        self._get('/test/ui-api/rgw/status')
        self.assertStatus(200)
        self.assertJsonBody({'available': True, 'message': None})

    @patch.object(RgwClient, '_get_user_id', Mock(return_value='fake-user'))
    @patch.object(RgwClient, 'is_service_online', Mock(
        side_effect=RequestException('My test error')))
    @patch('dashboard.services.ceph_service.CephService.send_command')
    def test_status_online_check_error(self, send_command):
        send_command.return_value = ''
        self._get('/test/ui-api/rgw/status')
        self.assertStatus(200)
        self.assertJsonBody({'available': False,
                             'message': 'My test error'})

    @patch.object(RgwClient, '_get_user_id', Mock(return_value='fake-user'))
    @patch.object(RgwClient, 'is_service_online', Mock(return_value=False))
    @patch('dashboard.services.ceph_service.CephService.send_command')
    def test_status_not_online(self, send_command):
        send_command.return_value = ''
        self._get('/test/ui-api/rgw/status')
        self.assertStatus(200)
        self.assertJsonBody({'available': False,
                             'message': "Failed to connect to the Object Gateway's Admin Ops API."})

    @patch.object(RgwClient, '_get_user_id', Mock(return_value='fake-user'))
    @patch.object(RgwClient, 'is_service_online', Mock(return_value=True))
    @patch.object(RgwClient, '_is_system_user', Mock(return_value=False))
    @patch('dashboard.services.ceph_service.CephService.send_command')
    def test_status_not_system_user(self, send_command):
        send_command.return_value = ''
        self._get('/test/ui-api/rgw/status')
        self.assertStatus(200)
        self.assertJsonBody({'available': False,
                             'message': 'The system flag is not set for user "fake-user".'})

    def test_status_no_service(self):
        RgwStub.get_mgr_no_services()
        self._get('/test/ui-api/rgw/status')
        self.assertStatus(200)
        self.assertJsonBody({'available': False, 'message': 'No RGW service is running.'})


class RgwDaemonControllerTestCase(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        cls.setup_controllers([RgwDaemon], '/test')

    @patch('dashboard.services.rgw_client.RgwClient._get_user_id', Mock(
        return_value='dummy_admin'))
    @patch('dashboard.services.ceph_service.CephService.send_command')
    def test_list(self, send_command):
        send_command.return_value = ''
        RgwStub.get_daemons()
        RgwStub.get_settings()
        mgr.list_servers.return_value = [{
            'hostname': 'host1',
            'services': [
                {'id': '4832', 'type': 'rgw'},
                {'id': '5356', 'type': 'rgw'},
                {'id': '5357', 'type': 'rgw'},
                {'id': '5358', 'type': 'rgw'},
                {'id': '5359', 'type': 'rgw'}
            ]
        }]
        mgr.get_metadata.side_effect = [
            {
                'ceph_version': 'ceph version master (dev)',
                'id': 'daemon1',
                'realm_name': 'realm1',
                'zonegroup_name': 'zg1',
                'zonegroup_id': 'zg1-id',
                'zone_name': 'zone1',
                'frontend_config#0': 'beast port=80'
            },
            {
                'ceph_version': 'ceph version master (dev)',
                'id': 'daemon2',
                'realm_name': 'realm2',
                'zonegroup_name': 'zg2',
                'zonegroup_id': 'zg2-id',
                'zone_name': 'zone2',
                'frontend_config#0': 'beast ssl_port=443 ssl_certificate=config:/config'
            },
            {
                'ceph_version': 'ceph version master (dev)',
                'id': 'daemon3',
                'realm_name': 'realm3',
                'zonegroup_name': 'zg3',
                'zonegroup_id': 'zg3-id',
                'zone_name': 'zone3',
                'frontend_config#0':
                    'beast ssl_endpoint=0.0.0.0:8080 ssl_certificate=config:/config'
            },
            {
                'ceph_version': 'ceph version master (dev)',
                'id': 'daemon4',
                'realm_name': 'realm4',
                'zonegroup_name': 'zg4',
                'zonegroup_id': 'zg4-id',
                'zone_name': 'zone4',
                'frontend_config#0': 'beast ssl_certificate=config:/config'
            },
            {
                'ceph_version': 'ceph version master (dev)',
                'id': 'daemon5',
                'realm_name': 'realm5',
                'zonegroup_name': 'zg5',
                'zonegroup_id': 'zg5-id',
                'zone_name': 'zone5',
                'frontend_config#0':
                    'beast endpoint=0.0.0.0:8445 ssl_certificate=config:/config'
            }, ]
        self._get('/test/api/rgw/daemon')
        self.assertStatus(200)
        self.assertJsonBody([{
            'id': 'daemon1',
            'service_map_id': '4832',
            'version': 'ceph version master (dev)',
            'server_hostname': 'host1',
            'realm_name': 'realm1',
            'zonegroup_name': 'zg1',
            'zonegroup_id': 'zg1-id',
            'zone_name': 'zone1', 'default': True,
            'port': 80
        },
            {
            'id': 'daemon2',
            'service_map_id': '5356',
            'version': 'ceph version master (dev)',
            'server_hostname': 'host1',
            'realm_name': 'realm2',
            'zonegroup_name': 'zg2',
            'zonegroup_id': 'zg2-id',
            'zone_name': 'zone2',
            'default': False,
            'port': 443,
        },
            {
            'id': 'daemon3',
            'service_map_id': '5357',
            'version': 'ceph version master (dev)',
            'server_hostname': 'host1',
            'realm_name': 'realm3',
            'zonegroup_name': 'zg3',
            'zonegroup_id': 'zg3-id',
            'zone_name': 'zone3',
            'default': False,
            'port': 8080,
        },
            {
            'id': 'daemon4',
            'service_map_id': '5358',
            'version': 'ceph version master (dev)',
            'server_hostname': 'host1',
            'realm_name': 'realm4',
            'zonegroup_name': 'zg4',
            'zonegroup_id': 'zg4-id',
            'zone_name': 'zone4',
            'default': False,
            'port': None,
        },
            {
            'id': 'daemon5',
            'service_map_id': '5359',
            'version': 'ceph version master (dev)',
            'server_hostname': 'host1',
            'realm_name': 'realm5',
            'zonegroup_name': 'zg5',
            'zonegroup_id': 'zg5-id',
            'zone_name': 'zone5',
            'default': False,
            'port': 8445,
        }])

    def test_list_empty(self):
        RgwStub.get_mgr_no_services()
        self._get('/test/api/rgw/daemon')
        self.assertStatus(200)
        self.assertJsonBody([])

    @patch('dashboard.services.rgw_client.RgwClient._get_user_id', Mock(
        return_value='dummy_admin'))
    @patch('dashboard.services.ceph_service.CephService.send_command')
    @patch.object(RgwMultisite, 'get_all_zonegroups_info', Mock(
        return_value={'default_zonegroup': 'zonegroup2-id'}))
    def test_default_zonegroup_when_multiple_daemons(self, send_command):
        send_command.return_value = ''
        RgwStub.get_daemons()
        RgwStub.get_settings()
        metadata_return_values = [
            {
                'ceph_version': 'ceph version master (dev)',
                'id': 'daemon1',
                'realm_name': 'realm1',
                'zonegroup_name': 'zg1',
                'zonegroup_id': 'zg1-id',
                'zone_name': 'zone1',
                'frontend_config#0': 'beast port=80'
            },
            {
                'ceph_version': 'ceph version master (dev)',
                'id': 'daemon2',
                'realm_name': 'realm2',
                'zonegroup_name': 'zg2',
                'zonegroup_id': 'zg2-id',
                'zone_name': 'zone2',
                'frontend_config#0': 'beast ssl_port=443'
            }
        ]
        list_servers_return_value = [{
            'hostname': 'host1',
            'services': [
                {'id': '5297', 'type': 'rgw'},
                {'id': '5356', 'type': 'rgw'},
            ]
        }]

        mgr.list_servers.return_value = list_servers_return_value
        mgr.get_metadata.side_effect = metadata_return_values
        self._get('/test/api/rgw/daemon')
        self.assertStatus(200)

        self.assertJsonBody([{
            'id': 'daemon1',
            'service_map_id': '5297',
            'version': 'ceph version master (dev)',
            'server_hostname': 'host1',
            'realm_name': 'realm1',
            'zonegroup_name': 'zg1',
            'zonegroup_id': 'zg1-id',
            'zone_name': 'zone1',
            'default': False,
            'port': 80
        },
            {
            'id': 'daemon2',
            'service_map_id': '5356',
            'version': 'ceph version master (dev)',
            'server_hostname': 'host1',
            'realm_name': 'realm2',
            'zonegroup_name': 'zg2',
            'zonegroup_id': 'zg2-id',
            'zone_name': 'zone2',
            'default': True,
            'port': 443,
        }])

        # Change the default zonegroup and test if the correct daemon gets picked up
        RgwMultisite().get_all_zonegroups_info.return_value = {'default_zonegroup': 'zonegroup1-id'}
        mgr.list_servers.return_value = list_servers_return_value
        mgr.get_metadata.side_effect = metadata_return_values
        self._get('/test/api/rgw/daemon')
        self.assertStatus(200)

        self.assertJsonBody([{
            'id': 'daemon1',
            'service_map_id': '5297',
            'version': 'ceph version master (dev)',
            'server_hostname': 'host1',
            'realm_name': 'realm1',
            'zonegroup_name': 'zg1',
            'zonegroup_id': 'zg1-id',
            'zone_name': 'zone1',
            'default': True,
            'port': 80
        },
            {
            'id': 'daemon2',
            'service_map_id': '5356',
            'version': 'ceph version master (dev)',
            'server_hostname': 'host1',
            'realm_name': 'realm2',
            'zonegroup_name': 'zg2',
            'zonegroup_id': 'zg2-id',
            'zone_name': 'zone2',
            'default': False,
            'port': 443,
        }])


class RgwUserControllerTestCase(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        cls.setup_controllers([RgwUser], '/test')

    @patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    def test_user_list(self, mock_proxy):
        mock_proxy.side_effect = [{
            'count': 3,
            'keys': ['test1', 'test2', 'test3'],
            'truncated': False
        }]
        self._get('/test/api/rgw/user?daemon_name=dummy-daemon')
        self.assertStatus(200)
        mock_proxy.assert_has_calls([
            call('dummy-daemon', 'GET', 'user?list', {})
        ])
        self.assertJsonBody(['test1', 'test2', 'test3'])

    @patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    def test_user_list_marker(self, mock_proxy):
        mock_proxy.side_effect = [{
            'count': 3,
            'keys': ['test1', 'test2', 'test3'],
            'marker': 'foo:bar',
            'truncated': True
        }, {
            'count': 1,
            'keys': ['admin'],
            'truncated': False
        }]
        self._get('/test/api/rgw/user')
        self.assertStatus(200)
        mock_proxy.assert_has_calls([
            call(None, 'GET', 'user?list', {}),
            call(None, 'GET', 'user?list', {'marker': 'foo:bar'})
        ])
        self.assertJsonBody(['test1', 'test2', 'test3', 'admin'])

    @patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    @patch('dashboard.services.ceph_service.CephService.send_command')
    def test_user_list_duplicate_marker(self, mock_proxy, send_command):
        send_command.return_value = ''
        mock_proxy.side_effect = [{
            'count': 3,
            'keys': ['test1', 'test2', 'test3'],
            'marker': 'foo:bar',
            'truncated': True
        }, {
            'count': 3,
            'keys': ['test4', 'test5', 'test6'],
            'marker': 'foo:bar',
            'truncated': True
        }, {
            'count': 1,
            'keys': ['admin'],
            'truncated': False
        }]
        self._get('/test/api/rgw/user')
        self.assertStatus(500)

    @patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    def test_user_list_invalid_marker(self, mock_proxy):
        mock_proxy.side_effect = [{
            'count': 3,
            'keys': ['test1', 'test2', 'test3'],
            'marker': 'foo:bar',
            'truncated': True
        }, {
            'count': 3,
            'keys': ['test4', 'test5', 'test6'],
            'marker': '',
            'truncated': True
        }, {
            'count': 1,
            'keys': ['admin'],
            'truncated': False
        }]
        self._get('/test/api/rgw/user')
        self.assertStatus(500)

    @patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    @patch.object(RgwUser, '_keys_allowed')
    def test_user_get_with_keys(self, keys_allowed, mock_proxy):
        keys_allowed.return_value = True
        mock_proxy.return_value = {
            'tenant': '',
            'user_id': 'my_user_id',
            'full_user_id': 'my_user_id',
            'keys': [],
            'swift_keys': []
        }
        self._get('/test/api/rgw/user/testuser')
        self.assertStatus(200)
        self.assertInJsonBody('keys')
        self.assertInJsonBody('swift_keys')

    @patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    @patch.object(RgwUser, '_keys_allowed')
    def test_user_get_without_keys(self, keys_allowed, mock_proxy):
        keys_allowed.return_value = False
        mock_proxy.return_value = {
            'tenant': '',
            'user_id': 'my_user_id',
            'full_user_id': 'my_user_id',
            'keys': [],
            'swift_keys': []
        }
        self._get('/test/api/rgw/user/testuser')
        self.assertStatus(200)
        self.assertNotIn('keys', self.json_body())
        self.assertNotIn('swift_keys', self.json_body())

    @patch('dashboard.services.rgw_client.mgr.send_rgwadmin_command')
    @patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    def test_get_rate_limit(self, mock_proxy, send_rgwadmin_command):
        mock_proxy.side_effect = [{
            'count': 3,
            'keys': ['test1', 'test2', 'test3'],
            'truncated': False
        }]
        send_rgwadmin_command.return_value = (
            0,
            {
                "user_ratelimit": {
                    "max_read_ops": 100,
                    "max_write_ops": 50
                }
            },
            ""   # empty error msg
        )

        self._get('/test/api/rgw/user/testuser/ratelimit')
        self.assertStatus(200)
        self.assertInJsonBody('user_ratelimit')

    @patch('dashboard.services.rgw_client.mgr.send_rgwadmin_command')
    @patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    def test_get_global_rate_limit(self, mock_proxy, send_rgwadmin_command):
        mock_proxy.side_effect = [{
            'count': 3,
            'keys': ['test1', 'test2', 'test3'],
            'truncated': False
        }]
        mock_return_value = {
            "bucket_ratelimit": {
                "max_read_ops": 2024,
                "max_write_ops": 0,
                "max_read_bytes": 0,
                "max_write_bytes": 0,
                "enabled": True
            },
            "user_ratelimit": {
                "max_read_ops": 1024,
                "max_write_ops": 0,
                "max_read_bytes": 0,
                "max_write_bytes": 0,
                "enabled": True
            },
            "anonymous_ratelimit": {
                "max_read_ops": 0,
                "max_write_ops": 0,
                "max_read_bytes": 0,
                "max_write_bytes": 0,
                "enabled": True
            }
        }
        send_rgwadmin_command.return_value = (
            0,
            mock_return_value,
            ""   # empty error msg
        )

        self._get('/test/api/rgw/user/testuser/ratelimit')
        self.assertStatus(200)
        self.assertJsonBody(mock_return_value)


class TestRgwTopicController(ControllerTestCase):

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([RgwTopic], '/test')

    @patch('dashboard.services.rgw_client._get_daemons')
    @patch('dashboard.services.rgw_client.RgwClient', autospec=True)
    def test_create_topic(self, mock_rgw_client, mock_get_daemons):
        """
        Test creating a topic with mock return values.
        """
        mock_daemon = {
            "name": "dummy_daemon",
            "host": "127.0.0.1",
            "port": 8000,
            "ssl": False,
            "realm_name": "dummy_realm",
            "zonegroup_name": "dummy_zonegroup",
            "zonegroup_id": "dummy_zonegroup_id",
            "zone_name": "dummy_zone"
        }
        mock_daemon_dict = {'dummy_daemon': mock_daemon}

        mock_get_daemons.return_value = mock_daemon_dict
        mock_rgw_client_instance = mock_rgw_client.return_value
        mock_rgw_client_instance._daemons = mock_daemon_dict  # pylint: disable=W0212

        mock_response = {
            "CreateTopicResult": {
                "TopicArn": "arn:aws:sns:zg1-realm1::HttpTest"
            },
            "ResponseMetadata": {
                "RequestId": "b13925ff-a04a-4ff5-9578-7c51aa7932df.4926.3389207753149441947"
            }
        }
        with patch('dashboard.controllers.rgw.RgwTopic.create', return_value=mock_response):
            self._post('/test/api/rgw/topic', {
                'name': 'HttpTest',
                'owner': 'dashboard',
                'opaque_data': 'testopaque',
                'persistent': True,
                'time_to_live': '10',
                'max_retries': '3',
                'retry_sleep_duration': '5',
                'policy': {}
            })
            self.assertStatus(200)

    @patch('dashboard.controllers.rgw.RgwTopic.list')
    def test_list_topic_with_details(self, mock_list_topics):
        mock_return_value = [
            {
                "owner": "dashboard",
                "name": "HttpTest",
                "dest": {
                    "push_endpoint": "https://10.0.66.13:443",
                    "push_endpoint_args": "verify_ssl=true",
                    "push_endpoint_topic": "HttpTest",
                    "stored_secret": False,
                    "persistent": True,
                    "persistent_queue": ":HttpTest",
                    "time_to_live": "5",
                    "max_retries": "2",
                    "retry_sleep_duration": "2"
                },
                "arn": "arn:aws:sns:zg1-realm1::HttpTest",
                "opaqueData": "test123",
                "policy": "{}",
                "subscribed_buckets": []
            }
        ]
        mock_list_topics.return_value = mock_return_value
        controller = RgwTopic()
        result = controller.list(True, None)
        mock_list_topics.assert_called_with(True, None)
        self.assertEqual(result, mock_return_value)

    @patch('dashboard.controllers.rgw.RgwTopic.get')
    def test_get_topic(self, mock_get_topic):
        mock_return_value = [
            {
                "owner": "dashboard",
                "name": "HttpTest",
                "dest": {
                    "push_endpoint": "https://10.0.66.13:443",
                    "push_endpoint_args": "verify_ssl=true",
                    "push_endpoint_topic": "HttpTest",
                    "stored_secret": False,
                    "persistent": True,
                    "persistent_queue": ":HttpTest",
                    "time_to_live": "5",
                    "max_retries": "2",
                    "retry_sleep_duration": "2"
                },
                "arn": "arn:aws:sns:zg1-realm1::HttpTest",
                "opaqueData": "test123",
                "policy": "{}",
                "subscribed_buckets": []
            }
        ]
        mock_get_topic.return_value = mock_return_value

        controller = RgwTopic()
        result = controller.get('HttpTest', None)
        mock_get_topic.assert_called_with('HttpTest', None)
        self.assertEqual(result, mock_return_value)

    @patch('dashboard.controllers.rgw.RgwTopic.delete')
    def test_delete_topic(self, mock_delete_topic):
        mock_delete_topic.return_value = None

        controller = RgwTopic()
        result = controller.delete(name='HttpTest', tenant=None)
        mock_delete_topic.assert_called_with(name='HttpTest', tenant=None)
        self.assertEqual(result, None)
