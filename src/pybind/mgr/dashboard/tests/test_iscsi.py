# pylint: disable=too-many-public-methods, too-many-lines

import copy
import errno
import json
import unittest

try:
    import mock
except ImportError:
    import unittest.mock as mock

from mgr_module import ERROR_MSG_NO_INPUT_FILE

from .. import mgr
from ..controllers.iscsi import Iscsi, IscsiTarget, IscsiUi
from ..exceptions import DashboardException
from ..rest_client import RequestException
from ..services.exception import handle_request_error
from ..services.iscsi_client import IscsiClient
from ..services.orchestrator import OrchClient
from ..tests import CLICommandTestMixin, CmdException, ControllerTestCase, KVStoreMockMixin
from ..tools import NotificationQueue, TaskManager


class IscsiTestCli(unittest.TestCase, CLICommandTestMixin):

    def setUp(self):
        self.mock_kv_store()
        # pylint: disable=protected-access
        IscsiClientMock._instance = IscsiClientMock()
        IscsiClient.instance = IscsiClientMock.instance

    def test_cli_add_gateway_invalid_url(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('iscsi-gateway-add', name='node1',
                          inbuf='http:/hello.com')

        self.assertEqual(ctx.exception.retcode, -errno.EINVAL)
        self.assertEqual(str(ctx.exception),
                         "Invalid service URL 'http:/hello.com'. Valid format: "
                         "'<scheme>://<username>:<password>@<host>[:port]'.")

    def test_cli_add_gateway_empty_url(self):
        with self.assertRaises(CmdException) as ctx:
            self.exec_cmd('iscsi-gateway-add', name='node1',
                          inbuf='')

        self.assertEqual(ctx.exception.retcode, -errno.EINVAL)
        self.assertIn(ERROR_MSG_NO_INPUT_FILE, str(ctx.exception))

    def test_cli_add_gateway(self):
        self.exec_cmd('iscsi-gateway-add', name='node1',
                      inbuf='https://admin:admin@10.17.5.1:5001')
        self.exec_cmd('iscsi-gateway-add', name='node2',
                      inbuf='https://admin:admin@10.17.5.2:5001')
        iscsi_config = json.loads(self.get_key("_iscsi_config"))
        self.assertEqual(iscsi_config['gateways'], {
            'node1': {
                'service_url': 'https://admin:admin@10.17.5.1:5001'
            },
            'node2': {
                'service_url': 'https://admin:admin@10.17.5.2:5001'
            }
        })

    def test_cli_remove_gateway(self):
        self.test_cli_add_gateway()
        self.exec_cmd('iscsi-gateway-rm', name='node1')
        iscsi_config = json.loads(self.get_key("_iscsi_config"))
        self.assertEqual(iscsi_config['gateways'], {
            'node2': {
                'service_url': 'https://admin:admin@10.17.5.2:5001'
            }
        })


class IscsiTestController(ControllerTestCase, KVStoreMockMixin):

    @classmethod
    def setup_server(cls):
        NotificationQueue.start_queue()
        TaskManager.init()
        OrchClient.instance().available = lambda: False
        mgr.rados.side_effect = None
        cls.setup_controllers([Iscsi, IscsiTarget])

    @classmethod
    def tearDownClass(cls):
        NotificationQueue.stop()

    def setUp(self):
        self.mock_kv_store()
        self.CONFIG_KEY_DICT['_iscsi_config'] = '''
            {
                "gateways": {
                    "node1": {
                        "service_url": "https://admin:admin@10.17.5.1:5001"
                    },
                    "node2": {
                        "service_url": "https://admin:admin@10.17.5.2:5001"
                    }
                }
            }
        '''
        # pylint: disable=protected-access
        IscsiClientMock._instance = IscsiClientMock()
        IscsiClient.instance = IscsiClientMock.instance

    def test_enable_discoveryauth(self):
        discoveryauth = {
            'user': 'myiscsiusername',
            'password': 'myiscsipassword',
            'mutual_user': 'myiscsiusername2',
            'mutual_password': 'myiscsipassword2'
        }
        self._put('/api/iscsi/discoveryauth', discoveryauth)
        self.assertStatus(200)
        self.assertJsonBody(discoveryauth)
        self._get('/api/iscsi/discoveryauth')
        self.assertStatus(200)
        self.assertJsonBody(discoveryauth)

    def test_bad_discoveryauth(self):
        discoveryauth = {
            'user': 'myiscsiusername',
            'password': 'myiscsipasswordmyiscsipasswordmyiscsipassword',
            'mutual_user': '',
            'mutual_password': ''
        }
        put_response = {
            'detail': 'Bad authentication',
            'code': 'target_bad_auth',
            'component': 'iscsi'
        }
        get_response = {
            'user': '',
            'password': '',
            'mutual_user': '',
            'mutual_password': ''
        }
        self._put('/api/iscsi/discoveryauth', discoveryauth)
        self.assertStatus(400)
        self.assertJsonBody(put_response)
        self._get('/api/iscsi/discoveryauth')
        self.assertStatus(200)
        self.assertJsonBody(get_response)

    def test_disable_discoveryauth(self):
        discoveryauth = {
            'user': '',
            'password': '',
            'mutual_user': '',
            'mutual_password': ''
        }
        self._put('/api/iscsi/discoveryauth', discoveryauth)
        self.assertStatus(200)
        self.assertJsonBody(discoveryauth)
        self._get('/api/iscsi/discoveryauth')
        self.assertStatus(200)
        self.assertJsonBody(discoveryauth)

    def test_list_empty(self):
        self._get('/api/iscsi/target')
        self.assertStatus(200)
        self.assertJsonBody([])

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_list(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw1"
        request = copy.deepcopy(iscsi_target_request)
        request['target_iqn'] = target_iqn
        self._task_post('/api/iscsi/target', request)
        self.assertStatus(201)
        self._get('/api/iscsi/target')
        self.assertStatus(200)
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        self.assertJsonBody([response])

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_create(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw2"
        request = copy.deepcopy(iscsi_target_request)
        request['target_iqn'] = target_iqn
        self._task_post('/api/iscsi/target', request)
        self.assertStatus(201)
        self._get('/api/iscsi/target/{}'.format(request['target_iqn']))
        self.assertStatus(200)
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        self.assertJsonBody(response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_create_acl_enabled(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw2"
        request = copy.deepcopy(iscsi_target_request)
        request['target_iqn'] = target_iqn
        request['acl_enabled'] = False
        self._task_post('/api/iscsi/target', request)
        self.assertStatus(201)
        self._get('/api/iscsi/target/{}'.format(request['target_iqn']))
        self.assertStatus(200)
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['acl_enabled'] = False
        self.assertJsonBody(response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._create')
    def test_create_error(self, _create_mock):
        # pylint: disable=protected-access
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw2"
        request = copy.deepcopy(iscsi_target_request)
        request['target_iqn'] = target_iqn
        request['config'] = ""
        request['settings'] = ""
        request['task_progress_begin'] = 0
        request['task_progress_end'] = 100
        _create_mock.side_effect = RequestException("message error")
        with self.assertRaises(DashboardException):
            with handle_request_error('iscsi'):
                IscsiTarget._create(**request)

    def test_validate_error_iqn(self):
        # pylint: disable=protected-access
        with self.assertRaises(DashboardException) as ctx:
            IscsiTarget._validate(None, None, None, None, None, None)
        self.assertEqual(ctx.exception.__str__(),
                         "Target IQN is required")

    def test_validate_error_portals(self):
        # pylint: disable=protected-access
        target_iqn = iscsi_target_request['target_iqn']
        target_controls = iscsi_target_request['target_controls']
        portals = {}
        disks = iscsi_target_request['disks']
        groups = iscsi_target_request['groups']
        settings = {'config': {'minimum_gateways': 1}}
        with self.assertRaises(DashboardException) as ctx:
            IscsiTarget._validate(target_iqn, target_controls, portals, disks, groups, settings)
        self.assertEqual(ctx.exception.__str__(),
                         "At least one portal is required")
        settings = {'config': {'minimum_gateways': 2}}
        with self.assertRaises(DashboardException) as ctx:
            IscsiTarget._validate(target_iqn, target_controls, portals, disks, groups, settings)
        self.assertEqual(ctx.exception.__str__(),
                         "At least 2 portals are required")

    def test_validate_error_target_control(self):
        # pylint: disable=protected-access
        target_iqn = iscsi_target_request['target_iqn']
        target_controls = {
            'target_name': 0
        }
        portals = iscsi_target_request['portals']
        disks = iscsi_target_request['disks']
        groups = iscsi_target_request['groups']
        settings = {
            'config': {'minimum_gateways': 1},
            'target_controls_limits': {
                'target_name': {
                    'min': 1,
                    'max': 2,
                }
            }
        }
        with self.assertRaises(DashboardException) as ctx:
            IscsiTarget._validate(target_iqn, target_controls, portals, disks, groups, settings)
        self.assertEqual(ctx.exception.__str__(),
                         "Target control target_name must be >= 1")
        target_controls = {
            'target_name': 3
        }
        with self.assertRaises(DashboardException) as ctx:
            IscsiTarget._validate(target_iqn, target_controls, portals, disks, groups, settings)
        self.assertEqual(ctx.exception.__str__(),
                         "Target control target_name must be <= 2")

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_validate_error_disk_control(self, _validate_image_mock):
        # pylint: disable=protected-access
        target_iqn = iscsi_target_request['target_iqn']
        target_controls = {}
        portals = iscsi_target_request['portals']
        disks = iscsi_target_request['disks']
        groups = iscsi_target_request['groups']
        settings = {
            'config': {'minimum_gateways': 1},
            'required_rbd_features': {
                'user:rbd': 0
            },
            'unsupported_rbd_features': {
                'user:rbd': 0
            },
            'disk_controls_limits': {
                'user:rbd': {'max_data_area_mb': {
                    'min': 129,
                    'max': 127,
                }}
            }
        }
        with self.assertRaises(DashboardException) as ctx:
            IscsiTarget._validate(target_iqn, target_controls, portals, disks, groups, settings)
        self.assertEqual(ctx.exception.__str__(),
                         "Disk control max_data_area_mb must be >= 129")
        settings['disk_controls_limits']['user:rbd']['max_data_area_mb']['min'] = 1
        with self.assertRaises(DashboardException) as ctx:
            IscsiTarget._validate(target_iqn, target_controls, portals, disks, groups, settings)
        self.assertEqual(ctx.exception.__str__(),
                         "Disk control max_data_area_mb must be <= 127")

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_delete(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw3"
        request = copy.deepcopy(iscsi_target_request)
        request['target_iqn'] = target_iqn
        self._task_post('/api/iscsi/target', request)
        self.assertStatus(201)
        self._task_delete('/api/iscsi/target/{}'.format(request['target_iqn']))
        self.assertStatus(204)
        self._get('/api/iscsi/target')
        self.assertStatus(200)
        self.assertJsonBody([])

    @mock.patch('dashboard.tools.TaskManager.current_task')
    def test_delete_raises_exception(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw3"
        request = copy.deepcopy(iscsi_target_request)
        request['target_iqn'] = target_iqn
        configs = {'targets': {target_iqn: {'portals': {}}}}
        with self.assertRaises(DashboardException):
            # pylint: disable=protected-access
            IscsiTarget._delete(target_iqn, configs, 0, 100)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_add_client(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw4"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['clients'].append(
            {
                "luns": [{"image": "lun1", "pool": "rbd"}],
                "client_iqn": "iqn.1994-05.com.redhat:rh7-client3",
                "auth": {
                    "password": "myiscsipassword5",
                    "user": "myiscsiusername5",
                    "mutual_password": "myiscsipassword6",
                    "mutual_user": "myiscsiusername6"}
            })
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['clients'].append(
            {
                "luns": [{"image": "lun1", "pool": "rbd"}],
                "client_iqn": "iqn.1994-05.com.redhat:rh7-client3",
                "auth": {
                    "password": "myiscsipassword5",
                    "user": "myiscsiusername5",
                    "mutual_password": "myiscsipassword6",
                    "mutual_user": "myiscsiusername6"},
                "info": {
                    "alias": "",
                    "ip_address": [],
                    "state": {}
                }
            })
        self._update_iscsi_target(create_request, update_request, 200, None, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_add_bad_client(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw4"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['clients'].append(
            {
                "luns": [{"image": "lun1", "pool": "rbd"}],
                "client_iqn": "iqn.1994-05.com.redhat:rh7-client4",
                "auth": {
                    "password": "myiscsipassword7myiscsipassword7myiscsipasswo",
                    "user": "myiscsiusername7",
                    "mutual_password": "myiscsipassword8",
                    "mutual_user": "myiscsiusername8"}
            })
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn

        self._task_post('/api/iscsi/target', create_request)
        self.assertStatus(201)
        self._task_put('/api/iscsi/target/{}'.format(create_request['target_iqn']), update_request)
        self.assertStatus(400)
        self._get('/api/iscsi/target/{}'.format(update_request['new_target_iqn']))
        self.assertStatus(200)
        self.assertJsonBody(response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_change_client_password(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw5"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['clients'][0]['auth']['password'] = 'MyNewPassword'
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['clients'][0]['auth']['password'] = 'MyNewPassword'
        self._update_iscsi_target(create_request, update_request, 200, None, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_rename_client(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw6"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['clients'][0]['client_iqn'] = 'iqn.1994-05.com.redhat:rh7-client0'
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['clients'][0]['client_iqn'] = 'iqn.1994-05.com.redhat:rh7-client0'
        self._update_iscsi_target(create_request, update_request, 200, None, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_add_disk(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw7"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['disks'].append(
            {
                "image": "lun3",
                "pool": "rbd",
                "controls": {},
                "backstore": "user:rbd"
            })
        update_request['clients'][0]['luns'].append({"image": "lun3", "pool": "rbd"})
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['disks'].append(
            {
                "image": "lun3",
                "pool": "rbd",
                "controls": {},
                "backstore": "user:rbd",
                "wwn": "64af6678-9694-4367-bacc-f8eb0baa2",
                "lun": 2

            })
        response['clients'][0]['luns'].append({"image": "lun3", "pool": "rbd"})
        self._update_iscsi_target(create_request, update_request, 200, None, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_change_disk_image(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw8"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['disks'][0]['image'] = 'lun0'
        update_request['clients'][0]['luns'][0]['image'] = 'lun0'
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['disks'][0]['image'] = 'lun0'
        response['clients'][0]['luns'][0]['image'] = 'lun0'
        self._update_iscsi_target(create_request, update_request, 200, None, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_change_disk_controls(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw9"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['disks'][0]['controls'] = {"qfull_timeout": 15}
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['disks'][0]['controls'] = {"qfull_timeout": 15}
        self._update_iscsi_target(create_request, update_request, 200, None, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_rename_target(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw10"
        new_target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw11"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = new_target_iqn
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = new_target_iqn
        self._update_iscsi_target(create_request, update_request, 200, None, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_rename_group(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw12"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['groups'][0]['group_id'] = 'mygroup0'
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['groups'][0]['group_id'] = 'mygroup0'
        self._update_iscsi_target(create_request, update_request, 200, None, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_add_client_to_group(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw13"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['clients'].append(
            {
                "luns": [],
                "client_iqn": "iqn.1994-05.com.redhat:rh7-client3",
                "auth": {
                    "password": None,
                    "user": None,
                    "mutual_password": None,
                    "mutual_user": None}
            })
        update_request['groups'][0]['members'].append('iqn.1994-05.com.redhat:rh7-client3')
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['clients'].append(
            {
                "luns": [],
                "client_iqn": "iqn.1994-05.com.redhat:rh7-client3",
                "auth": {
                    "password": None,
                    "user": None,
                    "mutual_password": None,
                    "mutual_user": None},
                "info": {
                    "alias": "",
                    "ip_address": [],
                    "state": {}
                }
            })
        response['groups'][0]['members'].append('iqn.1994-05.com.redhat:rh7-client3')
        self._update_iscsi_target(create_request, update_request, 200, None, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_remove_client_from_group(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw14"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['groups'][0]['members'].remove('iqn.1994-05.com.redhat:rh7-client2')
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['groups'][0]['members'].remove('iqn.1994-05.com.redhat:rh7-client2')
        self._update_iscsi_target(create_request, update_request, 200, None, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_remove_groups(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw15"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['groups'] = []
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['groups'] = []
        self._update_iscsi_target(create_request, update_request, 200, None, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_add_client_to_multiple_groups(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw16"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        create_request['groups'].append(copy.deepcopy(create_request['groups'][0]))
        create_request['groups'][1]['group_id'] = 'mygroup2'
        self._task_post('/api/iscsi/target', create_request)
        self.assertStatus(400)
        self.assertJsonBody({
            'detail': 'Each initiator can only be part of 1 group at a time',
            'code': 'initiator_in_multiple_groups',
            'component': 'iscsi'
        })

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_remove_client_lun(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw17"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        create_request['clients'][0]['luns'] = [
            {"image": "lun1", "pool": "rbd"},
            {"image": "lun2", "pool": "rbd"},
            {"image": "lun3", "pool": "rbd"}
        ]
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['clients'][0]['luns'] = [
            {"image": "lun1", "pool": "rbd"},
            {"image": "lun3", "pool": "rbd"}
        ]
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['clients'][0]['luns'] = [
            {"image": "lun1", "pool": "rbd"},
            {"image": "lun3", "pool": "rbd"}
        ]
        self._update_iscsi_target(create_request, update_request, 200, None, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_change_client_auth(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw18"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['clients'][0]['auth']['password'] = 'myiscsipasswordX'
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['clients'][0]['auth']['password'] = 'myiscsipasswordX'
        self._update_iscsi_target(create_request, update_request, 200, None, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_remove_client_logged_in(self, _validate_image_mock):
        client_info = {
            'alias': '',
            'ip_address': [],
            'state': {'LOGGED_IN': ['node1']}
        }
        # pylint: disable=protected-access
        IscsiClientMock._instance.clientinfo = client_info
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw19"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['clients'].pop(0)
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        for client in response['clients']:
            client['info'] = client_info
        update_response = {
            'detail': "Client 'iqn.1994-05.com.redhat:rh7-client' cannot be deleted until it's "
                      "logged out",
            'code': 'client_logged_in',
            'component': 'iscsi'
        }
        self._update_iscsi_target(create_request, update_request, 400, update_response, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_remove_client(self, _validate_image_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw20"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['clients'].pop(0)
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['clients'].pop(0)
        self._update_iscsi_target(create_request, update_request, 200, None, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_add_image_to_group_with_client_logged_in(self, _validate_image_mock):
        client_info = {
            'alias': '',
            'ip_address': [],
            'state': {'LOGGED_IN': ['node1']}
        }
        new_disk = {"pool": "rbd", "image": "lun1"}
        # pylint: disable=protected-access
        IscsiClientMock._instance.clientinfo = client_info
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw21"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['groups'][0]['disks'].append(new_disk)
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['groups'][0]['disks'].insert(0, new_disk)
        for client in response['clients']:
            client['info'] = client_info
        self._update_iscsi_target(create_request, update_request, 200, None, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_add_image_to_initiator_with_client_logged_in(self, _validate_image_mock):
        client_info = {
            'alias': '',
            'ip_address': [],
            'state': {'LOGGED_IN': ['node1']}
        }
        new_disk = {"pool": "rbd", "image": "lun2"}
        # pylint: disable=protected-access
        IscsiClientMock._instance.clientinfo = client_info
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw22"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['clients'][0]['luns'].append(new_disk)
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['clients'][0]['luns'].append(new_disk)
        for client in response['clients']:
            client['info'] = client_info
        self._update_iscsi_target(create_request, update_request, 200, None, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image')
    def test_remove_image_from_group_with_client_logged_in(self, _validate_image_mock):
        client_info = {
            'alias': '',
            'ip_address': [],
            'state': {'LOGGED_IN': ['node1']}
        }
        # pylint: disable=protected-access
        IscsiClientMock._instance.clientinfo = client_info
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw23"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['groups'][0]['disks'] = []
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['groups'][0]['disks'] = []
        for client in response['clients']:
            client['info'] = client_info
        self._update_iscsi_target(create_request, update_request, 200, None, response)

    def _update_iscsi_target(self, create_request, update_request, update_response_code,
                             update_response, response):
        self._task_post('/api/iscsi/target', create_request)
        self.assertStatus(201)
        self._task_put(
            '/api/iscsi/target/{}'.format(create_request['target_iqn']), update_request)
        self.assertStatus(update_response_code)
        self.assertJsonBody(update_response)
        self._get(
            '/api/iscsi/target/{}'.format(update_request['new_target_iqn']))
        self.assertStatus(200)
        self.assertJsonBody(response)


class TestIscsiUi(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        cls.setup_controllers([IscsiUi])

    @mock.patch('dashboard.services.tcmu_service.TcmuService.get_image_info')
    @mock.patch('dashboard.services.tcmu_service.TcmuService.get_iscsi_info')
    def test_overview(self, get_iscsi_info_mock, get_image_info_mock):
        get_iscsi_info_mock.return_value = None
        get_image_info_mock.return_value = None
        response = copy.deepcopy(iscsiui_response)
        response['images'] = []
        self._get('/ui-api/iscsi/overview')
        self.assertStatus(200)
        self.assertJsonBody(response)

    @mock.patch('dashboard.services.iscsi_config.IscsiGatewaysConfig.get_gateways_config')
    @mock.patch('dashboard.services.tcmu_service.TcmuService.get_image_info')
    @mock.patch('dashboard.services.tcmu_service.TcmuService.get_iscsi_info')
    def test_overview_config(self, get_iscsi_info_mock, get_image_info_mock,
                             get_gateways_config_mock):
        get_iscsi_info_mock.return_value = None
        get_image_info_mock.return_value = None
        response = copy.deepcopy(iscsiui_response)
        response['images'] = []
        get_gateways_config_mock.return_value = iscsiui_gateways_config_mock
        self._get('/ui-api/iscsi/overview')
        self.assertStatus(200)
        self.assertJsonBody(response)

        def raise_ex(self):
            raise RequestException('error')
        config_method = IscsiClientMock.get_config
        IscsiClientMock.get_config = raise_ex
        response['gateways'][0]['num_sessions'] = 'n/a'
        response['gateways'][1]['num_sessions'] = 'n/a'
        response['gateways'][0]['num_targets'] = 'n/a'
        response['gateways'][1]['num_targets'] = 'n/a'
        self._get('/ui-api/iscsi/overview')
        self.assertStatus(200)
        self.assertJsonBody(response)
        IscsiClientMock.get_config = config_method

    @mock.patch('dashboard.services.iscsi_config.IscsiGatewaysConfig.get_gateways_config')
    @mock.patch('dashboard.services.tcmu_service.TcmuService.get_image_info')
    @mock.patch('dashboard.services.tcmu_service.TcmuService.get_iscsi_info')
    def test_overview_ping(self, get_iscsi_info_mock, get_image_info_mock,
                           get_gateways_config_mock):
        get_iscsi_info_mock.return_value = None
        get_image_info_mock.return_value = None
        get_gateways_config_mock.return_value = iscsiui_gateways_config_mock
        response = copy.deepcopy(iscsiui_response)
        response['gateways'][0]['num_sessions'] = 0
        response['gateways'][1]['num_sessions'] = 0
        response['gateways'][0]['num_targets'] = 0
        response['gateways'][1]['num_targets'] = 0
        self._get('/ui-api/iscsi/overview')
        self.assertStatus(200)
        self.assertJsonBody(response)

        def raise_ex(self):
            raise RequestException('error')
        ping_method = IscsiClientMock.ping
        IscsiClientMock.ping = raise_ex
        response['gateways'][0]['num_sessions'] = 'n/a'
        response['gateways'][1]['num_sessions'] = 'n/a'
        response['gateways'][0]['state'] = 'down'
        response['gateways'][1]['state'] = 'down'
        self._get('/ui-api/iscsi/overview')
        self.assertStatus(200)
        self.assertJsonBody(response)
        IscsiClientMock.ping = ping_method

    @mock.patch(
        'dashboard.services.iscsi_config.IscsiGatewaysConfig.get_gateways_config')
    @mock.patch('dashboard.services.tcmu_service.TcmuService.get_image_info')
    @mock.patch('dashboard.services.tcmu_service.TcmuService.get_iscsi_info')
    def test_overview_images_info(self, get_iscsi_info_mock, get_image_info_mock,
                                  get_gateways_config_mock):
        get_iscsi_info_mock.return_value = None
        image_info = {"optimized_since": "1616735075", "stats": {}, "stats_history": {}}
        # pylint: disable=protected-access
        IscsiClientMock._instance.config['disks'] = {
            1: {"image": "lun1", "pool": "rbd", "backstore": "user:rbd",
                "optimized_since": "1616735075", "stats": {}, "stats_history": {}},
            2: {"image": "lun2", "pool": "rbd", "backstore": "user:rbd",
                "optimized_since": "1616735075", "stats": {}, "stats_history": {}},
        }
        response = copy.deepcopy(iscsiui_response)
        response['images'][0]['optimized_since'] = '1616735075'
        response['images'][1]['optimized_since'] = '1616735075'
        response['images'][0]['stats'] = {}
        response['images'][1]['stats'] = {}
        response['images'][0]['stats_history'] = {}
        response['images'][1]['stats_history'] = {}
        get_gateways_config_mock.return_value = iscsiui_gateways_config_mock
        get_image_info_mock.return_value = image_info
        self._get('/ui-api/iscsi/overview')
        self.assertStatus(200)
        self.assertJsonBody(response)


iscsiui_gateways_config_mock = {
    'gateways': {
        'node1': None,
        'node2': None,
    },
    'disks': {
        1: {"image": "lun1", "pool": "rbd", "backstore": "user:rbd",
            "controls": {"max_data_area_mb": 128}},
        2: {"image": "lun2", "pool": "rbd", "backstore": "user:rbd",
            "controls": {"max_data_area_mb": 128}}
    }
}
iscsiui_response = {
    "gateways": [
        {"name": "node1", "state": "up", "num_targets": 0, "num_sessions": 0},
        {"name": "node2", "state": "up", "num_targets": 0, "num_sessions": 0}
    ],
    "images": [
        {
            'pool': 'rbd',
            'image': 'lun1',
            'backstore': 'user:rbd',
            'optimized_since': None,
            'stats': None,
            'stats_history': None
        },
        {
            'pool': 'rbd',
            'image': 'lun2',
            'backstore': 'user:rbd',
            'optimized_since': None,
            'stats': None,
            'stats_history': None
        }
    ]
}
iscsi_target_request = {
    "target_iqn": "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw",
    "portals": [
        {"ip": "192.168.100.202", "host": "node2"},
        {"ip": "10.0.2.15", "host": "node2"},
        {"ip": "192.168.100.203", "host": "node3"}
    ],
    "disks": [
        {"image": "lun1", "pool": "rbd", "backstore": "user:rbd",
         "controls": {"max_data_area_mb": 128}},
        {"image": "lun2", "pool": "rbd", "backstore": "user:rbd",
         "controls": {"max_data_area_mb": 128}}
    ],
    "clients": [
        {
            "luns": [{"image": "lun1", "pool": "rbd"}],
            "client_iqn": "iqn.1994-05.com.redhat:rh7-client",
            "auth": {
                "password": "myiscsipassword1",
                "user": "myiscsiusername1",
                "mutual_password": "myiscsipassword2",
                "mutual_user": "myiscsiusername2"}
        },
        {
            "luns": [],
            "client_iqn": "iqn.1994-05.com.redhat:rh7-client2",
            "auth": {
                "password": "myiscsipassword3",
                "user": "myiscsiusername3",
                "mutual_password": "myiscsipassword4",
                "mutual_user": "myiscsiusername4"
            }
        }
    ],
    "acl_enabled": True,
    "auth": {
        "password": "",
        "user": "",
        "mutual_password": "",
        "mutual_user": ""},
    "target_controls": {},
    "groups": [
        {
            "group_id": "mygroup",
            "disks": [{"pool": "rbd", "image": "lun2"}],
            "members": ["iqn.1994-05.com.redhat:rh7-client2"]
        }
    ]
}

iscsi_target_response = {
    'target_iqn': 'iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw',
    'portals': [
        {'host': 'node2', 'ip': '10.0.2.15'},
        {'host': 'node2', 'ip': '192.168.100.202'},
        {'host': 'node3', 'ip': '192.168.100.203'}
    ],
    'disks': [
        {'pool': 'rbd', 'image': 'lun1', 'backstore': 'user:rbd',
         'wwn': '64af6678-9694-4367-bacc-f8eb0baa0', 'lun': 0,
         'controls': {'max_data_area_mb': 128}},
        {'pool': 'rbd', 'image': 'lun2', 'backstore': 'user:rbd',
         'wwn': '64af6678-9694-4367-bacc-f8eb0baa1', 'lun': 1,
         'controls': {'max_data_area_mb': 128}}
    ],
    'clients': [
        {
            'client_iqn': 'iqn.1994-05.com.redhat:rh7-client',
            'luns': [{'pool': 'rbd', 'image': 'lun1'}],
            'auth': {
                'user': 'myiscsiusername1',
                'password': 'myiscsipassword1',
                'mutual_password': 'myiscsipassword2',
                'mutual_user': 'myiscsiusername2'
            },
            'info': {
                'alias': '',
                'ip_address': [],
                'state': {}
            }
        },
        {
            'client_iqn': 'iqn.1994-05.com.redhat:rh7-client2',
            'luns': [],
            'auth': {
                'user': 'myiscsiusername3',
                'password': 'myiscsipassword3',
                'mutual_password': 'myiscsipassword4',
                'mutual_user': 'myiscsiusername4'
            },
            'info': {
                'alias': '',
                'ip_address': [],
                'state': {}
            }
        }
    ],
    "acl_enabled": True,
    "auth": {
        "password": "",
        "user": "",
        "mutual_password": "",
        "mutual_user": ""},
    'groups': [
        {
            'group_id': 'mygroup',
            'disks': [{'pool': 'rbd', 'image': 'lun2'}],
            'members': ['iqn.1994-05.com.redhat:rh7-client2']
        }
    ],
    'target_controls': {},
    'info': {
        'num_sessions': 0
    }
}


class IscsiClientMock(object):

    _instance = None

    def __init__(self):
        self.gateway_name = None
        self.service_url = None
        self.config = {
            "created": "2019/01/17 08:57:16",
            "discovery_auth": {
                "username": "",
                "password": "",
                "password_encryption_enabled": False,
                "mutual_username": "",
                "mutual_password": "",
                "mutual_password_encryption_enabled": False
            },
            "disks": {},
            "epoch": 0,
            "gateways": {},
            "targets": {},
            "updated": "",
            "version": 11
        }
        self.clientinfo = {
            'alias': '',
            'ip_address': [],
            'state': {}
        }

    @classmethod
    def instance(cls, gateway_name=None, service_url=None):
        cls._instance.gateway_name = gateway_name
        cls._instance.service_url = service_url
        # pylint: disable=unused-argument
        return cls._instance

    def ping(self):
        return {
            "message": "pong"
        }

    def get_settings(self):
        return {
            "api_version": 2,
            "backstores": [
                "user:rbd"
            ],
            "config": {
                "minimum_gateways": 2
            },
            "default_backstore": "user:rbd",
            "required_rbd_features": {
                "rbd": 0,
                "user:rbd": 4,
            },
            "unsupported_rbd_features": {
                "rbd": 88,
                "user:rbd": 0,
            },
            "disk_default_controls": {
                "user:rbd": {
                    "hw_max_sectors": 1024,
                    "max_data_area_mb": 8,
                    "osd_op_timeout": 30,
                    "qfull_timeout": 5
                }
            },
            "target_default_controls": {
                "cmdsn_depth": 128,
                "dataout_timeout": 20,
                "first_burst_length": 262144,
                "immediate_data": "Yes",
                "initial_r2t": "Yes",
                "max_burst_length": 524288,
                "max_outstanding_r2t": 1,
                "max_recv_data_segment_length": 262144,
                "max_xmit_data_segment_length": 262144,
                "nopin_response_timeout": 5,
                "nopin_timeout": 5
            }
        }

    def get_config(self):
        return copy.deepcopy(self.config)

    def create_target(self, target_iqn, target_controls):
        self.config['targets'][target_iqn] = {
            "clients": {},
            "acl_enabled": True,
            "auth": {
                "username": "",
                "password": "",
                "password_encryption_enabled": False,
                "mutual_username": "",
                "mutual_password": "",
                "mutual_password_encryption_enabled": False
            },
            "controls": target_controls,
            "created": "2019/01/17 09:22:34",
            "disks": {},
            "groups": {},
            "portals": {}
        }

    def create_gateway(self, target_iqn, gateway_name, ip_addresses):
        target_config = self.config['targets'][target_iqn]
        if 'ip_list' not in target_config:
            target_config['ip_list'] = []
        target_config['ip_list'] += ip_addresses
        target_config['portals'][gateway_name] = {
            "portal_ip_addresses": ip_addresses
        }

    def delete_gateway(self, target_iqn, gateway_name):
        target_config = self.config['targets'][target_iqn]
        portal_config = target_config['portals'][gateway_name]
        for ip in portal_config['portal_ip_addresses']:
            target_config['ip_list'].remove(ip)
        target_config['portals'].pop(gateway_name)

    def create_disk(self, pool, image, backstore, wwn):
        if wwn is None:
            wwn = '64af6678-9694-4367-bacc-f8eb0baa' + str(len(self.config['disks']))
        image_id = '{}/{}'.format(pool, image)
        self.config['disks'][image_id] = {
            "pool": pool,
            "image": image,
            "backstore": backstore,
            "controls": {},
            "wwn": wwn
        }

    def create_target_lun(self, target_iqn, image_id, lun):
        target_config = self.config['targets'][target_iqn]
        if lun is None:
            lun = len(target_config['disks'])
        target_config['disks'][image_id] = {
            "lun_id": lun
        }
        self.config['disks'][image_id]['owner'] = list(target_config['portals'].keys())[0]

    def reconfigure_disk(self, pool, image, controls):
        image_id = '{}/{}'.format(pool, image)
        settings = self.get_settings()
        backstore = self.config['disks'][image_id]['backstore']
        disk_default_controls = settings['disk_default_controls'][backstore]
        new_controls = {}
        for control_k, control_v in controls.items():
            if control_v != disk_default_controls[control_k]:
                new_controls[control_k] = control_v
        self.config['disks'][image_id]['controls'] = new_controls

    def create_client(self, target_iqn, client_iqn):
        target_config = self.config['targets'][target_iqn]
        target_config['clients'][client_iqn] = {
            "auth": {
                "username": "",
                "password": "",
                "password_encryption_enabled": False,
                "mutual_username": "",
                "mutual_password": "",
                "mutual_password_encryption_enabled": False
            },
            "group_name": "",
            "luns": {}
        }

    def create_client_lun(self, target_iqn, client_iqn, image_id):
        target_config = self.config['targets'][target_iqn]
        target_config['clients'][client_iqn]['luns'][image_id] = {}

    def delete_client_lun(self, target_iqn, client_iqn, image_id):
        target_config = self.config['targets'][target_iqn]
        del target_config['clients'][client_iqn]['luns'][image_id]

    def create_client_auth(self, target_iqn, client_iqn, user, password, m_user, m_password):
        target_config = self.config['targets'][target_iqn]
        target_config['clients'][client_iqn]['auth']['username'] = user
        target_config['clients'][client_iqn]['auth']['password'] = password
        target_config['clients'][client_iqn]['auth']['mutual_username'] = m_user
        target_config['clients'][client_iqn]['auth']['mutual_password'] = m_password

    def create_group(self, target_iqn, group_name, members, image_ids):
        target_config = self.config['targets'][target_iqn]
        target_config['groups'][group_name] = {
            "disks": {},
            "members": []
        }
        for image_id in image_ids:
            target_config['groups'][group_name]['disks'][image_id] = {}
        target_config['groups'][group_name]['members'] = members

    def update_group(self, target_iqn, group_name, members, image_ids):
        target_config = self.config['targets'][target_iqn]
        group = target_config['groups'][group_name]
        old_members = group['members']
        disks = group['disks']
        target_config['groups'][group_name] = {
            "disks": {},
            "members": []
        }

        for image_id in disks.keys():
            if image_id not in image_ids:
                target_config['groups'][group_name]['disks'][image_id] = {}

        new_members = []
        for member_iqn in old_members:
            if member_iqn not in members:
                new_members.append(member_iqn)
        target_config['groups'][group_name]['members'] = new_members

    def delete_group(self, target_iqn, group_name):
        target_config = self.config['targets'][target_iqn]
        del target_config['groups'][group_name]

    def delete_client(self, target_iqn, client_iqn):
        target_config = self.config['targets'][target_iqn]
        del target_config['clients'][client_iqn]

    def delete_target_lun(self, target_iqn, image_id):
        target_config = self.config['targets'][target_iqn]
        target_config['disks'].pop(image_id)
        del self.config['disks'][image_id]['owner']

    def delete_disk(self, pool, image):
        image_id = '{}/{}'.format(pool, image)
        del self.config['disks'][image_id]

    def delete_target(self, target_iqn):
        del self.config['targets'][target_iqn]

    def get_ip_addresses(self):
        ips = {
            'node1': ['192.168.100.201'],
            'node2': ['192.168.100.202', '10.0.2.15'],
            'node3': ['192.168.100.203']
        }
        return {'data': ips[self.gateway_name]}

    def get_hostname(self):
        hostnames = {
            'https://admin:admin@10.17.5.1:5001': 'node1',
            'https://admin:admin@10.17.5.2:5001': 'node2',
            'https://admin:admin@10.17.5.3:5001': 'node3'
        }
        if self.service_url not in hostnames:
            raise RequestException('No route to host')
        return {'data': hostnames[self.service_url]}

    def update_discoveryauth(self, user, password, mutual_user, mutual_password):
        self.config['discovery_auth']['username'] = user
        self.config['discovery_auth']['password'] = password
        self.config['discovery_auth']['mutual_username'] = mutual_user
        self.config['discovery_auth']['mutual_password'] = mutual_password

    def update_targetacl(self, target_iqn, action):
        self.config['targets'][target_iqn]['acl_enabled'] = (action == 'enable_acl')

    def update_targetauth(self, target_iqn, user, password, mutual_user, mutual_password):
        target_config = self.config['targets'][target_iqn]
        target_config['auth']['username'] = user
        target_config['auth']['password'] = password
        target_config['auth']['mutual_username'] = mutual_user
        target_config['auth']['mutual_password'] = mutual_password

    def get_targetinfo(self, target_iqn):
        # pylint: disable=unused-argument
        return {
            'num_sessions': 0
        }

    def get_clientinfo(self, target_iqn, client_iqn):
        # pylint: disable=unused-argument
        return self.clientinfo
