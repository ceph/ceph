import copy
import mock

from .helper import ControllerTestCase
from .. import mgr
from ..controllers.iscsi import Iscsi, IscsiTarget
from ..services.iscsi_client import IscsiClient


class IscsiTest(ControllerTestCase):

    @classmethod
    def setup_server(cls):
        mgr.rados.side_effect = None
        # pylint: disable=protected-access
        Iscsi._cp_config['tools.authenticate.on'] = False
        IscsiTarget._cp_config['tools.authenticate.on'] = False
        cls.setup_controllers([Iscsi, IscsiTarget])

    def setUp(self):
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

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image_exists')
    def test_list(self, _validate_image_exists_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw1"
        request = copy.deepcopy(iscsi_target_request)
        request['target_iqn'] = target_iqn
        self._post('/api/iscsi/target', request)
        self.assertStatus(201)
        self._get('/api/iscsi/target')
        self.assertStatus(200)
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        self.assertJsonBody([response])

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image_exists')
    def test_create(self, _validate_image_exists_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw2"
        request = copy.deepcopy(iscsi_target_request)
        request['target_iqn'] = target_iqn
        self._post('/api/iscsi/target', request)
        self.assertStatus(201)
        self._get('/api/iscsi/target/{}'.format(request['target_iqn']))
        self.assertStatus(200)
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        self.assertJsonBody(response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image_exists')
    def test_delete(self, _validate_image_exists_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw3"
        request = copy.deepcopy(iscsi_target_request)
        request['target_iqn'] = target_iqn
        self._post('/api/iscsi/target', request)
        self.assertStatus(201)
        self._delete('/api/iscsi/target/{}'.format(request['target_iqn']))
        self.assertStatus(204)
        self._get('/api/iscsi/target')
        self.assertStatus(200)
        self.assertJsonBody([])

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image_exists')
    def test_add_client(self, _validate_image_exists_mock):
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
                    "mutual_user": "myiscsiusername6"}
            })
        self._update_iscsi_target(create_request, update_request, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image_exists')
    def test_change_client_password(self, _validate_image_exists_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw5"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['clients'][0]['auth']['password'] = 'mynewiscsipassword'
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['clients'][0]['auth']['password'] = 'mynewiscsipassword'
        self._update_iscsi_target(create_request, update_request, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image_exists')
    def test_rename_client(self, _validate_image_exists_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw6"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['clients'][0]['client_iqn'] = 'iqn.1994-05.com.redhat:rh7-client0'
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['clients'][0]['client_iqn'] = 'iqn.1994-05.com.redhat:rh7-client0'
        self._update_iscsi_target(create_request, update_request, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image_exists')
    def test_add_disk(self, _validate_image_exists_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw7"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['disks'].append(
            {
                "image": "lun3",
                "pool": "rbd",
                "controls": {}
            })
        update_request['clients'][0]['luns'].append({"image": "lun3", "pool": "rbd"})
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['disks'].append(
            {
                "image": "lun3",
                "pool": "rbd",
                "controls": {}
            })
        response['clients'][0]['luns'].append({"image": "lun3", "pool": "rbd"})
        self._update_iscsi_target(create_request, update_request, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image_exists')
    def test_change_disk_image(self, _validate_image_exists_mock):
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
        self._update_iscsi_target(create_request, update_request, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image_exists')
    def test_change_disk_controls(self, _validate_image_exists_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw9"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['disks'][0]['controls'] = {"qfull_timeout": 15}
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['disks'][0]['controls'] = {"qfull_timeout": 15}
        self._update_iscsi_target(create_request, update_request, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image_exists')
    def test_rename_target(self, _validate_image_exists_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw10"
        new_target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw11"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = new_target_iqn
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = new_target_iqn
        self._update_iscsi_target(create_request, update_request, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image_exists')
    def test_rename_group(self, _validate_image_exists_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw12"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['groups'][0]['group_id'] = 'mygroup0'
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['groups'][0]['group_id'] = 'mygroup0'
        self._update_iscsi_target(create_request, update_request, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image_exists')
    def test_add_client_to_group(self, _validate_image_exists_mock):
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
                    "mutual_user": None}
            })
        response['groups'][0]['members'].append('iqn.1994-05.com.redhat:rh7-client3')
        self._update_iscsi_target(create_request, update_request, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image_exists')
    def test_remove_client_from_group(self, _validate_image_exists_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw14"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['groups'][0]['members'].remove('iqn.1994-05.com.redhat:rh7-client2')
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['groups'][0]['members'].remove('iqn.1994-05.com.redhat:rh7-client2')
        self._update_iscsi_target(create_request, update_request, response)

    @mock.patch('dashboard.controllers.iscsi.IscsiTarget._validate_image_exists')
    def test_remove_groups(self, _validate_image_exists_mock):
        target_iqn = "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw15"
        create_request = copy.deepcopy(iscsi_target_request)
        create_request['target_iqn'] = target_iqn
        update_request = copy.deepcopy(create_request)
        update_request['new_target_iqn'] = target_iqn
        update_request['groups'] = []
        response = copy.deepcopy(iscsi_target_response)
        response['target_iqn'] = target_iqn
        response['groups'] = []
        self._update_iscsi_target(create_request, update_request, response)

    def _update_iscsi_target(self, create_request, update_request, response):
        self._post('/api/iscsi/target', create_request)
        self.assertStatus(201)
        self._put('/api/iscsi/target/{}'.format(create_request['target_iqn']), update_request)
        self.assertStatus(200)
        self._get('/api/iscsi/target/{}'.format(update_request['new_target_iqn']))
        self.assertStatus(200)
        self.assertJsonBody(response)


iscsi_target_request = {
    "target_iqn": "iqn.2003-01.com.redhat.iscsi-gw:iscsi-igw",
    "portals": [
        {"ip": "192.168.100.202", "host": "node2"},
        {"ip": "10.0.2.15", "host": "node2"},
        {"ip": "192.168.100.203", "host": "node3"}
    ],
    "disks": [
        {"image": "lun1", "pool": "rbd", "controls": {"max_data_area_mb": 128}},
        {"image": "lun2", "pool": "rbd", "controls": {"max_data_area_mb": 128}}
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
        {'pool': 'rbd', 'image': 'lun1', 'controls': {'max_data_area_mb': 128}},
        {'pool': 'rbd', 'image': 'lun2', 'controls': {'max_data_area_mb': 128}}
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
            }
        }
    ],
    'groups': [
        {
            'group_id': 'mygroup',
            'disks': [{'pool': 'rbd', 'image': 'lun2'}],
            'members': ['iqn.1994-05.com.redhat:rh7-client2']
        }
    ],
    'target_controls': {}
}


class IscsiClientMock(object):

    _instance = None

    def __init__(self):
        self.gateway_name = None
        self.config = {
            "created": "2019/01/17 08:57:16",
            "discovery_auth": {
                "chap": "",
                "chap_mutual": ""
            },
            "disks": {},
            "epoch": 0,
            "gateways": {},
            "targets": {},
            "updated": "",
            "version": 4
        }

    @classmethod
    def instance(cls, gateway_name=None):
        cls._instance.gateway_name = gateway_name
        # pylint: disable=unused-argument
        return cls._instance

    def ping(self):
        return {
            "message": "pong"
        }

    def get_settings(self):
        return {
            "config": {
                "minimum_gateways": 2
            },
            "disk_default_controls": {
                "hw_max_sectors": 1024,
                "max_data_area_mb": 8,
                "osd_op_timeout": 30,
                "qfull_timeout": 5
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
        return self.config

    def create_target(self, target_iqn, target_controls):
        self.config['targets'][target_iqn] = {
            "clients": {},
            "controls": target_controls,
            "created": "2019/01/17 09:22:34",
            "disks": [],
            "groups": {},
            "portals": {}
        }

    def create_gateway(self, target_iqn, gateway_name, ip_address):
        target_config = self.config['targets'][target_iqn]
        if 'ip_list' not in target_config:
            target_config['ip_list'] = []
        target_config['ip_list'] += ip_address
        target_config['portals'][gateway_name] = {
            "portal_ip_address": ip_address[0]
        }

    def create_disk(self, image_id):
        pool, image = image_id.split('.')
        self.config['disks'][image_id] = {
            "pool": pool,
            "image": image,
            "controls": {}
        }

    def create_target_lun(self, target_iqn, image_id):
        target_config = self.config['targets'][target_iqn]
        target_config['disks'].append(image_id)
        self.config['disks'][image_id]['owner'] = list(target_config['portals'].keys())[0]

    def reconfigure_disk(self, image_id, controls):
        self.config['disks'][image_id]['controls'] = controls

    def create_client(self, target_iqn, client_iqn):
        target_config = self.config['targets'][target_iqn]
        target_config['clients'][client_iqn] = {
            "auth": {
                "chap": "",
                "chap_mutual": ""
            },
            "group_name": "",
            "luns": {}
        }

    def create_client_lun(self, target_iqn, client_iqn, image_id):
        target_config = self.config['targets'][target_iqn]
        target_config['clients'][client_iqn]['luns'][image_id] = {}

    def create_client_auth(self, target_iqn, client_iqn, chap, chap_mutual):
        target_config = self.config['targets'][target_iqn]
        target_config['clients'][client_iqn]['auth']['chap'] = chap
        target_config['clients'][client_iqn]['auth']['chap_mutual'] = chap_mutual

    def create_group(self, target_iqn, group_name, members, image_ids):
        target_config = self.config['targets'][target_iqn]
        target_config['groups'][group_name] = {
            "disks": {},
            "members": []
        }
        for image_id in image_ids:
            target_config['groups'][group_name]['disks'][image_id] = {}
        target_config['groups'][group_name]['members'] = members

    def delete_group(self, target_iqn, group_name):
        target_config = self.config['targets'][target_iqn]
        del target_config['groups'][group_name]

    def delete_client(self, target_iqn, client_iqn):
        target_config = self.config['targets'][target_iqn]
        del target_config['clients'][client_iqn]

    def delete_target_lun(self, target_iqn, image_id):
        target_config = self.config['targets'][target_iqn]
        target_config['disks'].remove(image_id)
        del self.config['disks'][image_id]['owner']

    def delete_disk(self, image_id):
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

    def update_discoveryauth(self, user, password, mutual_user, mutual_password):
        chap = ''
        if user and password:
            chap = '{}/{}'.format(user, password)
        chap_mutual = ''
        if mutual_user and mutual_password:
            chap_mutual = '{}/{}'.format(mutual_user, mutual_password)
        self.config['discovery_auth'] = {
            'chap': chap,
            'chap_mutual': chap_mutual
        }
