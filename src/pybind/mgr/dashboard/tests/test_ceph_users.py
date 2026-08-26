import unittest.mock as mock

from jsonschema import ValidationError, validate

from ..controllers.ceph_users import CephUser, create_form
from ..tests import ControllerTestCase

auth_dump_mock = {"auth_dump": [
    {"entity": "client.admin",
     "key": "RANDOMFi7NwMARAA7RdGqdav+BEEFDEAD0x00g==",
     "caps": {"mds": "allow *",
              "mgr": "allow *",
              "mon": "allow *",
              "osd": "allow *"}},
    {"entity": "client.bootstrap-mds",
     "key": "2RANDOMi7NwMARAA7RdGqdav+BEEFDEAD0x00g==",
     "caps": {"mds": "allow *",
              "osd": "allow *"}}
]}

CREATE_USER_PAYLOAD = {
    'user_entity': 'client.test',
    'capabilities': [{'entity': 'mon', 'cap': 'allow *'}],
}


class CephUsersControllerTestCase(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        cls.setup_crud_controllers(CephUser)

    @mock.patch('dashboard.services.ceph_service.CephService.send_command')
    def test_get_all(self, send_command):
        send_command.return_value = auth_dump_mock
        self._get('/api/cluster/user')
        self.assertStatus(200)
        self.assertJsonBody([
            {"entity": "client.admin",
             "caps": {"mds": "allow *",
                      "mgr": "allow *",
                      "mon": "allow *",
                      "osd": "allow *"},
             "key": "***********"
             },
            {"entity": "client.bootstrap-mds",
             "caps": {"mds": "allow *",
                      "osd": "allow *"},
             "key": "***********"
             }
        ])

    def test_create_form(self):
        form_dict = create_form.to_dict()
        schema = {'schema': form_dict['control_schema'], 'layout': form_dict['ui_schema']}
        validate(instance={'user_entity': 'foo',
                           'key_type': 'aes',
                           'capabilities': [{"entity": "mgr", "cap": "allow *"}]},
                 schema=schema['schema'])

    def test_create_form_key_type_options(self):
        schema = create_form.to_dict()['control_schema']
        key_type = schema['properties']['key_type']
        self.assertEqual(key_type['enum'], ['aes', 'aes256k'])
        self.assertEqual(key_type['default'], 'aes')
        self.assertIn('key_type', schema['required'])
        validate(instance={**CREATE_USER_PAYLOAD, 'key_type': 'aes256k'},
                 schema=schema)
        with self.assertRaises(ValidationError):
            validate(instance={**CREATE_USER_PAYLOAD, 'key_type': 'rsa'},
                     schema=schema)

    @mock.patch('dashboard.services.ceph_service.CephService.send_command')
    def test_create_user_without_key_type(self, send_command):
        send_command.return_value = ''
        self._post('/api/cluster/user', CREATE_USER_PAYLOAD)
        self.assertStatus(201)
        send_command.assert_called_with(
            'mon', 'auth add', entity='client.test', caps=['mon', 'allow *'])

    @mock.patch('dashboard.services.ceph_service.CephService.send_command')
    def test_create_user_with_aes(self, send_command):
        send_command.return_value = ''
        self._post('/api/cluster/user', {**CREATE_USER_PAYLOAD, 'key_type': 'aes'})
        self.assertStatus(201)
        send_command.assert_called_with(
            'mon', 'auth add', entity='client.test', caps=['mon', 'allow *'],
            key_type='aes')

    @mock.patch('dashboard.services.ceph_service.CephService.send_command')
    def test_create_user_with_aes256k(self, send_command):
        send_command.return_value = ''
        self._post('/api/cluster/user', {**CREATE_USER_PAYLOAD, 'key_type': 'aes256k'})
        self.assertStatus(201)
        send_command.assert_called_with(
            'mon', 'auth add', entity='client.test', caps=['mon', 'allow *'],
            key_type='aes256k')

    @mock.patch('dashboard.services.ceph_service.CephService.send_command')
    def test_create_user_with_invalid_key_type(self, send_command):
        self._post('/api/cluster/user', {**CREATE_USER_PAYLOAD, 'key_type': 'rsa'})
        self.assertStatus(400)
        send_command.assert_not_called()
