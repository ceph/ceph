import unittest.mock as mock

from jsonschema import validate

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
                           'capabilities': [{"entity": "mgr", "cap": "allow *"}]},
                 schema=schema['schema'])
