# -*- coding: utf-8 -*-
from __future__ import absolute_import

import unittest

from mock import MagicMock, Mock

from .. import mgr
from ..settings import Settings
from ..services import ganesha
from ..services.ganesha import GaneshaConf, Export, GaneshaConfParser


class GaneshaConfTest(unittest.TestCase):
    export_1 = """
EXPORT {
    Export_ID=1;
    Protocols = 4;
    Path = /;
    Pseudo = /cephfs_a/;
    Access_Type = RW;
    Protocols = 4;
    Attr_Expiration_Time = 0;
    # Delegations = R;
    # Squash = root;

    FSAL {
        Name = CEPH;
        Filesystem = "a";
        User_Id = "ganesha";
        # Secret_Access_Key = "YOUR SECRET KEY HERE";
    }

    CLIENT
    {
        Clients = 192.168.0.10, 192.168.1.0/8;
        Squash = None;
    }

    CLIENT
    {
        Clients = 192.168.0.0/16;
        Squash = All;
        Access_Type = RO;
    }
}
"""

    export_2 = """
EXPORT
{
    Export_ID=2;

    Path = "/";

    Pseudo = "/rgw";

    Access_Type = RW;

    squash = AllAnonymous;

    Protocols = 4, 3;

    Transports = TCP, UDP;

    FSAL {
        Name = RGW;
        User_Id = "testuser";
        Access_Key_Id ="access_key";
        Secret_Access_Key = "secret_key";
    }
}
"""

    conf_nodea = '''
%url rados://ganesha/ns/export-2

%url "rados://ganesha/ns/export-1"'''

    conf_nodeb = '%url "rados://ganesha/ns/export-1"'

    class RObject(object):
        def __init__(self, key, raw):
            self.key = key
            self.raw = raw

        def read(self, _):
            return self.raw.encode('utf-8')

        def stat(self):
            return len(self.raw), None

    def _ioctx_write_full_mock(self, key, content):
        if key not in self.temp_store:
            self.temp_store[key] = GaneshaConfTest.RObject(key,
                                                           content.decode('utf-8'))
        else:
            self.temp_store[key].raw = content.decode('utf-8')

    def _ioctx_remove_mock(self, key):
        del self.temp_store[key]

    def _ioctx_list_objects_mock(self):
        return [obj for _, obj in self.temp_store.items()]

    CONFIG_KEY_DICT = {}

    @classmethod
    def mock_set_module_option(cls, attr, val):
        cls.CONFIG_KEY_DICT[attr] = val

    @classmethod
    def mock_get_module_option(cls, attr, default):
        return cls.CONFIG_KEY_DICT.get(attr, default)

    def setUp(self):
        mgr.set_module_option.side_effect = self.mock_set_module_option
        mgr.get_module_option.side_effect = self.mock_get_module_option

        Settings.GANESHA_CLUSTERS_RADOS_POOL_NAMESPACE = "ganesha/ns"

        self.temp_store = {
            'export-1': GaneshaConfTest.RObject("export-1", self.export_1),
            'conf-nodea': GaneshaConfTest.RObject("conf-nodea", self.conf_nodea),
            'export-2': GaneshaConfTest.RObject("export-2", self.export_2),
            'conf-nodeb': GaneshaConfTest.RObject("conf-nodeb", self.conf_nodeb)
        }

        self.io_mock = MagicMock()
        self.io_mock.list_objects.side_effect = self._ioctx_list_objects_mock
        self.io_mock.write_full.side_effect = self._ioctx_write_full_mock
        self.io_mock.remove_object.side_effect = self._ioctx_remove_mock

        ioctx_mock = MagicMock()
        ioctx_mock.__enter__ = Mock(return_value=(self.io_mock))
        ioctx_mock.__exit__ = Mock(return_value=None)

        mgr.rados = MagicMock()
        mgr.rados.open_ioctx.return_value = ioctx_mock
        mgr.remote.return_value = False, None

        ganesha.CephX = MagicMock()
        ganesha.CephX.list_clients.return_value = ['ganesha']
        ganesha.CephX.get_client_key.return_value = 'ganesha'

        ganesha.CephFS = MagicMock()

    def test_export_parser_1(self):
        blocks = GaneshaConfParser(self.export_1).parse()
        self.assertIsInstance(blocks, list)
        self.assertEqual(len(blocks), 1)
        export = Export.from_export_block(blocks[0], '_default_',
                                          GaneshaConf.ganesha_defaults({}))

        self.assertEqual(export.export_id, 1)
        self.assertEqual(export.path, "/")
        self.assertEqual(export.pseudo, "/cephfs_a")
        self.assertIsNone(export.tag)
        self.assertEqual(export.access_type, "RW")
        self.assertEqual(export.squash, "root_squash")
        self.assertEqual(export.protocols, {4})
        self.assertEqual(export.transports, {"TCP", "UDP"})
        self.assertEqual(export.fsal.name, "CEPH")
        self.assertEqual(export.fsal.user_id, "ganesha")
        self.assertEqual(export.fsal.fs_name, "a")
        self.assertEqual(export.fsal.sec_label_xattr, None)
        self.assertEqual(len(export.clients), 2)
        self.assertEqual(export.clients[0].addresses,
                         ["192.168.0.10", "192.168.1.0/8"])
        self.assertEqual(export.clients[0].squash, "no_root_squash")
        self.assertIsNone(export.clients[0].access_type)
        self.assertEqual(export.clients[1].addresses, ["192.168.0.0/16"])
        self.assertEqual(export.clients[1].squash, "all_squash")
        self.assertEqual(export.clients[1].access_type, "RO")
        self.assertEqual(export.cluster_id, '_default_')
        self.assertEqual(export.attr_expiration_time, 0)
        self.assertEqual(export.security_label, False)

    def test_export_parser_2(self):
        blocks = GaneshaConfParser(self.export_2).parse()
        self.assertIsInstance(blocks, list)
        self.assertEqual(len(blocks), 1)
        export = Export.from_export_block(blocks[0], '_default_',
                                          GaneshaConf.ganesha_defaults({}))

        self.assertEqual(export.export_id, 2)
        self.assertEqual(export.path, "/")
        self.assertEqual(export.pseudo, "/rgw")
        self.assertIsNone(export.tag)
        self.assertEqual(export.access_type, "RW")
        self.assertEqual(export.squash, "all_squash")
        self.assertEqual(export.protocols, {4, 3})
        self.assertEqual(export.transports, {"TCP", "UDP"})
        self.assertEqual(export.fsal.name, "RGW")
        self.assertEqual(export.fsal.rgw_user_id, "testuser")
        self.assertEqual(export.fsal.access_key, "access_key")
        self.assertEqual(export.fsal.secret_key, "secret_key")
        self.assertEqual(len(export.clients), 0)
        self.assertEqual(export.cluster_id, '_default_')

    def test_daemon_conf_parser_a(self):
        blocks = GaneshaConfParser(self.conf_nodea).parse()
        self.assertIsInstance(blocks, list)
        self.assertEqual(len(blocks), 2)
        self.assertEqual(blocks[0]['block_name'], "%url")
        self.assertEqual(blocks[0]['value'], "rados://ganesha/ns/export-2")
        self.assertEqual(blocks[1]['block_name'], "%url")
        self.assertEqual(blocks[1]['value'], "rados://ganesha/ns/export-1")

    def test_daemon_conf_parser_b(self):
        blocks = GaneshaConfParser(self.conf_nodeb).parse()
        self.assertIsInstance(blocks, list)
        self.assertEqual(len(blocks), 1)
        self.assertEqual(blocks[0]['block_name'], "%url")
        self.assertEqual(blocks[0]['value'], "rados://ganesha/ns/export-1")

    def test_ganesha_conf(self):
        ganesha_conf = GaneshaConf.instance('_default_')
        exports = ganesha_conf.exports

        self.assertEqual(len(exports.items()), 2)
        self.assertIn(1, exports)
        self.assertIn(2, exports)

        # export_id = 1 asserts
        export = exports[1]
        self.assertEqual(export.export_id, 1)
        self.assertEqual(export.path, "/")
        self.assertEqual(export.pseudo, "/cephfs_a")
        self.assertIsNone(export.tag)
        self.assertEqual(export.access_type, "RW")
        self.assertEqual(export.squash, "root_squash")
        self.assertEqual(export.protocols, {4})
        self.assertEqual(export.transports, {"TCP", "UDP"})
        self.assertEqual(export.fsal.name, "CEPH")
        self.assertEqual(export.fsal.user_id, "ganesha")
        self.assertEqual(export.fsal.fs_name, "a")
        self.assertEqual(export.fsal.sec_label_xattr, None)
        self.assertEqual(len(export.clients), 2)
        self.assertEqual(export.clients[0].addresses,
                         ["192.168.0.10", "192.168.1.0/8"])
        self.assertEqual(export.clients[0].squash, "no_root_squash")
        self.assertIsNone(export.clients[0].access_type)
        self.assertEqual(export.clients[1].addresses, ["192.168.0.0/16"])
        self.assertEqual(export.clients[1].squash, "all_squash")
        self.assertEqual(export.clients[1].access_type, "RO")
        self.assertEqual(export.attr_expiration_time, 0)
        self.assertEqual(export.security_label, False)

        # export_id = 2 asserts
        export = exports[2]
        self.assertEqual(export.export_id, 2)
        self.assertEqual(export.path, "/")
        self.assertEqual(export.pseudo, "/rgw")
        self.assertIsNone(export.tag)
        self.assertEqual(export.access_type, "RW")
        self.assertEqual(export.squash, "all_squash")
        self.assertEqual(export.protocols, {4, 3})
        self.assertEqual(export.transports, {"TCP", "UDP"})
        self.assertEqual(export.fsal.name, "RGW")
        self.assertEqual(export.fsal.rgw_user_id, "testuser")
        self.assertEqual(export.fsal.access_key, "access_key")
        self.assertEqual(export.fsal.secret_key, "secret_key")
        self.assertEqual(len(export.clients), 0)

    def test_config_dict(self):
        conf = GaneshaConf.instance('_default_')
        export = conf.exports[1]
        ex_dict = export.to_dict()
        self.assertDictEqual(ex_dict, {
            'daemons': ['nodea', 'nodeb'],
            'export_id': 1,
            'path': '/',
            'pseudo': '/cephfs_a',
            'cluster_id': '_default_',
            'tag': None,
            'access_type': 'RW',
            'squash': 'root_squash',
            'security_label': False,
            'protocols': [4],
            'transports': ['TCP', 'UDP'],
            'clients': [{
                'addresses': ["192.168.0.10", "192.168.1.0/8"],
                'access_type': None,
                'squash': 'no_root_squash'
            }, {
                'addresses': ["192.168.0.0/16"],
                'access_type': 'RO',
                'squash': 'all_squash'
            }],
            'fsal': {
                'name': 'CEPH',
                'user_id': 'ganesha',
                'fs_name': 'a',
                'sec_label_xattr': None
            }
        })

        export = conf.exports[2]
        ex_dict = export.to_dict()
        self.assertDictEqual(ex_dict, {
            'daemons': ['nodea'],
            'export_id': 2,
            'path': '/',
            'pseudo': '/rgw',
            'cluster_id': '_default_',
            'tag': None,
            'access_type': 'RW',
            'squash': 'all_squash',
            'security_label': False,
            'protocols': [3, 4],
            'transports': ['TCP', 'UDP'],
            'clients': [],
            'fsal': {
                'name': 'RGW',
                'rgw_user_id': 'testuser'
            }
        })

    def test_config_from_dict(self):
        export = Export.from_dict(1, {
            'daemons': ['nodea', 'nodeb'],
            'export_id': 1,
            'path': '/',
            'cluster_id': '_default_',
            'pseudo': '/cephfs_a',
            'tag': None,
            'access_type': 'RW',
            'squash': 'root_squash',
            'security_label': True,
            'protocols': [4],
            'transports': ['TCP', 'UDP'],
            'clients': [{
                'addresses': ["192.168.0.10", "192.168.1.0/8"],
                'access_type': None,
                'squash': 'no_root_squash'
            }, {
                'addresses': ["192.168.0.0/16"],
                'access_type': 'RO',
                'squash': 'all_squash'
            }],
            'fsal': {
                'name': 'CEPH',
                'user_id': 'ganesha',
                'fs_name': 'a',
                'sec_label_xattr': 'security.selinux'
            }
        })

        self.assertEqual(export.export_id, 1)
        self.assertEqual(export.path, "/")
        self.assertEqual(export.pseudo, "/cephfs_a")
        self.assertIsNone(export.tag)
        self.assertEqual(export.access_type, "RW")
        self.assertEqual(export.squash, "root_squash")
        self.assertEqual(export.protocols, {4})
        self.assertEqual(export.transports, {"TCP", "UDP"})
        self.assertEqual(export.fsal.name, "CEPH")
        self.assertEqual(export.fsal.user_id, "ganesha")
        self.assertEqual(export.fsal.fs_name, "a")
        self.assertEqual(export.fsal.sec_label_xattr, 'security.selinux')
        self.assertEqual(len(export.clients), 2)
        self.assertEqual(export.clients[0].addresses,
                         ["192.168.0.10", "192.168.1.0/8"])
        self.assertEqual(export.clients[0].squash, "no_root_squash")
        self.assertIsNone(export.clients[0].access_type)
        self.assertEqual(export.clients[1].addresses, ["192.168.0.0/16"])
        self.assertEqual(export.clients[1].squash, "all_squash")
        self.assertEqual(export.clients[1].access_type, "RO")
        self.assertEqual(export.daemons, {"nodeb", "nodea"})
        self.assertEqual(export.cluster_id, '_default_')
        self.assertEqual(export.attr_expiration_time, 0)
        self.assertEqual(export.security_label, True)

        export = Export.from_dict(2, {
            'daemons': ['nodea'],
            'export_id': 2,
            'path': '/',
            'pseudo': '/rgw',
            'cluster_id': '_default_',
            'tag': None,
            'access_type': 'RW',
            'squash': 'all_squash',
            'security_label': False,
            'protocols': [4, 3],
            'transports': ['TCP', 'UDP'],
            'clients': [],
            'fsal': {
                'name': 'RGW',
                'rgw_user_id': 'testuser'
            }
        })

        self.assertEqual(export.export_id, 2)
        self.assertEqual(export.path, "/")
        self.assertEqual(export.pseudo, "/rgw")
        self.assertIsNone(export.tag)
        self.assertEqual(export.access_type, "RW")
        self.assertEqual(export.squash, "all_squash")
        self.assertEqual(export.protocols, {4, 3})
        self.assertEqual(export.transports, {"TCP", "UDP"})
        self.assertEqual(export.fsal.name, "RGW")
        self.assertEqual(export.fsal.rgw_user_id, "testuser")
        self.assertIsNone(export.fsal.access_key)
        self.assertIsNone(export.fsal.secret_key)
        self.assertEqual(len(export.clients), 0)
        self.assertEqual(export.daemons, {"nodea"})
        self.assertEqual(export.cluster_id, '_default_')

    def test_gen_raw_config(self):
        conf = GaneshaConf.instance('_default_')
        # pylint: disable=W0212
        export = conf.exports[1]
        del conf.exports[1]
        conf._save_export(export)
        conf = GaneshaConf.instance('_default_')
        exports = conf.exports
        self.assertEqual(len(exports.items()), 2)
        self.assertIn(1, exports)
        self.assertIn(2, exports)

        # export_id = 1 asserts
        export = exports[1]
        self.assertEqual(export.export_id, 1)
        self.assertEqual(export.path, "/")
        self.assertEqual(export.pseudo, "/cephfs_a")
        self.assertIsNone(export.tag)
        self.assertEqual(export.access_type, "RW")
        self.assertEqual(export.squash, "root_squash")
        self.assertEqual(export.protocols, {4})
        self.assertEqual(export.transports, {"TCP", "UDP"})
        self.assertEqual(export.fsal.name, "CEPH")
        self.assertEqual(export.fsal.user_id, "ganesha")
        self.assertEqual(export.fsal.fs_name, "a")
        self.assertEqual(export.fsal.sec_label_xattr, None)
        self.assertEqual(len(export.clients), 2)
        self.assertEqual(export.clients[0].addresses,
                         ["192.168.0.10", "192.168.1.0/8"])
        self.assertEqual(export.clients[0].squash, "no_root_squash")
        self.assertIsNone(export.clients[0].access_type)
        self.assertEqual(export.clients[1].addresses, ["192.168.0.0/16"])
        self.assertEqual(export.clients[1].squash, "all_squash")
        self.assertEqual(export.clients[1].access_type, "RO")
        self.assertEqual(export.daemons, {"nodeb", "nodea"})
        self.assertEqual(export.cluster_id, '_default_')
        self.assertEqual(export.attr_expiration_time, 0)
        self.assertEqual(export.security_label, False)

        # export_id = 2 asserts
        export = exports[2]
        self.assertEqual(export.export_id, 2)
        self.assertEqual(export.path, "/")
        self.assertEqual(export.pseudo, "/rgw")
        self.assertIsNone(export.tag)
        self.assertEqual(export.access_type, "RW")
        self.assertEqual(export.squash, "all_squash")
        self.assertEqual(export.protocols, {4, 3})
        self.assertEqual(export.transports, {"TCP", "UDP"})
        self.assertEqual(export.fsal.name, "RGW")
        self.assertEqual(export.fsal.rgw_user_id, "testuser")
        self.assertEqual(export.fsal.access_key, "access_key")
        self.assertEqual(export.fsal.secret_key, "secret_key")
        self.assertEqual(len(export.clients), 0)
        self.assertEqual(export.daemons, {"nodea"})
        self.assertEqual(export.cluster_id, '_default_')

    def test_update_export(self):
        ganesha.RgwClient = MagicMock()
        admin_inst_mock = MagicMock()
        admin_inst_mock.get_user_keys.return_value = {
            'access_key': 'access_key',
            'secret_key': 'secret_key'
        }
        ganesha.RgwClient.admin_instance.return_value = admin_inst_mock

        conf = GaneshaConf.instance('_default_')
        conf.update_export({
            'export_id': 2,
            'daemons': ["nodeb"],
            'path': 'bucket',
            'pseudo': '/rgw/bucket',
            'cluster_id': '_default_',
            'tag': 'bucket_tag',
            'access_type': 'RW',
            'squash': 'all_squash',
            'security_label': False,
            'protocols': [4, 3],
            'transports': ['TCP', 'UDP'],
            'clients': [{
                'addresses': ["192.168.0.0/16"],
                'access_type': None,
                'squash': None
            }],
            'fsal': {
                'name': 'RGW',
                'rgw_user_id': 'testuser'
            }
        })

        conf = GaneshaConf.instance('_default_')
        export = conf.get_export(2)
        self.assertEqual(export.export_id, 2)
        self.assertEqual(export.path, "bucket")
        self.assertEqual(export.pseudo, "/rgw/bucket")
        self.assertEqual(export.tag, "bucket_tag")
        self.assertEqual(export.access_type, "RW")
        self.assertEqual(export.squash, "all_squash")
        self.assertEqual(export.protocols, {4, 3})
        self.assertEqual(export.transports, {"TCP", "UDP"})
        self.assertEqual(export.fsal.name, "RGW")
        self.assertEqual(export.fsal.rgw_user_id, "testuser")
        self.assertEqual(export.fsal.access_key, "access_key")
        self.assertEqual(export.fsal.secret_key, "secret_key")
        self.assertEqual(len(export.clients), 1)
        self.assertEqual(export.clients[0].addresses, ["192.168.0.0/16"])
        self.assertIsNone(export.clients[0].squash)
        self.assertIsNone(export.clients[0].access_type)
        self.assertEqual(export.daemons, {"nodeb"})
        self.assertEqual(export.cluster_id, '_default_')

    def test_remove_export(self):
        conf = GaneshaConf.instance('_default_')
        conf.remove_export(1)
        exports = conf.list_exports()
        self.assertEqual(len(exports), 1)
        self.assertEqual(2, exports[0].export_id)
        export = conf.get_export(2)
        self.assertEqual(export.export_id, 2)
        self.assertEqual(export.path, "/")
        self.assertEqual(export.pseudo, "/rgw")
        self.assertIsNone(export.tag)
        self.assertEqual(export.access_type, "RW")
        self.assertEqual(export.squash, "all_squash")
        self.assertEqual(export.protocols, {4, 3})
        self.assertEqual(export.transports, {"TCP", "UDP"})
        self.assertEqual(export.fsal.name, "RGW")
        self.assertEqual(export.fsal.rgw_user_id, "testuser")
        self.assertEqual(export.fsal.access_key, "access_key")
        self.assertEqual(export.fsal.secret_key, "secret_key")
        self.assertEqual(len(export.clients), 0)
        self.assertEqual(export.daemons, {"nodea"})
        self.assertEqual(export.cluster_id, '_default_')

    def test_create_export_rgw(self):
        ganesha.RgwClient = MagicMock()
        admin_inst_mock = MagicMock()
        admin_inst_mock.get_user_keys.return_value = {
            'access_key': 'access_key2',
            'secret_key': 'secret_key2'
        }
        ganesha.RgwClient.admin_instance.return_value = admin_inst_mock

        conf = GaneshaConf.instance('_default_')
        ex_id = conf.create_export({
            'daemons': ["nodeb"],
            'path': 'bucket',
            'pseudo': '/rgw/bucket',
            'tag': 'bucket_tag',
            'cluster_id': '_default_',
            'access_type': 'RW',
            'squash': 'all_squash',
            'security_label': False,
            'protocols': [4, 3],
            'transports': ['TCP', 'UDP'],
            'clients': [{
                'addresses': ["192.168.0.0/16"],
                'access_type': None,
                'squash': None
            }],
            'fsal': {
                'name': 'RGW',
                'rgw_user_id': 'testuser'
            }
        })

        conf = GaneshaConf.instance('_default_')
        exports = conf.list_exports()
        self.assertEqual(len(exports), 3)
        export = conf.get_export(ex_id)
        self.assertEqual(export.export_id, ex_id)
        self.assertEqual(export.path, "bucket")
        self.assertEqual(export.pseudo, "/rgw/bucket")
        self.assertEqual(export.tag, "bucket_tag")
        self.assertEqual(export.access_type, "RW")
        self.assertEqual(export.squash, "all_squash")
        self.assertEqual(export.protocols, {4, 3})
        self.assertEqual(export.transports, {"TCP", "UDP"})
        self.assertEqual(export.fsal.name, "RGW")
        self.assertEqual(export.fsal.rgw_user_id, "testuser")
        self.assertEqual(export.fsal.access_key, "access_key2")
        self.assertEqual(export.fsal.secret_key, "secret_key2")
        self.assertEqual(len(export.clients), 1)
        self.assertEqual(export.clients[0].addresses, ["192.168.0.0/16"])
        self.assertIsNone(export.clients[0].squash)
        self.assertIsNone(export.clients[0].access_type)
        self.assertEqual(export.daemons, {"nodeb"})
        self.assertEqual(export.cluster_id, '_default_')

    def test_create_export_cephfs(self):
        ganesha.CephX = MagicMock()
        ganesha.CephX.list_clients.return_value = ["fs"]
        ganesha.CephX.get_client_key.return_value = "fs_key"

        ganesha.CephFS = MagicMock()
        ganesha.CephFS.dir_exists.return_value = True

        conf = GaneshaConf.instance('_default_')
        ex_id = conf.create_export({
            'daemons': ['nodea', 'nodeb'],
            'path': '/',
            'pseudo': '/cephfs2',
            'cluster_id': '_default_',
            'tag': None,
            'access_type': 'RW',
            'squash': 'all_squash',
            'security_label': True,
            'protocols': [4],
            'transports': ['TCP'],
            'clients': [],
            'fsal': {
                'name': 'CEPH',
                'user_id': 'fs',
                'fs_name': None,
                'sec_label_xattr': 'security.selinux'
            }
        })

        conf = GaneshaConf.instance('_default_')
        exports = conf.list_exports()
        self.assertEqual(len(exports), 3)
        export = conf.get_export(ex_id)
        self.assertEqual(export.export_id, ex_id)
        self.assertEqual(export.path, "/")
        self.assertEqual(export.pseudo, "/cephfs2")
        self.assertIsNone(export.tag)
        self.assertEqual(export.access_type, "RW")
        self.assertEqual(export.squash, "all_squash")
        self.assertEqual(export.protocols, {4})
        self.assertEqual(export.transports, {"TCP"})
        self.assertEqual(export.fsal.name, "CEPH")
        self.assertEqual(export.fsal.user_id, "fs")
        self.assertEqual(export.fsal.cephx_key, "fs_key")
        self.assertEqual(export.fsal.sec_label_xattr, "security.selinux")
        self.assertIsNone(export.fsal.fs_name)
        self.assertEqual(len(export.clients), 0)
        self.assertEqual(export.daemons, {"nodeb", "nodea"})
        self.assertEqual(export.cluster_id, '_default_')
        self.assertEqual(export.attr_expiration_time, 0)
        self.assertEqual(export.security_label, True)
