# -*- coding: utf-8 -*-
# pylint: disable=too-many-lines
from __future__ import absolute_import

import unittest
from unittest.mock import MagicMock, Mock, patch
from urllib.parse import urlencode

from ceph.deployment.service_spec import NFSServiceSpec
from orchestrator import DaemonDescription, ServiceDescription

from .. import mgr
from ..controllers.nfsganesha import NFSGaneshaUi
from ..services import ganesha
from ..services.ganesha import ClusterType, Export, GaneshaConf, GaneshaConfParser, NFSException
from ..settings import Settings
from . import ControllerTestCase  # pylint: disable=no-name-in-module
from . import KVStoreMockMixin  # pylint: disable=no-name-in-module


class GaneshaConfTest(unittest.TestCase, KVStoreMockMixin):
    daemon_raw_config = """
NFS_CORE_PARAM {
            Enable_NLM = false;
            Enable_RQUOTA = false;
            Protocols = 4;
            NFS_Port = 14000;
        }

        MDCACHE {
           Dir_Chunk = 0;
        }

        NFSv4 {
           RecoveryBackend = rados_cluster;
           Minor_Versions = 1, 2;
        }

        RADOS_KV {
           pool = nfs-ganesha;
           namespace = vstart;
           UserId = vstart;
           nodeid = a;
        }

        RADOS_URLS {
       Userid = vstart;
       watch_url = 'rados://nfs-ganesha/vstart/conf-nfs.vstart';
        }

    %url rados://nfs-ganesha/vstart/conf-nfs.vstart
"""

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
%url "rados://ganesha/ns/export-2"

%url "rados://ganesha/ns/export-1"'''

    conf_nodeb = '%url "rados://ganesha/ns/export-1"'

    conf_nfs_foo = '''
%url "rados://ganesha2/ns2/export-1"

%url "rados://ganesha2/ns2/export-2"'''

    class RObject(object):
        def __init__(self, key, raw):
            self.key = key
            self.raw = raw

        def read(self, _):
            return self.raw.encode('utf-8')

        def stat(self):
            return len(self.raw), None

    def _ioctx_write_full_mock(self, key, content):
        if key not in self.temp_store[self.temp_store_namespace]:
            self.temp_store[self.temp_store_namespace][key] = \
                GaneshaConfTest.RObject(key, content.decode('utf-8'))
        else:
            self.temp_store[self.temp_store_namespace][key].raw = content.decode('utf-8')

    def _ioctx_remove_mock(self, key):
        del self.temp_store[self.temp_store_namespace][key]

    def _ioctx_list_objects_mock(self):
        return [obj for _, obj in self.temp_store[self.temp_store_namespace].items()]

    def _ioctl_stat_mock(self, key):
        return self.temp_store[self.temp_store_namespace][key].stat()

    def _ioctl_read_mock(self, key, size):
        return self.temp_store[self.temp_store_namespace][key].read(size)

    def _ioctx_set_namespace_mock(self, namespace):
        self.temp_store_namespace = namespace

    def setUp(self):
        self.mock_kv_store()

        self.clusters = {
            '_default_': {
                'pool': 'ganesha',
                'namespace': 'ns',
                'type': ClusterType.USER,
                'daemon_conf': None,
                'daemons': ['nodea', 'nodeb'],
                'exports': {
                    1: ['nodea', 'nodeb'],
                    2: ['nodea'],
                    3: ['nodea', 'nodeb']  # for new-added export
                }
            },
            'foo': {
                'pool': 'ganesha2',
                'namespace': 'ns2',
                'type': ClusterType.ORCHESTRATOR,
                'daemon_conf': 'conf-nfs.foo',
                'daemons': ['foo.host_a', 'foo.host_b'],
                'exports': {
                    1: ['foo.host_a', 'foo.host_b'],
                    2: ['foo.host_a', 'foo.host_b'],
                    3: ['foo.host_a', 'foo.host_b']  # for new-added export
                }
            }
        }

        Settings.GANESHA_CLUSTERS_RADOS_POOL_NAMESPACE = '{pool}/{namespace}'.format_map(
            self.clusters['_default_'])

        self.temp_store_namespace = None
        self._reset_temp_store()

        self.io_mock = MagicMock()
        self.io_mock.set_namespace.side_effect = self._ioctx_set_namespace_mock
        self.io_mock.read = self._ioctl_read_mock
        self.io_mock.stat = self._ioctl_stat_mock
        self.io_mock.list_objects.side_effect = self._ioctx_list_objects_mock
        self.io_mock.write_full.side_effect = self._ioctx_write_full_mock
        self.io_mock.remove_object.side_effect = self._ioctx_remove_mock

        ioctx_mock = MagicMock()
        ioctx_mock.__enter__ = Mock(return_value=(self.io_mock))
        ioctx_mock.__exit__ = Mock(return_value=None)

        mgr.rados = MagicMock()
        mgr.rados.open_ioctx.return_value = ioctx_mock

        self._mock_orchestrator(True)

        ganesha.CephX = MagicMock()
        ganesha.CephX.list_clients.return_value = ['ganesha']
        ganesha.CephX.get_client_key.return_value = 'ganesha'

        ganesha.CephFS = MagicMock()

    def _reset_temp_store(self):
        self.temp_store_namespace = None
        self.temp_store = {
            'ns': {
                'export-1': GaneshaConfTest.RObject("export-1", self.export_1),
                'export-2': GaneshaConfTest.RObject("export-2", self.export_2),
                'conf-nodea': GaneshaConfTest.RObject("conf-nodea", self.conf_nodea),
                'conf-nodeb': GaneshaConfTest.RObject("conf-nodeb", self.conf_nodeb),
            },
            'ns2': {
                'export-1': GaneshaConfTest.RObject("export-1", self.export_1),
                'export-2': GaneshaConfTest.RObject("export-2", self.export_2),
                'conf-nfs.foo': GaneshaConfTest.RObject("conf-nfs.foo", self.conf_nfs_foo)
            }
        }

    def _mock_orchestrator(self, enable):
        # mock nfs services
        cluster_info = self.clusters['foo']
        orch_nfs_services = [
            ServiceDescription(spec=NFSServiceSpec(service_id='foo',
                                                   pool=cluster_info['pool'],
                                                   namespace=cluster_info['namespace']))
        ] if enable else []
        # pylint: disable=protected-access
        ganesha.Ganesha._get_orch_nfs_services = Mock(return_value=orch_nfs_services)

        # mock nfs daemons
        def _get_nfs_instances(service_name=None):
            if not enable:
                return []
            instances = {
                'nfs.foo': [
                    DaemonDescription(daemon_id='foo.host_a', status=1),
                    DaemonDescription(daemon_id='foo.host_b', status=1)
                ],
                'nfs.bar': [
                    DaemonDescription(daemon_id='bar.host_c', status=1)
                ]
            }
            if service_name is not None:
                return instances[service_name]
            result = []
            for _, daemons in instances.items():
                result.extend(daemons)
            return result
        ganesha.GaneshaConfOrchestrator._get_orch_nfs_instances = Mock(
            side_effect=_get_nfs_instances)

    def test_parse_daemon_raw_config(self):
        expected_daemon_config = [
            {
                "block_name": "NFS_CORE_PARAM",
                "enable_nlm": False,
                "enable_rquota": False,
                "protocols": 4,
                "nfs_port": 14000
            },
            {
                "block_name": "MDCACHE",
                "dir_chunk": 0
            },
            {
                "block_name": "NFSV4",
                "recoverybackend": "rados_cluster",
                "minor_versions": [1, 2]
            },
            {
                "block_name": "RADOS_KV",
                "pool": "nfs-ganesha",
                "namespace": "vstart",
                "userid": "vstart",
                "nodeid": "a"
            },
            {
                "block_name": "RADOS_URLS",
                "userid": "vstart",
                "watch_url": "'rados://nfs-ganesha/vstart/conf-nfs.vstart'"
            },
            {
                "block_name": "%url",
                "value": "rados://nfs-ganesha/vstart/conf-nfs.vstart"
            }
        ]
        daemon_config = GaneshaConfParser(self.daemon_raw_config).parse()
        self.assertEqual(daemon_config, expected_daemon_config)

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
        for cluster_id, info in self.clusters.items():
            self._do_test_ganesha_conf(cluster_id, info['exports'])
            self._reset_temp_store()

    def _do_test_ganesha_conf(self, cluster, expected_exports):
        ganesha_conf = GaneshaConf.instance(cluster)
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
        self.assertSetEqual(export.daemons, set(expected_exports[1]))

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
        self.assertSetEqual(export.daemons, set(expected_exports[2]))

    def test_config_dict(self):
        for cluster_id, info in self.clusters.items():
            self._do_test_config_dict(cluster_id, info['exports'])
            self._reset_temp_store()

    def _do_test_config_dict(self, cluster, expected_exports):
        conf = GaneshaConf.instance(cluster)
        export = conf.exports[1]
        ex_dict = export.to_dict()
        self.assertDictEqual(ex_dict, {
            'daemons': expected_exports[1],
            'export_id': 1,
            'path': '/',
            'pseudo': '/cephfs_a',
            'cluster_id': cluster,
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
            'daemons': expected_exports[2],
            'export_id': 2,
            'path': '/',
            'pseudo': '/rgw',
            'cluster_id': cluster,
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
        for cluster_id, info in self.clusters.items():
            self._do_test_config_from_dict(cluster_id, info['exports'])
            self._reset_temp_store()

    def _do_test_config_from_dict(self, cluster_id, expected_exports):
        export = Export.from_dict(1, {
            'daemons': expected_exports[1],
            'export_id': 1,
            'path': '/',
            'cluster_id': cluster_id,
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
        self.assertEqual(export.daemons, set(expected_exports[1]))
        self.assertEqual(export.cluster_id, cluster_id)
        self.assertEqual(export.attr_expiration_time, 0)
        self.assertEqual(export.security_label, True)

        export = Export.from_dict(2, {
            'daemons': expected_exports[2],
            'export_id': 2,
            'path': '/',
            'pseudo': '/rgw',
            'cluster_id': cluster_id,
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
        self.assertEqual(export.daemons, set(expected_exports[2]))
        self.assertEqual(export.cluster_id, cluster_id)

    def test_gen_raw_config(self):
        for cluster_id, info in self.clusters.items():
            self._do_test_gen_raw_config(cluster_id, info['exports'])
            self._reset_temp_store()

    def _do_test_gen_raw_config(self, cluster_id, expected_exports):
        conf = GaneshaConf.instance(cluster_id)
        # pylint: disable=W0212
        export = conf.exports[1]
        del conf.exports[1]
        conf._save_export(export)
        conf = GaneshaConf.instance(cluster_id)
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
        self.assertEqual(export.daemons, set(expected_exports[1]))
        self.assertEqual(export.cluster_id, cluster_id)
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
        self.assertEqual(export.daemons, set(expected_exports[2]))
        self.assertEqual(export.cluster_id, cluster_id)

    def test_update_export(self):
        for cluster_id, info in self.clusters.items():
            self._do_test_update_export(cluster_id, info['exports'])
            self._reset_temp_store()

    def _do_test_update_export(self, cluster_id, expected_exports):
        ganesha.RgwClient = MagicMock()
        admin_inst_mock = MagicMock()
        admin_inst_mock.get_user_keys.return_value = {
            'access_key': 'access_key',
            'secret_key': 'secret_key'
        }
        ganesha.RgwClient.admin_instance.return_value = admin_inst_mock

        conf = GaneshaConf.instance(cluster_id)
        conf.update_export({
            'export_id': 2,
            'daemons': expected_exports[2],
            'path': 'bucket',
            'pseudo': '/rgw/bucket',
            'cluster_id': cluster_id,
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

        conf = GaneshaConf.instance(cluster_id)
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
        self.assertEqual(export.daemons, set(expected_exports[2]))
        self.assertEqual(export.cluster_id, cluster_id)

    def test_remove_export(self):
        for cluster_id, info in self.clusters.items():
            self._do_test_remove_export(cluster_id, info['exports'])
            self._reset_temp_store()

    def _do_test_remove_export(self, cluster_id, expected_exports):
        conf = GaneshaConf.instance(cluster_id)
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
        self.assertEqual(export.daemons, set(expected_exports[2]))
        self.assertEqual(export.cluster_id, cluster_id)

    def test_create_export_rgw(self):
        for cluster_id, info in self.clusters.items():
            self._do_test_create_export_rgw(cluster_id, info['exports'])
            self._reset_temp_store()

    def _do_test_create_export_rgw(self, cluster_id, expected_exports):
        ganesha.RgwClient = MagicMock()
        admin_inst_mock = MagicMock()
        admin_inst_mock.get_user_keys.return_value = {
            'access_key': 'access_key2',
            'secret_key': 'secret_key2'
        }
        ganesha.RgwClient.admin_instance.return_value = admin_inst_mock

        conf = GaneshaConf.instance(cluster_id)
        ex_id = conf.create_export({
            'daemons': expected_exports[3],
            'path': 'bucket',
            'pseudo': '/rgw/bucket',
            'tag': 'bucket_tag',
            'cluster_id': cluster_id,
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

        conf = GaneshaConf.instance(cluster_id)
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
        self.assertEqual(export.daemons, set(expected_exports[3]))
        self.assertEqual(export.cluster_id, cluster_id)

    def test_create_export_cephfs(self):
        for cluster_id, info in self.clusters.items():
            self._do_test_create_export_cephfs(cluster_id, info['exports'])
            self._reset_temp_store()

    def _do_test_create_export_cephfs(self, cluster_id, expected_exports):
        ganesha.CephX = MagicMock()
        ganesha.CephX.list_clients.return_value = ["fs"]
        ganesha.CephX.get_client_key.return_value = "fs_key"

        ganesha.CephFS = MagicMock()
        ganesha.CephFS.dir_exists.return_value = True

        conf = GaneshaConf.instance(cluster_id)
        ex_id = conf.create_export({
            'daemons': expected_exports[3],
            'path': '/',
            'pseudo': '/cephfs2',
            'cluster_id': cluster_id,
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

        conf = GaneshaConf.instance(cluster_id)
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
        self.assertEqual(export.daemons, set(expected_exports[3]))
        self.assertEqual(export.cluster_id, cluster_id)
        self.assertEqual(export.attr_expiration_time, 0)
        self.assertEqual(export.security_label, True)

    def test_reload_daemons(self):
        # Fail to import call in Python 3.8, see https://bugs.python.org/issue35753
        mock_call = unittest.mock.call

        # Orchestrator cluster: reload all daemon config objects.
        conf = GaneshaConf.instance('foo')
        calls = [mock_call(conf) for conf in conf.list_daemon_confs()]
        for daemons in [[], ['a', 'b']]:
            conf.reload_daemons(daemons)
            self.io_mock.notify.assert_has_calls(calls)
            self.io_mock.reset_mock()

        # User-defined cluster: reload daemons in the parameter
        conf = GaneshaConf.instance('_default_')
        calls = [mock_call('conf-{}'.format(daemon)) for daemon in ['nodea', 'nodeb']]
        conf.reload_daemons(['nodea', 'nodeb'])
        self.io_mock.notify.assert_has_calls(calls)

    def test_list_daemons(self):
        for cluster_id, info in self.clusters.items():
            instance = GaneshaConf.instance(cluster_id)
            daemons = instance.list_daemons()
            for daemon in daemons:
                self.assertEqual(daemon['cluster_id'], cluster_id)
                self.assertEqual(daemon['cluster_type'], info['type'])
                self.assertIn('daemon_id', daemon)
                self.assertIn('status', daemon)
                self.assertIn('status_desc', daemon)
            self.assertEqual([daemon['daemon_id'] for daemon in daemons], info['daemons'])

    def test_validate_orchestrator(self):
        cluster_id = 'foo'
        cluster_info = self.clusters[cluster_id]
        instance = GaneshaConf.instance(cluster_id)
        export = MagicMock()

        # export can be linked to none or all daemons
        export_daemons = [[], cluster_info['daemons']]
        for daemons in export_daemons:
            export.daemons = daemons
            instance.validate(export)

        # raise if linking to partial or non-exist daemons
        export_daemons = [cluster_info['daemons'][:1], 'xxx']
        for daemons in export_daemons:
            with self.assertRaises(NFSException):
                export.daemons = daemons
                instance.validate(export)

    def test_validate_user(self):
        cluster_id = '_default_'
        cluster_info = self.clusters[cluster_id]
        instance = GaneshaConf.instance(cluster_id)
        export = MagicMock()

        # export can be linked to none, partial, or all daemons
        export_daemons = [[], cluster_info['daemons'][:1], cluster_info['daemons']]
        for daemons in export_daemons:
            export.daemons = daemons
            instance.validate(export)

        # raise if linking to non-exist daemons
        export_daemons = ['xxx']
        for daemons in export_daemons:
            with self.assertRaises(NFSException):
                export.daemons = daemons
                instance.validate(export)

    def _verify_locations(self, locations, cluster_ids):
        for cluster_id in cluster_ids:
            self.assertIn(cluster_id, locations)
            cluster = locations.pop(cluster_id)
            expected_info = self.clusters[cluster_id]
            self.assertDictEqual(cluster, {key: expected_info[key] for key in [
                'pool', 'namespace', 'type', 'daemon_conf']})
        self.assertDictEqual(locations, {})

    def test_get_cluster_locations(self):
        # pylint: disable=protected-access
        # There are both Orchstrator cluster and user-defined cluster.
        locations = ganesha.Ganesha._get_clusters_locations()
        self._verify_locations(locations, self.clusters.keys())

        # There is only a user-defined cluster.
        self._mock_orchestrator(False)
        locations = ganesha.Ganesha._get_clusters_locations()
        self._verify_locations(locations, ['_default_'])
        self._mock_orchestrator(True)

        # There is only a Orchstrator cluster.
        Settings.GANESHA_CLUSTERS_RADOS_POOL_NAMESPACE = ''
        locations = ganesha.Ganesha._get_clusters_locations()
        self._verify_locations(locations, ['foo'])

        # No cluster.
        self._mock_orchestrator(False)
        with self.assertRaises(NFSException):
            ganesha.Ganesha._get_clusters_locations()

    def test_get_cluster_locations_conflict(self):
        # pylint: disable=protected-access
        # Raise an exception when there is a user-defined cluster that conlicts
        # with Orchestrator clusters.
        old_setting = Settings.GANESHA_CLUSTERS_RADOS_POOL_NAMESPACE
        conflicted_location = ',foo:{pool}/{namespace}'.format_map(
            self.clusters['foo'])
        Settings.GANESHA_CLUSTERS_RADOS_POOL_NAMESPACE = old_setting + conflicted_location
        with self.assertRaises(NFSException):
            ganesha.Ganesha._get_clusters_locations()


class NFSGaneshaUiControllerTest(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        # pylint: disable=protected-access
        NFSGaneshaUi._cp_config['tools.authenticate.on'] = False
        cls.setup_controllers([NFSGaneshaUi])

    @classmethod
    def _create_ls_dir_url(cls, fs_name, query_params):
        api_url = '/ui-api/nfs-ganesha/lsdir/{}'.format(fs_name)
        if query_params is not None:
            return '{}?{}'.format(api_url, urlencode(query_params))
        return api_url

    @patch('dashboard.controllers.nfsganesha.CephFS')
    def test_lsdir(self, cephfs_class):
        cephfs_class.return_value.ls_dir.return_value = [
            {'path': '/foo'},
            {'path': '/foo/bar'}
        ]
        mocked_ls_dir = cephfs_class.return_value.ls_dir

        reqs = [
            {
                'params': None,
                'cephfs_ls_dir_args': ['/', 1],
                'path0': '/',
                'status': 200
            },
            {
                'params': {'root_dir': '/', 'depth': '1'},
                'cephfs_ls_dir_args': ['/', 1],
                'path0': '/',
                'status': 200
            },
            {
                'params': {'root_dir': '', 'depth': '1'},
                'cephfs_ls_dir_args': ['/', 1],
                'path0': '/',
                'status': 200
            },
            {
                'params': {'root_dir': '/foo', 'depth': '3'},
                'cephfs_ls_dir_args': ['/foo', 3],
                'path0': '/foo',
                'status': 200
            },
            {
                'params': {'root_dir': 'foo', 'depth': '6'},
                'cephfs_ls_dir_args': ['/foo', 5],
                'path0': '/foo',
                'status': 200
            },
            {
                'params': {'root_dir': '/', 'depth': '-1'},
                'status': 400
            },
            {
                'params': {'root_dir': '/', 'depth': 'abc'},
                'status': 400
            }
        ]

        for req in reqs:
            self._get(self._create_ls_dir_url('a', req['params']))
            self.assertStatus(req['status'])

            # Returned paths should contain root_dir as first element
            if req['status'] == 200:
                paths = self.json_body()['paths']
                self.assertEqual(paths[0], req['path0'])
                cephfs_class.assert_called_once_with('a')

            # Check the arguments passed to `CephFS.ls_dir`.
            if req.get('cephfs_ls_dir_args'):
                mocked_ls_dir.assert_called_once_with(*req['cephfs_ls_dir_args'])
            else:
                mocked_ls_dir.assert_not_called()
            mocked_ls_dir.reset_mock()
            cephfs_class.reset_mock()

    @patch('dashboard.controllers.nfsganesha.cephfs')
    @patch('dashboard.controllers.nfsganesha.CephFS')
    def test_lsdir_non_existed_dir(self, cephfs_class, cephfs):
        cephfs.ObjectNotFound = Exception
        cephfs.PermissionError = Exception
        cephfs_class.return_value.ls_dir.side_effect = cephfs.ObjectNotFound()
        self._get(self._create_ls_dir_url('a', {'root_dir': '/foo', 'depth': '3'}))
        cephfs_class.assert_called_once_with('a')
        cephfs_class.return_value.ls_dir.assert_called_once_with('/foo', 3)
        self.assertStatus(200)
        self.assertJsonBody({'paths': []})
