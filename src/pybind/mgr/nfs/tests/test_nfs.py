# flake8: noqa
import json
import pytest
from typing import Dict, Optional, Tuple, Iterator, List, Any

from contextlib import contextmanager
from unittest import mock
from unittest.mock import MagicMock
import mgr_util
from mgr_module import MgrModule, NFS_POOL_NAME

from rados import ObjectNotFound

from ceph.deployment.service_spec import NFSServiceSpec, PlacementSpec
from ceph.utils import with_units_to_int, bytes_to_human
from nfs import Module
from nfs.export import ExportMgr, normalize_path
from nfs.utils import cephfs_client_for_mgr
from nfs.qos_conf import (
    RawBlock,
    QOS,
    QOSType,
    QOSParams,
    QOS_REQ_BW_PARAMS,
    QOSBandwidthControl,
    QOSOpsControl,
    QOS_REQ_OPS_PARAMS)
from nfs.ganesha_conf import GaneshaConfParser, Export, RawBlock, format_block
from nfs.cluster import NFSCluster
from orchestrator import ServiceDescription, DaemonDescription, OrchResult


class TestNFS:
    cluster_id = "foo"
    export_1 = """
EXPORT {
    Export_ID=1;
    Protocols = 4;
    Path = /;
    Pseudo = /cephfs_a/;
    Access_Type = RW;
    Protocols = 4;
    Attr_Expiration_Time = 0;
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
        User_Id = "nfs.foo.bucket";
        Access_Key_Id ="the_access_key";
        Secret_Access_Key = "the_secret_key";
    }
}
"""
    export_3 = """
EXPORT {
    FSAL {
        name = "CEPH";
        filesystem = "a";
        cmount_path = "/";
    }
    export_id = 1;
    path = "/";
    pseudo = "/a";
    access_type = "RW";
    squash = "none";
    attr_expiration_time = 0;
    security_label = true;
    protocols = 4;
    transports = "TCP";
}
"""
    export_4 = """
EXPORT {
    FSAL {
        name = "CEPH";
        filesystem = "a";
        cmount_path = "/";
    }
    export_id = 1;
    path = "/secure/me";
    pseudo = "/secure1";
    access_type = "RW";
    squash = "no_root_squash";
    SecType = "krb5p", "krb5i";
    attr_expiration_time = 0;
    security_label = true;
    protocols = 4;
    transports = "TCP";
}
"""
    export_5 = """
EXPORT {
    Export_ID=3;
    Protocols = 4;
    Path = /;
    Pseudo = /cephfs_b/;
    Access_Type = RW;
    Protocols = 3, 4;
    Attr_Expiration_Time = 0;

    FSAL {
        Name = CEPH;
        Filesystem = "b";
        User_Id = "nfs.foo.b.lgudhr";
        Secret_Access_Key = "YOUR SECRET KEY HERE";
        cmount_path = "/";
    }
}
"""

    conf_nfs_foo = f'''
%url "rados://{NFS_POOL_NAME}/{cluster_id}/export-1"

%url "rados://{NFS_POOL_NAME}/{cluster_id}/export-2"'''

    qos_cluster_block = """
QOS {
    enable_qos = true;
    enable_bw_control = true;
    combined_rw_bw_control = false;
    qos_type = "Per_Export_Per_Client";
    max_export_write_bw = 2000000;
    max_export_read_bw = 2000000;
    max_client_write_bw = 3000000;
    max_client_read_bw = 4000000;
    max_export_combined_bw = 0;
    max_client_combined_bw = 0;
}
"""

    qos_export_block = """
QOS_BLOCK {
    enable_qos = true;
    enable_bw_control = true;
    combined_rw_bw_control = false;
    max_export_write_bw = 2000000;
    max_export_read_bw = 2000000;
    max_client_write_bw = 3000000;
    max_client_read_bw = 4000000;
    max_export_combined_bw = 0;
    max_client_combined_bw = 0;

}
"""

    qos_cluster_dict = {
        "enable_bw_control": True,
        "enable_qos": True,
        "combined_rw_bw_control": False,
        "max_client_read_bw": bytes_to_human(4000000, mode='binary'),
        "max_client_write_bw": bytes_to_human(3000000, mode='binary'),
        "max_export_read_bw": bytes_to_human(2000000, mode='binary'),
        "max_export_write_bw": bytes_to_human(2000000, mode='binary'),
        "qos_type": "PerShare_PerClient",
        "enable_iops_control": False
    }

    qos_cluster_dict_bw_in_bytes = {
        "enable_bw_control": True,
        "enable_qos": True,
        "combined_rw_bw_control": False,
        "max_client_read_bw": "4000000",
        "max_client_write_bw": "3000000",
        "max_export_read_bw": "2000000",
        "max_export_write_bw": "2000000",
        "qos_type": "PerShare_PerClient",
        "enable_iops_control": False
    }

    qos_export_dict = {
        "enable_bw_control": True,
        "enable_qos": True,
        "combined_rw_bw_control": False,
        "max_client_read_bw": bytes_to_human(4000000, mode='binary'),
        "max_client_write_bw": bytes_to_human(3000000, mode='binary'),
        "max_export_read_bw": bytes_to_human(2000000, mode='binary'),
        "max_export_write_bw": bytes_to_human(2000000, mode='binary'),
        "enable_iops_control": False
    }
    qos_export_dict_bw_in_bytes = {
        "enable_bw_control": True,
        "enable_qos": True,
        "combined_rw_bw_control": False,
        "max_client_read_bw": "4000000",
        "max_client_write_bw": "3000000",
        "max_export_read_bw": "2000000",
        "max_export_write_bw": "2000000",
        "enable_iops_control": False
    }

    class RObject(object):
        def __init__(self, key: str, raw: str) -> None:
            self.key = key
            self.raw = raw

        def read(self, length: Optional[int] = None, offset: int = 0) -> bytes:
            raw = self.raw.encode('utf-8')
            if length is None:
                return raw[offset:]
            return raw[offset:offset + length]

        def stat(self) -> Tuple[int, None]:
            return len(self.raw), None

    def _ioctx_write_full_mock(self, key: str, content: bytes) -> None:
        if key not in self.temp_store[self.temp_store_namespace]:
            self.temp_store[self.temp_store_namespace][key] = \
                TestNFS.RObject(key, content.decode('utf-8'))
        else:
            self.temp_store[self.temp_store_namespace][key].raw = content.decode('utf-8')

    def _ioctx_append_mock(self, key: str, content: bytes) -> None:
        if key not in self.temp_store[self.temp_store_namespace]:
            self.temp_store[self.temp_store_namespace][key] = \
                TestNFS.RObject(key, content.decode('utf-8'))
        else:
            self.temp_store[self.temp_store_namespace][key].raw += content.decode('utf-8')

    def _ioctx_remove_mock(self, key: str) -> None:
        del self.temp_store[self.temp_store_namespace][key]

    def _ioctx_list_objects_mock(self) -> List['TestNFS.RObject']:
        r = [obj for _, obj in self.temp_store[self.temp_store_namespace].items()]
        return r

    def _ioctl_stat_mock(self, key):
        if key not in self.temp_store[self.temp_store_namespace]:
            raise ObjectNotFound
        return self.temp_store[self.temp_store_namespace][key].stat()

    def _ioctl_read_mock(self, key: str, length: int = 8192, offset: int = 0) -> bytes:
        # Truncating reads the way rados does: a caller that asks for a fixed
        # amount only gets that much back.
        if key not in self.temp_store[self.temp_store_namespace]:
            raise ObjectNotFound
        return self.temp_store[self.temp_store_namespace][key].read(length, offset)

    def _ioctx_set_namespace_mock(self, namespace: str) -> None:
        self.temp_store_namespace = namespace

    def _reset_temp_store(self) -> None:
        self.temp_store_namespace = None
        self.temp_store = {
            'foo': {
                'export-1': TestNFS.RObject("export-1", self.export_1),
                'export-2': TestNFS.RObject("export-2", self.export_2),
                'export-3': TestNFS.RObject("export-3", self.export_5),
                'conf-nfs.foo': TestNFS.RObject("conf-nfs.foo", self.conf_nfs_foo)
            }
        }

    @contextmanager
    def _mock_orchestrator(self, enable: bool, clients_per_pool: Optional[int] = None) -> Iterator:
        self.io_mock = MagicMock()
        self.io_mock.set_namespace.side_effect = self._ioctx_set_namespace_mock
        self.io_mock.read = self._ioctl_read_mock
        self.io_mock.stat = self._ioctl_stat_mock
        self.io_mock.list_objects.side_effect = self._ioctx_list_objects_mock
        self.io_mock.write_full.side_effect = self._ioctx_write_full_mock
        self.io_mock.remove_object.side_effect = self._ioctx_remove_mock
        self.io_mock.append.side_effect = self._ioctx_append_mock

        # mock nfs services
        orch_nfs_services = [
            ServiceDescription(spec=NFSServiceSpec(
                service_id=self.cluster_id,
                clients_per_pool=clients_per_pool,
            ))
        ] if enable else []

        orch_nfs_daemons = [
            DaemonDescription('nfs', 'foo.mydaemon', 'myhostname')
        ] if enable else []

        def mock_exec(cls, args):
            if args[1:3] == ['bucket', 'stats']:
                bucket_info = {
                    "owner": "bucket_owner_user",
                }
                return 0, json.dumps(bucket_info), ''
            u = {
                "user_id": "abc",
                "display_name": "foo",
                "email": "",
                "suspended": 0,
                "max_buckets": 1000,
                "subusers": [],
                "keys": [
                    {
                        "user": "abc",
                        "access_key": "the_access_key",
                        "secret_key": "the_secret_key"
                    }
                ],
                "swift_keys": [],
                "caps": [],
                "op_mask": "read, write, delete",
                "default_placement": "",
                "default_storage_class": "",
                "placement_tags": [],
                "bucket_quota": {
                    "enabled": False,
                    "check_on_raw": False,
                    "max_size": -1,
                    "max_size_kb": 0,
                    "max_objects": -1
                },
                "user_quota": {
                    "enabled": False,
                    "check_on_raw": False,
                    "max_size": -1,
                    "max_size_kb": 0,
                    "max_objects": -1
                },
                "temp_url_keys": [],
                "type": "rgw",
                "mfa_ids": []
            }
            if args[2] == 'list':
                return 0, json.dumps([u]), ''
            return 0, json.dumps(u), ''

        def mock_describe_service(cls, *args, **kwargs):
            if kwargs['service_type'] == 'nfs':
                return OrchResult(orch_nfs_services)
            return OrchResult([])

        def mock_list_daemons(cls, *args, **kwargs):
            if kwargs['daemon_type'] == 'nfs':
                return OrchResult(orch_nfs_daemons)
            return OrchResult([])

        with mock.patch('nfs.module.Module.describe_service', mock_describe_service) as describe_service, \
            mock.patch('nfs.module.Module.list_daemons', mock_list_daemons) as list_daemons, \
                mock.patch('nfs.module.Module.rados') as rados, \
                mock.patch('nfs.export.available_clusters',
                           return_value=[self.cluster_id]), \
                mock.patch('nfs.export.restart_nfs_service'), \
                mock.patch('nfs.cluster.restart_nfs_service'), \
                mock.patch.object(MgrModule, 'tool_exec', mock_exec), \
                mock.patch('nfs.export.check_fs', return_value=True), \
                mock.patch('nfs.ganesha_conf.check_fs', return_value=True), \
                mock.patch('nfs.export.ExportMgr._create_user_key',
                           return_value='thekeyforclientabc'), \
                mock.patch('nfs.export.cephfs_path_is_dir'):

            rados.open_ioctx.return_value.__enter__.return_value = self.io_mock
            rados.open_ioctx.return_value.__exit__ = mock.Mock(return_value=None)

            self._reset_temp_store()

            yield

    def test_parse_daemon_raw_config(self) -> None:
        expected_daemon_config = [
            RawBlock('NFS_CORE_PARAM', values={
                "enable_nlm": False,
                "enable_rquota": False,
                "protocols": 4,
                "nfs_port": 14000
            }),
            RawBlock('MDCACHE', values={
                "dir_chunk": 0
            }),
            RawBlock('NFSV4', values={
                "recoverybackend": "rados_cluster"
            }),
            RawBlock('RADOS_KV', values={
                "pool": NFS_POOL_NAME,
                "namespace": "vstart",
                "userid": "vstart",
                "nodeid": "a"
            }),
            RawBlock('RADOS_URLS', values={
                "userid": "vstart",
                "watch_url": f"'rados://{NFS_POOL_NAME}/vstart/conf-nfs.vstart'"
            }),
            RawBlock('%url', values={
                "value": f"rados://{NFS_POOL_NAME}/vstart/conf-nfs.vstart"
            })
        ]
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
        }

        RADOS_KV {
           pool = {};
           namespace = vstart;
           UserId = vstart;
           nodeid = a;
        }

        RADOS_URLS {
       Userid = vstart;
       watch_url = 'rados://{}/vstart/conf-nfs.vstart';
        }

    %url rados://{}/vstart/conf-nfs.vstart
""".replace('{}', NFS_POOL_NAME)
        daemon_config = GaneshaConfParser(daemon_raw_config).parse()
        assert daemon_config == expected_daemon_config

    def _validate_export_1(self, export: Export):
        assert export.export_id == 1
        assert export.path == "/"
        assert export.pseudo == "/cephfs_a/"
        assert export.access_type == "RW"
        # assert export.squash == "root_squash"  # probably correct value
        assert export.squash == "no_root_squash"
        assert export.protocols == [4]
        #        assert export.transports == {"TCP", "UDP"}
        assert export.fsal.name == "CEPH"
        assert export.fsal.user_id == "ganesha"
        assert export.fsal.fs_name == "a"
        assert export.fsal.sec_label_xattr == None
        assert len(export.clients) == 2
        assert export.clients[0].addresses == \
            ["192.168.0.10", "192.168.1.0/8"]
        # assert export.clients[0].squash ==  "no_root_squash"  # probably correct value
        assert export.clients[0].squash == "None"
        assert export.clients[0].access_type is None
        assert export.clients[1].addresses == ["192.168.0.0/16"]
        # assert export.clients[1].squash ==  "all_squash"  # probably correct value
        assert export.clients[1].squash == "All"
        assert export.clients[1].access_type == "RO"
        assert export.cluster_id == 'foo'
        assert export.attr_expiration_time == 0
        # assert export.security_label == False  # probably correct value
        assert export.security_label == True

    def test_export_parser_1(self) -> None:
        blocks = GaneshaConfParser(self.export_1).parse()
        assert isinstance(blocks, list)
        assert len(blocks) == 1
        export = Export.from_export_block(blocks[0], self.cluster_id)
        self._validate_export_1(export)

    def _validate_export_2(self, export: Export):
        assert export.export_id == 2
        assert export.path == "/"
        assert export.pseudo == "/rgw"
        assert export.access_type == "RW"
        # assert export.squash == "all_squash"  # probably correct value
        assert export.squash == "AllAnonymous"
        assert export.protocols == [4, 3]
        assert set(export.transports) == {"TCP", "UDP"}
        assert export.fsal.name == "RGW"
        assert export.fsal.user_id == "nfs.foo.bucket"
        assert export.fsal.access_key_id == "the_access_key"
        assert export.fsal.secret_access_key == "the_secret_key"
        assert len(export.clients) == 0
        assert export.cluster_id == 'foo'

    def test_export_parser_2(self) -> None:
        blocks = GaneshaConfParser(self.export_2).parse()
        assert isinstance(blocks, list)
        assert len(blocks) == 1
        export = Export.from_export_block(blocks[0], self.cluster_id)
        self._validate_export_2(export)

    def _validate_export_3(self, export: Export):
        assert export.export_id == 3
        assert export.path == "/"
        assert export.pseudo == "/cephfs_b/"
        assert export.access_type == "RW"
        assert export.squash == "no_root_squash"
        assert export.protocols == [3, 4]
        assert export.fsal.name == "CEPH"
        assert export.fsal.user_id == "nfs.foo.b.lgudhr"
        assert export.fsal.fs_name == "b"
        assert export.fsal.sec_label_xattr == None
        assert export.fsal.cmount_path == "/"
        assert export.cluster_id == 'foo'
        assert export.attr_expiration_time == 0
        assert export.security_label == True

    def test_export_parser_3(self) -> None:
        blocks = GaneshaConfParser(self.export_5).parse()
        assert isinstance(blocks, list)
        assert len(blocks) == 1
        export = Export.from_export_block(blocks[0], self.cluster_id)
        self._validate_export_3(export)

    def test_daemon_conf_parser(self) -> None:
        blocks = GaneshaConfParser(self.conf_nfs_foo).parse()
        assert isinstance(blocks, list)
        assert len(blocks) == 2
        assert blocks[0].block_name == "%url"
        assert blocks[0].values['value'] == f"rados://{NFS_POOL_NAME}/{self.cluster_id}/export-1"
        assert blocks[1].block_name == "%url"
        assert blocks[1].values['value'] == f"rados://{NFS_POOL_NAME}/{self.cluster_id}/export-2"

    def _do_mock_test(self, func, *args, clients_per_pool: Optional[int] = None) -> None:
        with self._mock_orchestrator(True, clients_per_pool=clients_per_pool):
            func(*args)
            self._reset_temp_store()

    @staticmethod
    def _create_key_mock(created: Optional[List] = None):
        """_create_user_key stand-in giving every entity a key of its own.

        Pass created to also record the (entity, path, fs_name) of each call.
        """
        def mock_create_key(cluster_id, entity, path, fs_name):
            if created is not None:
                created.append((entity, path, fs_name))
            return f'key-{entity}'
        return mock_create_key

    @staticmethod
    def _rotate_key_mock(failing_entity: Optional[str] = None):
        """_create_user_key stand-in returning a visibly rotated key.

        Patched onto the class, hence the leading self. Pass failing_entity to
        make the rotation of that one entity blow up.
        """
        def mock_create_user_key(self, cluster_id, entity_name, path, fs_name):
            if failing_entity is not None and entity_name == failing_entity:
                raise RuntimeError('mon is unavailable')
            return f'rotated-{entity_name}'
        return mock_create_user_key

    @staticmethod
    def _auth_rm_mock(rm_entities: List[str], prefix: str = ''):
        """check_mon_command stand-in recording the `auth rm` calls it sees."""
        def mock_check_mon_command(cmd):
            if cmd.get('prefix') == 'auth rm':
                rm_entities.append(f"{prefix}{cmd['entity']}")
            return 0, '', ''
        return mock_check_mon_command

    def test_ganesha_conf(self) -> None:
        self._do_mock_test(self._do_test_ganesha_conf)

    def _do_test_ganesha_conf(self) -> None:
        nfs_mod = Module('nfs', '', '')
        ganesha_conf = ExportMgr(nfs_mod)
        exports = ganesha_conf.exports[self.cluster_id]

        assert len(exports) == 3

        self._validate_export_1([e for e in exports if e.export_id == 1][0])
        self._validate_export_2([e for e in exports if e.export_id == 2][0])
        self._validate_export_3([e for e in exports if e.export_id == 3][0])

    def test_config_dict(self) -> None:
        self._do_mock_test(self._do_test_config_dict)

    def _do_test_config_dict(self) -> None:
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        export = [e for e in conf.exports['foo'] if e.export_id == 1][0]
        ex_dict = export.to_dict()

        assert ex_dict == {'access_type': 'RW',
                           'clients': [{'access_type': None,
                                        'addresses': ['192.168.0.10', '192.168.1.0/8'],
                                        'squash': 'None'},
                                       {'access_type': 'RO',
                                        'addresses': ['192.168.0.0/16'],
                                        'squash': 'All'}],
                           'cluster_id': self.cluster_id,
                           'export_id': 1,
                           'fsal': {'fs_name': 'a', 'name': 'CEPH', 'user_id': 'ganesha'},
                           'path': '/',
                           'protocols': [4],
                           'pseudo': '/cephfs_a/',
                           'security_label': True,
                           'squash': 'no_root_squash',
                           'transports': []}

        export = [e for e in conf.exports['foo'] if e.export_id == 2][0]
        ex_dict = export.to_dict()
        assert ex_dict == {'access_type': 'RW',
                           'clients': [],
                           'cluster_id': self.cluster_id,
                           'export_id': 2,
                           'fsal': {'name': 'RGW',
                                    'access_key_id': 'the_access_key',
                                    'secret_access_key': 'the_secret_key',
                                    'user_id': 'nfs.foo.bucket'},
                           'path': '/',
                           'protocols': [3, 4],
                           'pseudo': '/rgw',
                           'security_label': True,
                           'squash': 'AllAnonymous',
                           'transports': ['TCP', 'UDP']}

    def test_config_from_dict(self) -> None:
        self._do_mock_test(self._do_test_config_from_dict)

    def _do_test_config_from_dict(self) -> None:
        export = Export.from_dict(1, {
            'export_id': 1,
            'path': '/',
            'cluster_id': self.cluster_id,
            'pseudo': '/cephfs_a',
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

        assert export.export_id == 1
        assert export.path == "/"
        assert export.pseudo == "/cephfs_a"
        assert export.access_type == "RW"
        assert export.squash == "root_squash"
        assert set(export.protocols) == {4}
        assert set(export.transports) == {"TCP", "UDP"}
        assert export.fsal.name == "CEPH"
        assert export.fsal.user_id == "ganesha"
        assert export.fsal.fs_name == "a"
        assert export.fsal.sec_label_xattr == 'security.selinux'
        assert len(export.clients) == 2
        assert export.clients[0].addresses == \
            ["192.168.0.10", "192.168.1.0/8"]
        assert export.clients[0].squash == "no_root_squash"
        assert export.clients[0].access_type is None
        assert export.clients[1].addresses == ["192.168.0.0/16"]
        assert export.clients[1].squash == "all_squash"
        assert export.clients[1].access_type == "RO"
        assert export.cluster_id == self.cluster_id
        assert export.attr_expiration_time == 0
        assert export.security_label

        export = Export.from_dict(2, {
            'export_id': 2,
            'path': 'bucket',
            'pseudo': '/rgw',
            'cluster_id': self.cluster_id,
            'access_type': 'RW',
            'squash': 'all_squash',
            'security_label': False,
            'protocols': [4, 3],
            'transports': ['TCP', 'UDP'],
            'clients': [],
            'fsal': {
                'name': 'RGW',
                'user_id': 'rgw.foo.bucket',
                'access_key_id': 'the_access_key',
                'secret_access_key': 'the_secret_key'
            }
        })

        assert export.export_id == 2
        assert export.path == "bucket"
        assert export.pseudo == "/rgw"
        assert export.access_type == "RW"
        assert export.squash == "all_squash"
        assert set(export.protocols) == {4, 3}
        assert set(export.transports) == {"TCP", "UDP"}
        assert export.fsal.name == "RGW"
        assert export.fsal.user_id == "rgw.foo.bucket"
        assert export.fsal.access_key_id == "the_access_key"
        assert export.fsal.secret_access_key == "the_secret_key"
        assert len(export.clients) == 0
        assert export.cluster_id == self.cluster_id

    @pytest.mark.parametrize(
        "block",
        [
            export_1,
            export_2,
        ]
    )
    def test_export_from_to_export_block(self, block):
        blocks = GaneshaConfParser(block).parse()
        export = Export.from_export_block(blocks[0], self.cluster_id)
        newblock = export.to_export_block()
        export2 = Export.from_export_block(newblock, self.cluster_id)
        newblock2 = export2.to_export_block()
        assert newblock == newblock2

    @pytest.mark.parametrize(
        "block",
        [
            export_1,
            export_2,
        ]
    )
    def test_export_from_to_dict(self, block):
        blocks = GaneshaConfParser(block).parse()
        export = Export.from_export_block(blocks[0], self.cluster_id)
        j = export.to_dict()
        export2 = Export.from_dict(j['export_id'], j)
        j2 = export2.to_dict()
        assert j == j2

    @pytest.mark.parametrize(
        "block",
        [
            export_1,
            export_2,
        ]
    )
    def test_export_validate(self, block):
        blocks = GaneshaConfParser(block).parse()
        export = Export.from_export_block(blocks[0], self.cluster_id)
        nfs_mod = Module('nfs', '', '')
        with mock.patch('nfs.ganesha_conf.check_fs', return_value=True):
            export.validate(nfs_mod)

    def test_update_export(self):
        self._do_mock_test(self._do_test_update_export)

    def _do_test_update_export(self):
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        r = conf.apply_export(self.cluster_id, json.dumps({
            'export_id': 2,
            'path': 'bucket',
            'pseudo': '/rgw/bucket',
            'cluster_id': self.cluster_id,
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
                'user_id': 'nfs.foo.bucket',
                'access_key_id': 'the_access_key',
                'secret_access_key': 'the_secret_key',
            }
        }))
        assert len(r.changes) == 1

        export = conf._fetch_export('foo', '/rgw/bucket')
        assert export.export_id == 2
        assert export.path == "bucket"
        assert export.pseudo == "/rgw/bucket"
        assert export.access_type == "RW"
        assert export.squash == "all_squash"
        assert export.protocols == [4, 3]
        assert export.transports == ["TCP", "UDP"]
        assert export.fsal.name == "RGW"
        assert export.fsal.access_key_id == "the_access_key"
        assert export.fsal.secret_access_key == "the_secret_key"
        assert len(export.clients) == 1
        assert export.clients[0].squash is None
        assert export.clients[0].access_type is None
        assert export.cluster_id == self.cluster_id

        # do it again, with changes
        r = conf.apply_export(self.cluster_id, json.dumps({
            'export_id': 2,
            'path': 'newbucket',
            'pseudo': '/rgw/bucket',
            'cluster_id': self.cluster_id,
            'access_type': 'RO',
            'squash': 'root',
            'security_label': False,
            'protocols': [4],
            'transports': ['TCP'],
            'clients': [{
                'addresses': ["192.168.10.0/16"],
                'access_type': None,
                'squash': None
            }],
            'fsal': {
                'name': 'RGW',
                'user_id': 'nfs.foo.newbucket',
                'access_key_id': 'the_access_key',
                'secret_access_key': 'the_secret_key',
            }
        }))
        assert len(r.changes) == 1

        export = conf._fetch_export('foo', '/rgw/bucket')
        assert export.export_id == 2
        assert export.path == "newbucket"
        assert export.pseudo == "/rgw/bucket"
        assert export.access_type == "RO"
        assert export.squash == "root"
        assert export.protocols == [4]
        assert export.transports == ["TCP"]
        assert export.fsal.name == "RGW"
        assert export.fsal.access_key_id == "the_access_key"
        assert export.fsal.secret_access_key == "the_secret_key"
        assert len(export.clients) == 1
        assert export.clients[0].squash is None
        assert export.clients[0].access_type is None
        assert export.cluster_id == self.cluster_id

        # again, but without export_id and qos_block
        cluster = NFSCluster(nfs_mod)
        bw_obj = QOSBandwidthControl(True, export_writebw='100MB', export_readbw='200MB')
        cluster.enable_cluster_qos_bw(self.cluster_id, QOSType['PerShare'], bw_obj)

        r = conf.apply_export(self.cluster_id, json.dumps({
            'path': 'newestbucket',
            'pseudo': '/rgw/bucket',
            'cluster_id': self.cluster_id,
            'access_type': 'RW',
            'squash': 'root',
            'security_label': False,
            'protocols': [4],
            'transports': ['TCP'],
            'clients': [{
                'addresses': ["192.168.10.0/16"],
                'access_type': None,
                'squash': None
            }],
            'fsal': {
                'name': 'RGW',
                'user_id': 'nfs.foo.newestbucket',
                'access_key_id': 'the_access_key',
                'secret_access_key': 'the_secret_key',
            },
            'qos_block': {
               'combined_rw_bw_control': False,
               'enable_bw_control': True,
               'enable_qos': True,
               'max_export_read_bw': '3000000',
               'max_export_write_bw': '2000000'
            }
        }))
        assert len(r.changes) == 1

        export = conf._fetch_export(self.cluster_id, '/rgw/bucket')
        assert export.export_id == 2
        assert export.path == "newestbucket"
        assert export.pseudo == "/rgw/bucket"
        assert export.access_type == "RW"
        assert export.squash == "root"
        assert export.protocols == [4]
        assert export.transports == ["TCP"]
        assert export.fsal.name == "RGW"
        assert export.fsal.access_key_id == "the_access_key"
        assert export.fsal.secret_access_key == "the_secret_key"
        assert len(export.clients) == 1
        assert export.clients[0].squash is None
        assert export.clients[0].access_type is None
        assert export.cluster_id == self.cluster_id
        assert export.qos_block.enable_qos == True
        assert export.qos_block.bw_obj.enable_bw_ctrl == True
        assert export.qos_block.bw_obj.combined_bw_ctrl == False
        assert export.qos_block.bw_obj.export_writebw == 2000000
        assert export.qos_block.bw_obj.export_readbw == 3000000

    def test_update_export_sectype(self):
        self._do_mock_test(self._test_update_export_sectype)

    def _test_update_export_sectype(self):
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        r = conf.apply_export(self.cluster_id, json.dumps({
            'export_id': 2,
            'path': 'bucket',
            'pseudo': '/rgw/bucket',
            'cluster_id': self.cluster_id,
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
                'user_id': 'nfs.foo.bucket',
                'access_key_id': 'the_access_key',
                'secret_access_key': 'the_secret_key',
            }
        }))
        assert len(r.changes) == 1

        # no sectype was given, key not present
        info = conf._get_export_dict(self.cluster_id, "/rgw/bucket")
        assert info["export_id"] == 2
        assert info["path"] == "bucket"
        assert "sectype" not in info
        assert 'XprtSec' not in info

        r = conf.apply_export(self.cluster_id, json.dumps({
            'export_id': 2,
            'path': 'bucket',
            'pseudo': '/rgw/bucket',
            'cluster_id': self.cluster_id,
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
            'sectype': ["krb5p", "krb5i", "sys"],
            'XprtSec': 'tls',
            'fsal': {
                'name': 'RGW',
                'user_id': 'nfs.foo.bucket',
                'access_key_id': 'the_access_key',
                'secret_access_key': 'the_secret_key',
            }
        }))
        assert len(r.changes) == 1

        # assert sectype matches new value(s)
        info = conf._get_export_dict(self.cluster_id, "/rgw/bucket")
        assert info["export_id"] == 2
        assert info["path"] == "bucket"
        assert info["sectype"] == ["krb5p", "krb5i", "sys"]
        assert info['XprtSec'] == 'tls'

    def test_update_export_with_ganesha_conf(self):
        self._do_mock_test(self._do_test_update_export_with_ganesha_conf)

    def _do_test_update_export_with_ganesha_conf(self):
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        r = conf.apply_export(self.cluster_id, self.export_3)
        assert len(r.changes) == 1

    def test_update_export_with_ganesha_conf_sectype(self):
        self._do_mock_test(
            self._do_test_update_export_with_ganesha_conf_sectype,
            self.export_4, ["krb5p", "krb5i"])

    def test_update_export_with_ganesha_conf_sectype_lcase(self):
        export_conf = self.export_4.replace("SecType", "sectype").replace("krb5i", "sys")
        self._do_mock_test(
            self._do_test_update_export_with_ganesha_conf_sectype,
            export_conf, ["krb5p", "sys"])

    def _do_test_update_export_with_ganesha_conf_sectype(self, export_conf, expect_sectype):
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        r = conf.apply_export(self.cluster_id, export_conf)
        assert len(r.changes) == 1

        # assert sectype matches new value(s)
        info = conf._get_export_dict(self.cluster_id, "/secure1")
        assert info["export_id"] == 1
        assert info["path"] == "/secure/me"
        assert info["sectype"] == expect_sectype

    def test_update_export_with_list(self):
        self._do_mock_test(self._do_test_update_export_with_list)
    
    def test_update_export_cephfs(self):
        self._do_mock_test(self._do_test_update_export_cephfs)

    def _do_test_update_export_with_list(self):
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        r = conf.apply_export(self.cluster_id, json.dumps([
            {
                'path': 'bucket',
                'pseudo': '/rgw/bucket',
                'cluster_id': self.cluster_id,
                'access_type': 'RW',
                'squash': 'root',
                'security_label': False,
                'protocols': [4],
                'transports': ['TCP'],
                'clients': [{
                    'addresses': ["192.168.0.0/16"],
                    'access_type': None,
                    'squash': None
                }],
                'fsal': {
                    'name': 'RGW',
                    'user_id': 'nfs.foo.bucket',
                    'access_key_id': 'the_access_key',
                    'secret_access_key': 'the_secret_key',
                }
            },
            {
                'path': 'bucket2',
                'pseudo': '/rgw/bucket2',
                'cluster_id': self.cluster_id,
                'access_type': 'RO',
                'squash': 'root',
                'security_label': False,
                'protocols': [4],
                'transports': ['TCP'],
                'clients': [{
                    'addresses': ["192.168.0.0/16"],
                    'access_type': None,
                    'squash': None
                }],
                'fsal': {
                    'name': 'RGW',
                    'user_id': 'nfs.foo.bucket2',
                    'access_key_id': 'the_access_key',
                    'secret_access_key': 'the_secret_key',
                }
            },
        ]))
        # The input object above contains TWO items (two different pseudo paths)
        # therefore we expect the result to report that two changes have been
        # applied, rather than the typical 1 change.
        assert len(r.changes) == 2

        export = conf._fetch_export('foo', '/rgw/bucket')
        assert export.export_id == 4
        assert export.path == "bucket"
        assert export.pseudo == "/rgw/bucket"
        assert export.access_type == "RW"
        assert export.squash == "root"
        assert export.protocols == [4]
        assert export.transports == ["TCP"]
        assert export.fsal.name == "RGW"
        assert export.fsal.access_key_id == "the_access_key"
        assert export.fsal.secret_access_key == "the_secret_key"
        assert len(export.clients) == 1
        assert export.clients[0].squash is None
        assert export.clients[0].access_type is None
        assert export.cluster_id == self.cluster_id

        export = conf._fetch_export('foo', '/rgw/bucket2')
        assert export.export_id == 5
        assert export.path == "bucket2"
        assert export.pseudo == "/rgw/bucket2"
        assert export.access_type == "RO"
        assert export.squash == "root"
        assert export.protocols == [4]
        assert export.transports == ["TCP"]
        assert export.fsal.name == "RGW"
        assert export.fsal.access_key_id == "the_access_key"
        assert export.fsal.secret_access_key == "the_secret_key"
        assert len(export.clients) == 1
        assert export.clients[0].squash is None
        assert export.clients[0].access_type is None
        assert export.cluster_id == self.cluster_id

    def _do_test_update_export_cephfs(self):
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        r = conf.apply_export(self.cluster_id, json.dumps({
            'export_id': 3,
            'path': '/',
            'cluster_id': self.cluster_id,
            'pseudo': '/cephfs_c',
            'access_type': 'RW',
            'squash': 'root_squash',
            'security_label': True,
            'protocols': [4],
            'transports': ['TCP', 'UDP'],
            'fsal': {
                'name': 'CEPH',
                'fs_name': 'c',
            }
        }))
        assert len(r.changes) == 1

        export = conf._fetch_export('foo', '/cephfs_c')
        assert export.export_id == 3
        assert export.path == "/"
        assert export.pseudo == "/cephfs_c"
        assert export.access_type == "RW"
        assert export.squash == "root_squash"
        assert export.protocols == [4]
        assert export.transports == ["TCP", "UDP"]
        assert export.fsal.name == "CEPH"
        assert export.fsal.cmount_path == "/"
        assert export.fsal.user_id == "nfs.foo.c.02de2980"
        assert export.cluster_id == self.cluster_id
    
    def test_remove_export(self) -> None:
        self._do_mock_test(self._do_test_remove_export)

    def _do_test_remove_export(self) -> None:
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        assert len(conf.exports[self.cluster_id]) == 3
        conf.delete_export(cluster_id=self.cluster_id,
                           pseudo_path="/rgw")
        exports = conf.exports[self.cluster_id]
        assert len(exports) == 2
        assert exports[0].export_id == 1

    def test_create_export_rgw_bucket(self):
        self._do_mock_test(self._do_test_create_export_rgw_bucket)

    def _do_test_create_export_rgw_bucket(self):
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)

        ls = conf.list_exports(cluster_id=self.cluster_id)
        assert len(ls) == 3

        r = conf.create_export(
            fsal_type='rgw',
            cluster_id=self.cluster_id,
            bucket='bucket',
            pseudo_path='/mybucket',
            read_only=False,
            squash='root',
            addr=["192.168.0.0/16"],
            xprtsec='tls'
        )
        assert r["bind"] == "/mybucket"

        ls = conf.list_exports(cluster_id=self.cluster_id)
        assert len(ls) == 4

        export = conf._fetch_export('foo', '/mybucket')
        assert export.export_id
        assert export.path == "bucket"
        assert export.pseudo == "/mybucket"
        assert export.access_type == "none"
        assert export.squash == "none"
        assert export.protocols == [4]
        assert export.transports == ["TCP"]
        assert export.fsal.name == "RGW"
        assert export.fsal.user_id == "bucket_owner_user"
        assert export.fsal.access_key_id == "the_access_key"
        assert export.fsal.secret_access_key == "the_secret_key"
        assert len(export.clients) == 1
        assert export.clients[0].squash == 'root'
        assert export.clients[0].access_type == 'rw'
        assert export.clients[0].addresses == ["192.168.0.0/16"]
        assert export.cluster_id == self.cluster_id
        assert export.xprtsec == 'tls'

    def test_create_export_rgw_bucket_user(self):
        self._do_mock_test(self._do_test_create_export_rgw_bucket_user)

    def _do_test_create_export_rgw_bucket_user(self):
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)

        ls = conf.list_exports(cluster_id=self.cluster_id)
        assert len(ls) == 3

        r = conf.create_export(
            fsal_type='rgw',
            cluster_id=self.cluster_id,
            bucket='bucket',
            user_id='other_user',
            pseudo_path='/mybucket',
            read_only=False,
            squash='root',
            addr=["192.168.0.0/16"]
        )
        assert r["bind"] == "/mybucket"

        ls = conf.list_exports(cluster_id=self.cluster_id)
        assert len(ls) == 4

        export = conf._fetch_export('foo', '/mybucket')
        assert export.export_id
        assert export.path == "bucket"
        assert export.pseudo == "/mybucket"
        assert export.access_type == "none"
        assert export.squash == "none"
        assert export.protocols == [4]
        assert export.transports == ["TCP"]
        assert export.fsal.name == "RGW"
        assert export.fsal.access_key_id == "the_access_key"
        assert export.fsal.secret_access_key == "the_secret_key"
        assert len(export.clients) == 1
        assert export.clients[0].squash == 'root'
        assert export.fsal.user_id == "other_user"
        assert export.clients[0].access_type == 'rw'
        assert export.clients[0].addresses == ["192.168.0.0/16"]
        assert export.cluster_id == self.cluster_id

    def test_create_export_rgw_user(self):
        self._do_mock_test(self._do_test_create_export_rgw_user)

    def _do_test_create_export_rgw_user(self):
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)

        ls = conf.list_exports(cluster_id=self.cluster_id)
        assert len(ls) == 3

        r = conf.create_export(
            fsal_type='rgw',
            cluster_id=self.cluster_id,
            user_id='some_user',
            pseudo_path='/mybucket',
            read_only=False,
            squash='root',
            addr=["192.168.0.0/16"]
        )
        assert r["bind"] == "/mybucket"

        ls = conf.list_exports(cluster_id=self.cluster_id)
        assert len(ls) == 4

        export = conf._fetch_export('foo', '/mybucket')
        assert export.export_id
        assert export.path == "/"
        assert export.pseudo == "/mybucket"
        assert export.access_type == "none"
        assert export.squash == "none"
        assert export.protocols == [4]
        assert export.transports == ["TCP"]
        assert export.fsal.name == "RGW"
        assert export.fsal.access_key_id == "the_access_key"
        assert export.fsal.secret_access_key == "the_secret_key"
        assert len(export.clients) == 1
        assert export.clients[0].squash == 'root'
        assert export.fsal.user_id == "some_user"
        assert export.clients[0].access_type == 'rw'
        assert export.clients[0].addresses == ["192.168.0.0/16"]
        assert export.cluster_id == self.cluster_id

    def test_create_export_cephfs(self):
        self._do_mock_test(self._do_test_create_export_cephfs)
    
    def test_create_export_cephfs_with_cmount_path(self):
        self._do_mock_test(self._do_test_create_export_cephfs_with_cmount_path)
    
    def test_create_export_cephfs_with_invalid_cmount_path(self):
        self._do_mock_test(self._do_test_create_export_cephfs_with_invalid_cmount_path)

    def _do_test_create_export_cephfs(self):
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)

        ls = conf.list_exports(cluster_id=self.cluster_id)
        assert len(ls) == 3

        r = conf.create_export(
            fsal_type='cephfs',
            cluster_id=self.cluster_id,
            fs_name='myfs',
            path='/',
            pseudo_path='/cephfs2',
            read_only=False,
            squash='root',
            addr=["192.168.1.0/8"],
        )
        assert r["bind"] == "/cephfs2"

        ls = conf.list_exports(cluster_id=self.cluster_id)
        assert len(ls) == 4

        export = conf._fetch_export('foo', '/cephfs2')
        assert export.export_id
        assert export.path == "/"
        assert export.pseudo == "/cephfs2"
        assert export.access_type == "none"
        assert export.squash == "none"
        assert export.protocols == [4]
        assert export.transports == ["TCP"]
        assert export.fsal.name == "CEPH"
        assert export.fsal.user_id == "nfs.foo.myfs.86ca58ef"
        assert export.fsal.cephx_key == "thekeyforclientabc"
        assert len(export.clients) == 1
        assert export.clients[0].squash == 'root'
        assert export.clients[0].access_type == 'rw'
        assert export.clients[0].addresses == ["192.168.1.0/8"]
        assert export.cluster_id == self.cluster_id

    def test_rotate_keys(self):
        self._do_mock_test(self._do_test_rotate_keys)

    def test_rotate_keys_all_entities(self):
        self._do_mock_test(self._do_test_rotate_keys_all_entities)

    def test_rotate_keys_rejects_foreign_entity(self):
        self._do_mock_test(self._do_test_rotate_keys_rejects_foreign_entity)

    def test_rotate_keys_requires_explicit_selection(self):
        self._do_mock_test(self._do_test_rotate_keys_requires_explicit_selection)

    def test_rotate_keys_rejects_both_selections(self):
        self._do_mock_test(self._do_test_rotate_keys_rejects_both_selections)

    def _do_test_rotate_keys(self):
        nfs_mod = Module('nfs', '', '')
        conf = nfs_mod.export_mgr
        cluster = nfs_mod.nfs

        conf.create_export(
            fsal_type='cephfs',
            cluster_id=self.cluster_id,
            fs_name='myfs',
            path='/',
            pseudo_path='/cephfs_rotate',
            read_only=False,
            squash='none',
        )
        conf.create_export(
            fsal_type='cephfs',
            cluster_id=self.cluster_id,
            fs_name='myfs',
            path='/',
            pseudo_path='/cephfs_rotate2',
            read_only=False,
            squash='none',
        )

        entity = 'client.nfs.foo.myfs.86ca58ef'
        rotated_key = 'rotated-export-key'

        def mock_create_user_key(self, cluster_id, entity_name, path, fs_name):
            return rotated_key

        with mock.patch.object(nfs_mod, 'check_mon_command') as check_mon, \
                mock.patch.object(ExportMgr, '_create_user_key', mock_create_user_key), \
                mock.patch('nfs.cluster.redeploy_nfs_service') as redeploy:
            result = cluster.rotate_keys(
                cluster_id=self.cluster_id,
                auth_entities=[entity, 'nfs.foo.myfs.86ca58ef'],
                key_type='aes256k',
            )

        check_mon.assert_called_once_with({
            'prefix': 'auth rotate',
            'entity': entity,
            'key_type': 'aes256k',
        })
        redeploy.assert_not_called()
        assert result['cluster_id'] == self.cluster_id
        assert result['rotated'] == [entity]
        assert result['export_keys'] == [entity]
        assert result['daemon_keys'] == []
        assert result['key_type'] == 'aes256k'
        assert result['service_redeployed'] is False
        assert sorted(result['updated_exports']) == ['/cephfs_rotate', '/cephfs_rotate2']

        for pseudo in ('/cephfs_rotate', '/cephfs_rotate2'):
            export = conf._fetch_export(self.cluster_id, pseudo)
            assert export is not None
            assert export.fsal.cephx_key == rotated_key

    def _do_test_rotate_keys_all_entities(self):
        nfs_mod = Module('nfs', '', '')
        conf = nfs_mod.export_mgr
        cluster = nfs_mod.nfs

        conf.create_export(
            fsal_type='cephfs',
            cluster_id=self.cluster_id,
            fs_name='myfs',
            path='/',
            pseudo_path='/cephfs_rotate_all',
            read_only=False,
            squash='none',
        )

        export_entity = 'client.nfs.foo.myfs.86ca58ef'
        daemon_entity = 'client.nfs.foo.0.0.host.hash-rgw'
        cluster_entity = 'client.nfs.foo'
        rotated_key = 'rotated-export-key'

        def mock_create_user_key(self, cluster_id, entity_name, path, fs_name):
            return rotated_key

        auth_ls = {
            'auth_dump': [
                {'entity': cluster_entity},
                {'entity': daemon_entity},
                {'entity': export_entity},
                {'entity': 'client.nfs.other.fs.deadbeef'},
            ]
        }

        def mock_check_mon(cmd):
            if cmd.get('prefix') == 'auth ls':
                return mock.Mock(stdout=json.dumps(auth_ls), retval=0, stderr='')
            return mock.Mock(stdout='', retval=0, stderr='')

        with mock.patch.object(nfs_mod, 'check_mon_command', side_effect=mock_check_mon) as check_mon, \
                mock.patch.object(ExportMgr, '_create_user_key', mock_create_user_key), \
                mock.patch('nfs.cluster.redeploy_nfs_service') as redeploy:
            result = cluster.rotate_keys(
                cluster_id=self.cluster_id,
                all_daemon_and_export_keys=True,
                key_type='aes256k',
            )

        rotate_calls = [
            c.args[0] for c in check_mon.call_args_list
            if c.args[0].get('prefix') == 'auth rotate'
        ]
        rotated = [c['entity'] for c in rotate_calls]
        assert sorted(rotated) == sorted([cluster_entity, daemon_entity, export_entity])
        assert all(c.get('key_type') == 'aes256k' for c in rotate_calls)
        redeploy.assert_called_once_with(nfs_mod, self.cluster_id)
        assert result['service_redeployed'] is True
        assert sorted(result['daemon_keys']) == sorted([cluster_entity, daemon_entity])
        assert result['export_keys'] == [export_entity]
        assert result['updated_exports'] == ['/cephfs_rotate_all']

    def _do_test_rotate_keys_rejects_foreign_entity(self):
        nfs_mod = Module('nfs', '', '')
        cluster = nfs_mod.nfs

        with pytest.raises(Exception) as e:
            cluster.rotate_keys(
                cluster_id=self.cluster_id,
                auth_entities=['client.nfs.other.fs.deadbeef'],
            )
        assert 'does not belong to NFS cluster' in str(e.value)

    def _do_test_rotate_keys_requires_explicit_selection(self):
        nfs_mod = Module('nfs', '', '')
        cluster = nfs_mod.nfs

        with mock.patch.object(nfs_mod, 'check_mon_command') as check_mon, \
                mock.patch('nfs.cluster.redeploy_nfs_service') as redeploy:
            with pytest.raises(Exception) as e:
                cluster.rotate_keys(cluster_id=self.cluster_id)

        assert '--all-daemon-and-export-keys' in str(e.value)
        assert '--auth-entities' in str(e.value)
        assert 'ceph auth ls' in str(e.value)
        check_mon.assert_not_called()
        redeploy.assert_not_called()

    def _do_test_rotate_keys_rejects_both_selections(self):
        nfs_mod = Module('nfs', '', '')
        cluster = nfs_mod.nfs

        with mock.patch.object(nfs_mod, 'check_mon_command') as check_mon, \
                mock.patch('nfs.cluster.redeploy_nfs_service') as redeploy:
            with pytest.raises(Exception) as e:
                cluster.rotate_keys(
                    cluster_id=self.cluster_id,
                    all_daemon_and_export_keys=True,
                    auth_entities=['client.nfs.foo.myfs.86ca58ef'],
                )

        assert 'mutually exclusive' in str(e.value)
        check_mon.assert_not_called()
        redeploy.assert_not_called()

    def test_create_export_default_transports_rdma_cluster(self):
        """When cluster has enable_rdma=True, new exports get default Transports = tcp, RDMA."""
        self._do_mock_test(self._do_test_create_export_default_transports_rdma_cluster)

    def _do_test_create_export_default_transports_rdma_cluster(self):
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        rdma_spec = NFSServiceSpec(service_id=self.cluster_id, enable_rdma=True)
        with mock.patch('nfs.export.get_nfs_spec_for_cluster', return_value=rdma_spec):
            r = conf.create_export(
                fsal_type='rgw',
                cluster_id=self.cluster_id,
                bucket='rdmabucket',
                pseudo_path='/rdmabucket',
                read_only=False,
                squash='root',
                addr=["192.168.0.0/16"],
            )
        assert r["bind"] == "/rdmabucket"
        export = conf._fetch_export(self.cluster_id, '/rdmabucket')
        assert export is not None
        assert sorted(export.transports) == ["RDMA", "TCP"]

    def test_export_transport_rdma_valid(self):
        """Export with Transports = TCP, RDMA is valid."""
        export_block = """
EXPORT {
    export_id = 1;
    path = "/";
    pseudo = "/rdma_export";
    access_type = "RW";
    squash = "none";
    protocols = 4;
    transports = "TCP", "RDMA";
    FSAL {
        name = "CEPH";
        filesystem = "a";
        cmount_path = "/";
    }
}
"""
        blocks = GaneshaConfParser(export_block).parse()
        export = Export.from_export_block(blocks[0], self.cluster_id)
        assert set(export.transports) == {"TCP", "RDMA"}
        # Validate should pass (RDMA is in valid_transport)
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        with mock.patch('nfs.export.check_fs', return_value=True), \
                mock.patch('nfs.ganesha_conf.check_fs', return_value=True):
            export.validate(conf.mgr)

    def test_update_export_without_transport_rdma_cluster(self):
        """Apply export update without Transport fields; result has Transports = TCP, RDMA."""
        self._do_mock_test(self._do_test_update_export_without_transport_rdma_cluster)

    def _do_test_update_export_without_transport_rdma_cluster(self):
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        # Existing export at /rgw has Transports = TCP, UDP (from export_2)
        export_before = conf._fetch_export(self.cluster_id, '/rgw')
        assert export_before is not None
        assert set(export_before.transports) == {"TCP", "UDP"}

        # apply_export with no 'transports' field; cluster has enable_rdma -> default TCP, RDMA
        rdma_spec = NFSServiceSpec(service_id=self.cluster_id, enable_rdma=True)
        with mock.patch('nfs.export.get_nfs_spec_for_cluster', return_value=rdma_spec):
            r = conf.apply_export(self.cluster_id, json.dumps({
                'export_id': 2,
                'path': '/',
                'pseudo': '/rgw',
                'cluster_id': self.cluster_id,
                'access_type': 'RO',
                'squash': 'root',
                'security_label': False,
                'protocols': [4, 3],
                'clients': [],
                'fsal': {
                    'name': 'RGW',
                    'user_id': 'nfs.foo.bucket',
                    'access_key_id': 'the_access_key',
                    'secret_access_key': 'the_secret_key',
                },
            }))
        assert len(r.changes) == 1

        export_after = conf._fetch_export(self.cluster_id, '/rgw')
        assert export_after is not None
        assert export_after.export_id == 2
        assert export_after.access_type == 'RO'
        assert export_after.squash == 'root'
        # Updated export has Transports = TCP, RDMA (default when transports omitted and RDMA enabled)
        assert set(export_after.transports) == {'TCP', 'RDMA'}
    
    def _do_test_create_export_cephfs_with_cmount_path(self):
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)

        ls = conf.list_exports(cluster_id=self.cluster_id)
        assert len(ls) == 3

        r = conf.create_export(
            fsal_type='cephfs',
            cluster_id=self.cluster_id,
            fs_name='myfs',
            path='/',
            pseudo_path='/cephfs3',
            read_only=False,
            squash='root',
            cmount_path='/',
            )
        assert r["bind"] == "/cephfs3"

        ls = conf.list_exports(cluster_id=self.cluster_id)
        assert len(ls) == 4

        export = conf._fetch_export('foo', '/cephfs3')
        assert export.export_id
        assert export.path == "/"
        assert export.pseudo == "/cephfs3"
        assert export.access_type == "RW"
        assert export.squash == "root"
        assert export.protocols == [4]
        assert export.fsal.name == "CEPH"
        assert export.fsal.user_id == "nfs.foo.myfs.86ca58ef"
        assert export.fsal.cephx_key == "thekeyforclientabc"
        assert export.fsal.cmount_path == "/"
        assert export.cluster_id == self.cluster_id
    
    def _do_test_create_export_cephfs_with_invalid_cmount_path(self):
        import object_format

        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)

        with pytest.raises(object_format.ErrorResponse) as e:
            conf.create_export(
                fsal_type='cephfs',
                cluster_id=self.cluster_id,
                fs_name='myfs',
                path='/',
                pseudo_path='/cephfs4',
                read_only=False,
                squash='root',
                cmount_path='/invalid',
                )
        assert "Invalid cmount_path: '/invalid'" in str(e.value)

    def _do_test_cluster_ls(self):
        nfs_mod = Module('nfs', '', '')
        cluster = NFSCluster(nfs_mod)

        out = cluster.list_nfs_cluster()
        assert out[0] == self.cluster_id

    def test_cluster_ls(self):
        self._do_mock_test(self._do_test_cluster_ls)

    def _do_test_cluster_info(self):
        nfs_mod = Module('nfs', '', '')
        cluster = NFSCluster(nfs_mod)

        out = cluster.show_nfs_cluster_info(self.cluster_id)
        assert out == {"foo": {
            "deployment_type": "standalone",
            "virtual_ip": None,
            "backend": [],
            "placement": {},
            "enable_nfsv3": False,
            "enable_rdma": False,
        }}

    def test_cluster_info(self):
        self._do_mock_test(self._do_test_cluster_info)

    def _do_test_cluster_info_enable_rdma(self):
        nfs_mod = Module('nfs', '', '')
        cluster = NFSCluster(nfs_mod)
        rdma_services = [
            ServiceDescription(
                spec=NFSServiceSpec(service_id=self.cluster_id, enable_rdma=True)
            )
        ]

        def mock_describe_service(cls, *args, **kwargs):
            if kwargs['service_type'] == 'nfs':
                return OrchResult(rdma_services)
            return OrchResult([])

        with mock.patch('nfs.module.Module.describe_service', mock_describe_service):
            out = cluster.show_nfs_cluster_info(self.cluster_id)

        assert out == {"foo": {
            "deployment_type": "standalone",
            "virtual_ip": None,
            "backend": [],
            "placement": {},
            "enable_nfsv3": False,
            "enable_rdma": True,
        }}

    def test_cluster_info_enable_rdma(self):
        self._do_mock_test(self._do_test_cluster_info_enable_rdma)

    def _do_test_cluster_info_enable_nfsv3(self):
        nfs_mod = Module('nfs', '', '')
        cluster = NFSCluster(nfs_mod)
        nfs_services = [
            ServiceDescription(
                spec=NFSServiceSpec(service_id=self.cluster_id, enable_nfsv3=True)
            )
        ]

        def mock_describe_service(cls, *args, **kwargs):
            if kwargs['service_type'] == 'nfs':
                return OrchResult(nfs_services)
            return OrchResult([])

        with mock.patch('nfs.module.Module.describe_service', mock_describe_service):
            out = cluster.show_nfs_cluster_info(self.cluster_id)

        assert out == {"foo": {
            "deployment_type": "standalone",
            "virtual_ip": None,
            "backend": [],
            "placement": {},
            "enable_nfsv3": True,
            "enable_rdma": False,
        }}

    def test_cluster_info_enable_nfsv3(self):
        self._do_mock_test(self._do_test_cluster_info_enable_nfsv3)

    def _do_test_cluster_config(self):
        nfs_mod = Module('nfs', '', '')
        cluster = NFSCluster(nfs_mod)

        out = cluster.get_nfs_cluster_config(self.cluster_id)
        assert out == ""

        cluster.set_nfs_cluster_config(self.cluster_id, '# foo\n')

        out = cluster.get_nfs_cluster_config(self.cluster_id)
        assert out == "# foo\n"

        cluster.reset_nfs_cluster_config(self.cluster_id)

        out = cluster.get_nfs_cluster_config(self.cluster_id)
        assert out == ""

    def test_cluster_config(self):
        self._do_mock_test(self._do_test_cluster_config)

    def test_is_log_only_config(self):
        from nfs.cluster import _is_log_only_config

        assert _is_log_only_config("LOG { Default_log_level = FULL_DEBUG; }")
        assert _is_log_only_config(
            "LOG { COMPONENTS { FSAL = FULL_DEBUG; NFS4 = FULL_DEBUG; } }")

        assert not _is_log_only_config("EXPORT { Export_Id = 1; }")
        assert not _is_log_only_config("NFS_CORE_PARAM { some_val = 1; }")

        assert not _is_log_only_config(
            "LOG { Default_log_level = FULL_DEBUG; } EXPORT { Export_Id = 1; }")

        assert not _is_log_only_config("not valid ganesha config {{{")
        assert not _is_log_only_config("")

        # GaneshaConfParser does not strip comments; a comment before the
        # LOG block gets merged into the block name, so the config is not
        # recognised as LOG-only.  This is the safe fallback (restart).
        assert not _is_log_only_config(
            "# Enable debug\nLOG { Default_log_level = FULL_DEBUG; }")


    def _do_test_cluster_config_log_only_no_restart(self):
        nfs_mod = Module('nfs', '', '')
        cluster = NFSCluster(nfs_mod)

        log_config = "LOG {\n    Default_log_level = FULL_DEBUG;\n}\n"
        with mock.patch('nfs.cluster.restart_nfs_service') as mock_restart:
            cluster.set_nfs_cluster_config(self.cluster_id, log_config)
            mock_restart.assert_not_called()

    def test_cluster_config_log_only_no_restart(self):
        self._do_mock_test(self._do_test_cluster_config_log_only_no_restart)

    def _do_test_cluster_config_non_log_restarts(self):
        nfs_mod = Module('nfs', '', '')
        cluster = NFSCluster(nfs_mod)

        export_config = "EXPORT {\n    Export_Id = 100;\n}\n"
        with mock.patch('nfs.cluster.restart_nfs_service') as mock_restart:
            cluster.set_nfs_cluster_config(self.cluster_id, export_config)
            mock_restart.assert_called_once()

    def test_cluster_config_non_log_restarts(self):
        self._do_mock_test(self._do_test_cluster_config_non_log_restarts)

    def _do_test_cluster_config_mixed_restarts(self):
        nfs_mod = Module('nfs', '', '')
        cluster = NFSCluster(nfs_mod)

        mixed_config = ("LOG {\n    Default_log_level = FULL_DEBUG;\n}\n"
                        "EXPORT {\n    Export_Id = 100;\n}\n")
        with mock.patch('nfs.cluster.restart_nfs_service') as mock_restart:
            cluster.set_nfs_cluster_config(self.cluster_id, mixed_config)
            mock_restart.assert_called_once()

    def test_cluster_config_mixed_restarts(self):
        self._do_mock_test(self._do_test_cluster_config_mixed_restarts)

    def _do_test_cluster_config_reset_log_only_no_restart(self):
        nfs_mod = Module('nfs', '', '')
        cluster = NFSCluster(nfs_mod)

        log_config = "LOG {\n    Default_log_level = FULL_DEBUG;\n}\n"
        cluster.set_nfs_cluster_config(self.cluster_id, log_config)
        with mock.patch('nfs.cluster.restart_nfs_service') as mock_restart:
            cluster.reset_nfs_cluster_config(self.cluster_id)
            mock_restart.assert_not_called()

    def test_cluster_config_reset_log_only_no_restart(self):
        self._do_mock_test(self._do_test_cluster_config_reset_log_only_no_restart)

    def _do_test_cluster_config_reset_non_log_restarts(self):
        nfs_mod = Module('nfs', '', '')
        cluster = NFSCluster(nfs_mod)

        export_config = "EXPORT {\n    Export_Id = 100;\n}\n"
        cluster.set_nfs_cluster_config(self.cluster_id, export_config)
        with mock.patch('nfs.cluster.restart_nfs_service') as mock_restart:
            cluster.reset_nfs_cluster_config(self.cluster_id)
            mock_restart.assert_called_once()

    def test_cluster_config_reset_non_log_restarts(self):
        self._do_mock_test(self._do_test_cluster_config_reset_non_log_restarts)

    def test_qos_from_dict(self):
        qos = QOS.from_dict(self.qos_cluster_dict, True)
        assert qos.to_dict() == self.qos_cluster_dict

        qos = QOS.from_dict(self.qos_export_dict)
        assert qos.to_dict() == self.qos_export_dict

    @pytest.mark.parametrize("qos_block, qos_dict, qos_dict_bw_in_bytes, clust_op", [
        (qos_cluster_block, qos_cluster_dict, qos_cluster_dict_bw_in_bytes, True),
        (qos_export_block, qos_export_dict, qos_export_dict_bw_in_bytes, False)
        ])
    def test_qos_from_block(self, qos_block, qos_dict, qos_dict_bw_in_bytes, clust_op):
        blocks = GaneshaConfParser(qos_block).parse()
        assert isinstance(blocks, list)
        qos = QOS.from_qos_block(blocks[0], clust_op)
        assert qos.to_dict() == qos_dict
        assert qos.to_dict(ret_bw_in_bytes=True) == qos_dict_bw_in_bytes

    def _do_test_cluster_qos_bw(self, qos_type, combined_bw_ctrl, params, positive_tc):
        nfs_mod = Module('nfs', '', '')
        cluster = NFSCluster(nfs_mod)
        try:
            bw_obj = QOSBandwidthControl(True, combined_bw_ctrl, **params)
            cluster.enable_cluster_qos_bw(self.cluster_id, qos_type, bw_obj)
        except Exception:
            if not positive_tc:
                return
        if not positive_tc:
            raise Exception("This TC was supposed to fail")
        out = cluster.get_cluster_qos(self.cluster_id)
        expected_out = {"enable_bw_control": True, "enable_qos": True, "combined_rw_bw_control": combined_bw_ctrl, "qos_type": qos_type.name, "enable_iops_control": False}
        for key in params:
            expected_out[QOSParams[key].value] = bytes_to_human(with_units_to_int(params[key]), mode='binary')
        assert out == expected_out
        cluster.cluster_qos_set_config(self.cluster_id, 200)
        expected_out.update({'cqos_msg_interval': 200})
        assert cluster.get_cluster_qos(self.cluster_id) == expected_out
        cluster.disable_cluster_qos_bw(self.cluster_id)
        out = cluster.get_cluster_qos(self.cluster_id)
        assert out == {"enable_bw_control": False, "enable_qos": False, "combined_rw_bw_control": False, "enable_iops_control": False}

    @pytest.mark.parametrize("qos_type, combined_bw_ctrl, params, positive_tc", [
        (QOSType['PerShare'], False, {'export_writebw': '100MB', 'export_readbw': '200MB'}, True),
        (QOSType['PerClient'], False, {'client_writebw': '300MB', 'client_readbw': '400MB'}, True),
        (QOSType['PerShare_PerClient'], False, {'export_writebw': '100MB', 'export_readbw': '200MB', 'client_writebw': '300MB', 'client_readbw': '400MB'}, True),
        (QOSType['PerShare'], True, {'export_rw_bw': '100MB'}, True),
        (QOSType['PerClient'], True, {'client_rw_bw': '200MB'}, True),
        (QOSType['PerShare_PerClient'], True, {'export_rw_bw': '100MB', 'client_rw_bw': '200MB'}, True),
        # negative testing
        (QOSType['PerShare'], False, {'export_writebw': '100MB', 'client_readbw': '200MB'}, False),
        (QOSType['PerShare'], False, {'export_writebw': '100MB'}, False),
        (QOSType['PerClient'], False, {'client_writebw': '300MB'}, False),
        (QOSType['PerClient'], False, {'client_writebw': '300MB', 'export_readbw': '400MB'}, False),
        (QOSType['PerShare_PerClient'], False, {'export_writebw': '100MB', 'export_readbw': '200MB', 'client_writebw': '300MB'}, False),
        (QOSType['PerShare_PerClient'], False, {'export_writebw': '100MB'}, False),
        (QOSType['PerShare'], True, {'client_rw_bw': '100MB'}, False),
        (QOSType['PerShare'], True, {}, False),
        (QOSType['PerClient'], True, {'client_rw_bw': '200MB', 'export_rw_bw': '100MB'}, False),
        (QOSType['PerShare_PerClient'], True, {'export_rw_bw': '100MB'}, False)
        ])
    def test_cluster_qos_bw(self, qos_type, combined_bw_ctrl, params, positive_tc):
        self._do_mock_test(self._do_test_cluster_qos_bw, qos_type, combined_bw_ctrl, params, positive_tc)

    def _do_test_export_qos_bw(self, qos_type, clust_combined_bw_ctrl, clust_params, export_combined_bw_ctrl, export_params):
        nfs_mod = Module('nfs', '', '')
        cluster = NFSCluster(nfs_mod)
        export_mgr = ExportMgr(nfs_mod)
        # try enabling export level qos before enabling cluster level qos
        try:
            bw_obj = QOSBandwidthControl(True, export_combined_bw_ctrl, **export_params)
            export_mgr.enable_export_qos_bw(self.cluster_id, '/cephfs_a/', bw_obj)
        except Exception as e:
            assert str(e) == 'To configure bandwidth control for export, you must first enable bandwidth control at the cluster level for foo.'
        bw_obj = QOSBandwidthControl(True, clust_combined_bw_ctrl, **clust_params)
        cluster.enable_cluster_qos_bw(self.cluster_id, qos_type, bw_obj)
        clust_qos_conf = {'global_enable_qos': True, 'global_enable_bw_control': True, 'global_enable_iops_control': False}
        # set export qos
        try:
            bw_obj = QOSBandwidthControl(True, export_combined_bw_ctrl, **export_params)
            export_mgr.enable_export_qos_bw(self.cluster_id, '/cephfs_a/', bw_obj)
        except Exception:
            if export_combined_bw_ctrl:
                req = QOS_REQ_BW_PARAMS['combined_bw_enabled'][qos_type.name]
            else:
                req = QOS_REQ_BW_PARAMS['combined_bw_disabled'][qos_type.name]
            if sorted(export_params.keys()) != sorted(req):
                return
            if qos_type.name == 'PerClient':
                return
        out = export_mgr.get_export_qos(self.cluster_id, '/cephfs_a/')
        expected_out = {"enable_bw_control": True, "enable_qos": True, "combined_rw_bw_control": export_combined_bw_ctrl}
        for key in export_params:
            expected_out[QOSParams[key].value] = bytes_to_human(with_units_to_int(export_params[key]), mode='binary')
        expected_out.update(clust_qos_conf)
        assert out == expected_out
        export_mgr.disable_export_qos_bw(self.cluster_id, '/cephfs_a/')
        out = export_mgr.get_export_qos(self.cluster_id, '/cephfs_a/')
        clust_qos_conf.update({"enable_bw_control": False, "enable_qos": False, "combined_rw_bw_control": False})
        assert out == clust_qos_conf


    @pytest.mark.parametrize("qos_type, clust_combined_bw_ctrl, clust_params", [
        (QOSType['PerShare'], False, {'export_writebw': '100MB', 'export_readbw': '200MB'}),
        (QOSType['PerClient'], False, {'client_writebw': '300MB', 'client_readbw': '400MB'}),
        (QOSType['PerShare_PerClient'], False, {'export_writebw': '100MB', 'export_readbw': '200MB', 'client_writebw': '300MB', 'client_readbw': '400MB'}),
        (QOSType['PerShare'], True, {'export_rw_bw': '100MB'}),
        (QOSType['PerClient'], True, {'client_rw_bw': '200MB'}),
        (QOSType['PerShare_PerClient'], True, {'export_rw_bw': '100MB', 'client_rw_bw': '200MB'})
        ])
    @pytest.mark.parametrize("export_combined_bw_ctrl, export_params", [
        (False, {'export_writebw': '100MB', 'export_readbw': '200MB'}),
        (False, {'client_writebw': '300MB', 'client_readbw': '400MB'}),
        (False, {'export_writebw': '100MB', 'export_readbw': '200MB', 'client_writebw': '300MB', 'client_readbw': '400MB'}),
        (True, {'export_rw_bw': '100MB'}),
        (True, {'client_rw_bw': '200MB'}),
        (True, {'export_rw_bw': '100MB', 'client_rw_bw': '200MB'})
        ])
    def test_export_qos_bw(self, qos_type, clust_combined_bw_ctrl, clust_params,
                        export_combined_bw_ctrl, export_params):
        self._do_mock_test(self._do_test_export_qos_bw, qos_type, clust_combined_bw_ctrl,
                           clust_params, export_combined_bw_ctrl, export_params)

    def _do_test_cluster_qos_ops(self, qos_type, params, positive_tc):
        nfs_mod = Module('nfs', '', '')
        cluster = NFSCluster(nfs_mod)
        try:
            ops_obj = QOSOpsControl(True, **params)
            cluster.enable_cluster_qos_ops(self.cluster_id, qos_type, ops_obj)
        except Exception:
            if not positive_tc:
                return
        if not positive_tc:
            raise Exception("This TC was supposed to fail")
        out = cluster.get_cluster_qos(self.cluster_id)
        expected_out = {"enable_bw_control": False, "enable_qos": True, "combined_rw_bw_control": False, "qos_type": qos_type.name, "enable_iops_control": True}
        for key in params:
            expected_out[QOSParams[key].value] = params[key]
        assert out == expected_out
        cluster.cluster_qos_set_config(self.cluster_id, 200)
        expected_out.update({'cqos_msg_interval': 200})
        assert cluster.get_cluster_qos(self.cluster_id) == expected_out
        cluster.disable_cluster_qos_ops(self.cluster_id)
        out = cluster.get_cluster_qos(self.cluster_id)
        assert out == {"enable_bw_control": False, "enable_qos": False, "combined_rw_bw_control": False, "enable_iops_control": False}

    @pytest.mark.parametrize("qos_type, params, positive_tc", [
        (QOSType['PerShare'], {'max_export_iops': 10000}, True),
        (QOSType['PerClient'], {'max_client_iops': 15000}, True),
        (QOSType['PerShare_PerClient'], {'max_export_iops': 3000, 'max_client_iops': 14000}, True),
        # negative testing
        (QOSType['PerShare_PerClient'], {'max_export_iops': 1000}, False),
        (QOSType['PerShare'], {'max_client_iops': 10000}, False),
        (QOSType['PerShare'], {}, False),
        (QOSType['PerClient'], {'max_export_iops': 2000, 'max_client_iops': 1000}, False),
        (QOSType['PerShare_PerClient'], {'max_export_iops': 9}, False)
        ])
    def test_cluster_qos_ops(self, qos_type, params, positive_tc):
        self._do_mock_test(self._do_test_cluster_qos_ops, qos_type, params, positive_tc)

    def _do_test_export_qos_ops(self, qos_type, clust_params, export_params):
        nfs_mod = Module('nfs', '', '')
        cluster = NFSCluster(nfs_mod)
        export_mgr = ExportMgr(nfs_mod)
        # try enabling export level qos before enabling cluster level qos
        try:
            ops_obj = QOSOpsControl(True, **export_params)
            export_mgr.enable_export_qos_ops(self.cluster_id, '/cephfs_a/', ops_obj)
        except Exception as e:
            assert str(e) == 'To configure IOPS control for export, you must first enable IOPS control at the cluster level foo.'
        ops_obj = QOSOpsControl(True, **clust_params)
        cluster.enable_cluster_qos_ops(self.cluster_id, qos_type, ops_obj)
        clust_qos_conf = {'global_enable_qos': True, 'global_enable_bw_control': False, 'global_enable_iops_control': True}
        # set export qos
        try:
            ops_obj = QOSOpsControl(True, **export_params)
            export_mgr.enable_export_qos_ops(self.cluster_id, '/cephfs_a/', ops_obj)
        except Exception:
            req = QOS_REQ_OPS_PARAMS[qos_type.name]
            if sorted(export_params.keys()) != sorted(req):
                return
            if qos_type.name == 'PerClient':
                return
        out = export_mgr.get_export_qos(self.cluster_id, '/cephfs_a/')
        expected_out = {"enable_iops_control": True, "enable_qos": True}
        for key in export_params:
            expected_out[QOSParams[key].value] = export_params[key]
        expected_out.update(clust_qos_conf)
        assert out == expected_out
        export_mgr.disable_export_qos_ops(self.cluster_id, '/cephfs_a/')
        out = export_mgr.get_export_qos(self.cluster_id, '/cephfs_a/')
        clust_qos_conf.update({"enable_iops_control": False, "enable_qos": False})
        assert out == clust_qos_conf

    @pytest.mark.parametrize("qos_type, clust_params", [
        (QOSType['PerShare'], {'max_export_iops': 10000}),
        (QOSType['PerClient'], {'max_client_iops': 2000}),
        (QOSType['PerShare_PerClient'], {'max_export_iops': 3000, 'max_client_iops': 4000})
        ])
    @pytest.mark.parametrize("export_params", [
        ({'max_export_iops': 10000}),
        ({'max_client_iops': 2000}),
        ({'max_export_iops': 3000, 'max_client_iops': 4000})
        ])
    def test_export_qos_ops(self, qos_type, clust_params, export_params):
        self._do_mock_test(self._do_test_export_qos_ops, qos_type, clust_params, export_params)

    def _do_test_cluster_qos_bw_ops(self, bw_qos_type, bw_params, ops_qos_type, ops_params, positive_tc):
        nfs_mod = Module('nfs', '', '')
        cluster = NFSCluster(nfs_mod)
        try:
            bw_obj = QOSBandwidthControl(True, combined_bw_ctrl=False, **bw_params)
            cluster.enable_cluster_qos_bw(self.cluster_id, bw_qos_type, bw_obj)
            ops_obj = QOSOpsControl(True, **ops_params)
            cluster.enable_cluster_qos_ops(self.cluster_id, ops_qos_type, ops_obj)
        except Exception:
            if not positive_tc:
                return
        if not positive_tc:
            raise Exception("This TC passed but it was supposed to fail")
        out = cluster.get_cluster_qos(self.cluster_id)
        expected_out = {"enable_bw_control": True, "enable_qos": True, "combined_rw_bw_control": False, "qos_type": ops_qos_type.name, "enable_iops_control": True}
        bw_out = {}
        ops_out = {}
        for key in bw_params:
            bw_out[QOSParams[key].value] = bytes_to_human(with_units_to_int(bw_params[key]), mode='binary')
        for key in ops_params:
            ops_out[QOSParams[key].value] = ops_params[key]
        expected_out.update(bw_out)
        expected_out.update(ops_out)
        assert out == expected_out
        # disable bandwidth control
        cluster.disable_cluster_qos_bw(self.cluster_id)
        out = cluster.get_cluster_qos(self.cluster_id)
        ops_out.update({"enable_bw_control": False, "enable_qos": True, "combined_rw_bw_control": False, "enable_iops_control": True, "qos_type": ops_qos_type.name})
        assert out == ops_out
        # disable ops control
        cluster.disable_cluster_qos_ops(self.cluster_id)
        out = cluster.get_cluster_qos(self.cluster_id)
        assert out == {"enable_bw_control": False, "enable_qos": False, "combined_rw_bw_control": False, "enable_iops_control": False}

    @pytest.mark.parametrize("bw_qos_type, bw_params, ops_qos_type, ops_params, positive_tc", [
        # positive TCs
        (QOSType['PerShare'], {'export_writebw': '100MB', 'export_readbw': '200MB'}, 
         QOSType['PerShare'], {'max_export_iops': 10000}, True),
        (QOSType['PerClient'], {'client_writebw': '300MB', 'client_readbw': '400MB'},
         QOSType['PerClient'], {'max_client_iops': 2000}, True),
        (QOSType['PerShare_PerClient'],
         {'export_writebw': '100MB', 'export_readbw': '200MB', 'client_writebw': '300MB', 'client_readbw': '400MB'},
         QOSType['PerShare_PerClient'], {'max_export_iops': 3000, 'max_client_iops': 4000}, True),
        # negative TCs
        (QOSType['PerShare'], {'export_writebw': '100MB', 'export_readbw': '200MB'}, QOSType['PerClient'], {}, False),
        (QOSType['PerClient'], {'client_writebw': '300MB', 'client_readbw': '400MB'}, QOSType['PerShare'], {}, False),
        (QOSType['PerShare_PerClient'], {'export_writebw': '100MB', 'export_readbw': '200MB', 'client_writebw': '300MB', 'client_readbw': '400MB'}, QOSType['PerClient'], {'max_client_iops': 20000}, False),
        ])
    def test_cluster_qos_bw_ops(self, bw_qos_type, bw_params, ops_qos_type, ops_params, positive_tc):
        self._do_mock_test(self._do_test_cluster_qos_bw_ops, bw_qos_type, bw_params, ops_qos_type, ops_params, positive_tc)

    def _do_test_export_qos_bw_ops(self, qos_type, clust_bw_params, clust_ops_params, export_bw_params, export_ops_params):
        nfs_mod = Module('nfs', '', '')
        cluster = NFSCluster(nfs_mod)
        export_mgr = ExportMgr(nfs_mod)
        # enable cluster level bandwidth conrtol and try to enable ops control for export
        bw_obj = QOSBandwidthControl(True, combined_bw_ctrl=False, **clust_bw_params)
        cluster.enable_cluster_qos_bw(self.cluster_id, qos_type, bw_obj)
        try:
            ops_obj = QOSOpsControl(True, **export_ops_params)
            export_mgr.enable_export_qos_ops(self.cluster_id, '/cephfs_a/', ops_obj)
        except Exception:
            pass
        cluster.disable_cluster_qos_bw(self.cluster_id)
        # enable ops control for cluster and try to enable bw control for export
        ops_obj = QOSOpsControl(True, **clust_ops_params)
        cluster.enable_cluster_qos_ops(self.cluster_id, qos_type, ops_obj)
        try:
            bw_obj = QOSBandwidthControl(True, combined_bw_ctrl=False, **export_bw_params)
            export_mgr.enable_export_qos_bw(self.cluster_id, '/cephfs_a/', bw_obj)
        except Exception:
            pass
        # enbale both and verify export get
        bw_obj = QOSBandwidthControl(True, combined_bw_ctrl=False, **clust_bw_params)
        cluster.enable_cluster_qos_bw(self.cluster_id, qos_type, bw_obj)
        try:
            bw_obj = QOSBandwidthControl(True, combined_bw_ctrl=False, **export_bw_params)
            export_mgr.enable_export_qos_bw(self.cluster_id, '/cephfs_a/', bw_obj)
            ops_obj = QOSOpsControl(True, **export_ops_params)
            export_mgr.enable_export_qos_ops(self.cluster_id, '/cephfs_a/', ops_obj)
            clust_qos_conf = {'global_enable_qos': True, 'global_enable_bw_control': True, 'global_enable_iops_control': True}
        except Exception:
            req = QOS_REQ_BW_PARAMS['combined_bw_disabled'][qos_type.name]
            if sorted(export_bw_params.keys()) != sorted(req):
                return
            if qos_type.name == 'PerClient':
                return
            req = QOS_REQ_OPS_PARAMS[qos_type.name]
            if sorted(export_ops_params.keys()) != sorted(req):
                return
        out = export_mgr.get_export_qos(self.cluster_id, '/cephfs_a/')
        expected_out = {"enable_bw_control": True, "enable_qos": True, "combined_rw_bw_control": False, "enable_iops_control": True}
        bw_out = {}
        ops_out = {}
        for key in export_bw_params:
            bw_out[QOSParams[key].value] = bytes_to_human(with_units_to_int(export_bw_params[key]), mode='binary')
        for key in export_ops_params:
            ops_out[QOSParams[key].value] = export_ops_params[key]
        expected_out.update(bw_out)
        expected_out.update(ops_out)
        expected_out.update(clust_qos_conf)
        assert out == expected_out
        # disable bandwidth control of export
        export_mgr.disable_export_qos_bw(self.cluster_id, '/cephfs_a/')
        out = export_mgr.get_export_qos(self.cluster_id, '/cephfs_a/')
        ops_out.update({"enable_bw_control": False, "enable_qos": True, "combined_rw_bw_control": False, "enable_iops_control": True})
        ops_out.update(clust_qos_conf)
        assert out == ops_out
        # disable ops control of export
        export_mgr.disable_export_qos_ops(self.cluster_id, '/cephfs_a/')
        out = export_mgr.get_export_qos(self.cluster_id, '/cephfs_a/')
        clust_qos_conf.update({"enable_bw_control": False, "enable_qos": False, "combined_rw_bw_control": False, "enable_iops_control": False})
        assert out == clust_qos_conf

    @pytest.mark.parametrize("qos_type, clust_bw_params, clust_ops_params", [
        # positive TCs
        (QOSType['PerShare'], {'export_writebw': '100MB', 'export_readbw': '200MB'}, {'max_export_iops': 10000}),
        (QOSType['PerClient'], {'client_writebw': '300MB', 'client_readbw': '400MB'}, {'max_client_iops': 2000}),
        (QOSType['PerShare_PerClient'],
         {'export_writebw': '100MB', 'export_readbw': '200MB', 'client_writebw': '300MB', 'client_readbw': '400MB'}, {'max_export_iops': 3000, 'max_client_iops': 4000})
    ])
    @pytest.mark.parametrize("export_bw_params, export_ops_params", [
        ({'export_writebw': '100MB', 'export_readbw': '200MB'}, {'max_export_iops': 10000}),
        ({'client_writebw': '300MB', 'client_readbw': '400MB'}, {'max_client_iops': 12000}),
        ({'export_writebw': '100MB', 'export_readbw': '200MB', 'client_writebw': '300MB', 'client_readbw': '400MB'}, {'max_export_iops': 3000, 'max_client_iops': 4000})
        ])
    def test_export_qos_bw_ops(self, qos_type, clust_bw_params, clust_ops_params, export_bw_params, export_ops_params):
        self._do_mock_test(self._do_test_export_qos_bw_ops, qos_type, clust_bw_params, clust_ops_params, export_bw_params, export_ops_params)

    def _do_test_nfs_byok_export(self):
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        
        # Mock get_enctag_from_path to return matching enctag for validation to pass
        with mock.patch('nfs.export.get_enctag_from_path', return_value='12345'):
            conf.create_export(
                fsal_type='cephfs',
                cluster_id=self.cluster_id,
                fs_name='myfs',
                path='/',
                pseudo_path='/cephfs4',
                read_only=False,
                squash='root',
                kmip_key_id='12345'
            )
        export = conf._fetch_export(self.cluster_id, '/cephfs4')
        block = format_block(export.to_export_block())
        assert block.startswith('EXPORT {\n    kmip_key_id = "12345";')

    def test_nfs_byok_export(self):
        self._do_mock_test(self._do_test_nfs_byok_export)

    def _do_test_nfs_byok_export_enctag_validation_missing(self):
        """Test that export creation fails when kmip_key_id is provided but enctag is not set"""
        from object_format import ErrorResponse
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        # Mock get_enctag_from_path to return empty string (enctag not set)
        with mock.patch('nfs.export.get_enctag_from_path', return_value=''):
            with pytest.raises(ErrorResponse) as exc_info:
                conf.create_export(
                    fsal_type='cephfs',
                    cluster_id=self.cluster_id,
                    fs_name='myfs',
                    path='/',
                    pseudo_path='/cephfs_enctag_test1',
                    read_only=False,
                    squash='root',
                    kmip_key_id='KEY-12345'
                )
            assert "does not have an enctag set" in str(exc_info.value)

    def test_nfs_byok_export_enctag_validation_missing(self):
        self._do_mock_test(self._do_test_nfs_byok_export_enctag_validation_missing)

    def _do_test_nfs_byok_export_enctag_validation_success(self):
        """Test that export creation succeeds when kmip_key_id matches enctag"""
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        # Mock get_enctag_from_path to return matching enctag
        with mock.patch('nfs.export.get_enctag_from_path', return_value='KEY-12345'):
            conf.create_export(
                fsal_type='cephfs',
                cluster_id=self.cluster_id,
                fs_name='myfs',
                path='/',
                pseudo_path='/cephfs_enctag_test3',
                read_only=False,
                squash='root',
                kmip_key_id='KEY-12345'
            )
        export = conf._fetch_export(self.cluster_id, '/cephfs_enctag_test3')
        assert export is not None
        block = format_block(export.to_export_block())
        assert 'kmip_key_id = "KEY-12345"' in block

    def test_nfs_byok_export_enctag_validation_success(self):
        self._do_mock_test(self._do_test_nfs_byok_export_enctag_validation_success)

    def _do_test_nfs_byok_export_enctag_validation_mismatch(self):
        """Test that export creation fails when kmip_key_id does not match enctag"""
        from object_format import ErrorResponse
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        with mock.patch('nfs.export.get_enctag_from_path', return_value='ENCTAG-ACTUAL'):
            with pytest.raises(ErrorResponse) as exc_info:
                conf.create_export(
                    fsal_type='cephfs',
                    cluster_id=self.cluster_id,
                    fs_name='myfs',
                    path='/',
                    pseudo_path='/cephfs_enctag_test2',
                    read_only=False,
                    squash='root',
                    kmip_key_id='KEY-WRONG'
                )
            assert "does not match" in str(exc_info.value)
            assert "ENCTAG-ACTUAL" in str(exc_info.value)

    def test_nfs_byok_export_enctag_validation_mismatch(self):
        self._do_mock_test(self._do_test_nfs_byok_export_enctag_validation_mismatch)

    def _do_test_nfs_byok_rgw_export_rejected(self):
        """Test that kmip_key_id is rejected for RGW exports, BYOK is CephFS only"""
        from nfs.exception import NFSInvalidOperation
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        with pytest.raises(NFSInvalidOperation) as exc_info:
            conf._apply_export(self.cluster_id, {
                "export_id": 5,
                "path": "bucket",
                "pseudo": "/mybucket_byok",
                "cluster_id": self.cluster_id,
                "access_type": "RW",
                "squash": "root",
                "security_label": True,
                "protocols": [4],
                "transports": ["TCP"],
                "fsal": {
                    "name": "RGW",
                    "user_id": "nfs.foo.bucket",
                    "access_key_id": "the_access_key",
                    "secret_access_key": "the_secret_key",
                },
                "clients": [],
                "kmip_key_id": "KEY-12345",
            })
        assert "only supported for CephFS exports" in str(exc_info.value)

    def test_nfs_byok_rgw_export_rejected(self):
        self._do_mock_test(self._do_test_nfs_byok_rgw_export_rejected)

    def test_cephfs_client_pool_create_and_delete(self):
        self._do_mock_test(self._do_test_cephfs_client_pool_create_and_delete, clients_per_pool=3)

    def _do_test_cephfs_client_pool_create_and_delete(self):
        from nfs.export import get_pool_user_id, get_user_id
        from nfs.utils import ceph_users_obj_name
        from nfs.ganesha_conf import CephUsers

        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        created = []

        mock_create_key = self._create_key_mock(created)

        with mock.patch.object(conf, '_create_user_key', side_effect=mock_create_key):
            r = conf.create_export(
                fsal_type='cephfs',
                cluster_id=self.cluster_id,
                fs_name='myfs',
                path='/',
                pseudo_path='/cephfs_pool',
                read_only=False,
                squash='root',
            )
        assert r["bind"] == "/cephfs_pool"
        export = conf._fetch_export(self.cluster_id, '/cephfs_pool')
        first_pool_user = get_pool_user_id(self.cluster_id, 'myfs', 0)
        # Slot 0 is the id an export gets when client-pool mode is off.
        assert first_pool_user == f'nfs.{get_user_id(self.cluster_id, "myfs", "/")}'
        # Every other slot hangs off that same id, so a pool is recognisable
        # from its names alone and cannot collide with another filesystem's.
        assert [get_pool_user_id(self.cluster_id, 'myfs', i) for i in (1, 2)] == [
            f'{first_pool_user}.pool.1', f'{first_pool_user}.pool.2']
        assert export.fsal.user_id == first_pool_user
        assert export.fsal.cephx_key == f'key-{first_pool_user}'
        assert [c[0] for c in created] == [
            get_pool_user_id(self.cluster_id, 'myfs', 0),
            get_pool_user_id(self.cluster_id, 'myfs', 1),
            get_pool_user_id(self.cluster_id, 'myfs', 2),
        ]
        assert all(c[1] == '/' for c in created)

        raw = conf._rados(self.cluster_id).read_obj(ceph_users_obj_name(self.cluster_id))
        assert raw is not None
        ceph_users = CephUsers.from_raw(raw)
        assert len(ceph_users.users_for_fs('myfs')) == 3
        conf_raw = conf._rados(self.cluster_id).read_obj(f'conf-nfs.{self.cluster_id}')
        assert f'ceph-users-nfs.{self.cluster_id}' in conf_raw

        # Second export of the same filesystem reuses the pool users.
        created.clear()
        with mock.patch.object(conf, '_create_user_key', side_effect=mock_create_key):
            conf.create_export(
                fsal_type='cephfs',
                cluster_id=self.cluster_id,
                fs_name='myfs',
                path='/',
                pseudo_path='/cephfs_pool2',
                read_only=False,
                squash='root',
            )
        assert created == []
        # ... and share the first pool identity in their FSAL block.
        export2 = conf._fetch_export(self.cluster_id, '/cephfs_pool2')
        assert export2.fsal.user_id == first_pool_user
        assert export2.fsal.cephx_key == f'key-{first_pool_user}'

        rm_entities = []

        mock_check_mon_command = self._auth_rm_mock(rm_entities)

        with mock.patch.object(nfs_mod, 'check_mon_command', side_effect=mock_check_mon_command):
            conf.delete_export(cluster_id=self.cluster_id, pseudo_path='/cephfs_pool')
        assert rm_entities == []
        assert conf._rados(self.cluster_id).read_obj(ceph_users_obj_name(self.cluster_id)) is not None

        with mock.patch.object(nfs_mod, 'check_mon_command', side_effect=mock_check_mon_command):
            conf.delete_export(cluster_id=self.cluster_id, pseudo_path='/cephfs_pool2')
        assert set(rm_entities) == {
            f'client.{get_pool_user_id(self.cluster_id, "myfs", 0)}',
            f'client.{get_pool_user_id(self.cluster_id, "myfs", 1)}',
            f'client.{get_pool_user_id(self.cluster_id, "myfs", 2)}',
        }
        assert conf._rados(self.cluster_id).read_obj(ceph_users_obj_name(self.cluster_id)) is None
        conf_raw = conf._rados(self.cluster_id).read_obj(f'conf-nfs.{self.cluster_id}')
        assert f'ceph-users-nfs.{self.cluster_id}' not in conf_raw

    def test_cephfs_client_pool_create_reads_spec_once(self):
        self._do_mock_test(self._do_test_cephfs_client_pool_create_reads_spec_once,
                           clients_per_pool=3)

    def _do_test_cephfs_client_pool_create_reads_spec_once(self):
        """Creating an export asks the orchestrator for the cluster spec once.

        Both clients_per_pool and the RDMA setting come out of that spec, and
        the lookup is an orchestrator round trip, so it is resolved once per
        create and handed down rather than repeated per consumer.
        """
        from nfs import export as export_mod

        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)

        with mock.patch('nfs.export.get_nfs_spec_for_cluster',
                        wraps=export_mod.get_nfs_spec_for_cluster) as spec_lookup:
            conf.create_export(
                fsal_type='cephfs',
                cluster_id=self.cluster_id,
                fs_name='myfs',
                path='/',
                pseudo_path='/cephfs_pool',
                read_only=False,
                squash='root',
            )

        assert spec_lookup.call_count == 1

    def test_cephfs_client_pool_users_removed_on_orphan_cluster_delete(self):
        self._do_mock_test(
            self._do_test_cephfs_client_pool_users_removed_on_orphan_cluster_delete,
            clients_per_pool=3)

    def _do_test_cephfs_client_pool_users_removed_on_orphan_cluster_delete(self):
        """'orch rm' then 'nfs cluster rm' still retires the pool users.

        The orphan branch never reaches delete_all_exports, so nothing walks
        the exports taking their pools down, and remove_all_obj() is about to
        drop the CEPH_USERS object that names them. Read it while it is still
        there or the users outlive the cluster with nothing left recording
        them, one pool per filesystem the cluster served.
        """
        from nfs.export import get_pool_user_id

        nfs_mod = Module('nfs', '', '')
        conf = nfs_mod.export_mgr

        with mock.patch.object(conf, '_create_user_key',
                               side_effect=self._create_key_mock()):
            for fs_name, pseudo in (('myfs', '/pool_one'), ('altfs', '/pool_two')):
                conf.create_export(
                    fsal_type='cephfs',
                    cluster_id=self.cluster_id,
                    fs_name=fs_name,
                    path='/',
                    pseudo_path=pseudo,
                    read_only=False,
                    squash='none',
                )

        expected = sorted(
            f'client.{get_pool_user_id(self.cluster_id, fs, i)}'
            for fs in ('myfs', 'altfs') for i in range(3)
        )

        cluster = NFSCluster(nfs_mod)
        rm_entities: List[str] = []
        # The service is gone from the orchestrator while its objects are
        # not: exactly what 'orch rm nfs.<id>' leaves behind. has_objects()
        # and delete_config_obj() are stubbed because the fake RADOS of these
        # tests neither iterates nor removes objects.
        with mock.patch('nfs.cluster.available_clusters', return_value=[]), \
                mock.patch('nfs.rados_utils.NFSRados.has_objects',
                           return_value=True), \
                mock.patch.object(cluster, 'delete_config_obj'), \
                mock.patch.object(nfs_mod, 'check_mon_command',
                                  side_effect=self._auth_rm_mock(rm_entities)):
            cluster.delete_nfs_cluster(self.cluster_id)

        assert sorted(rm_entities) == expected

    def test_orphan_cluster_delete_without_a_client_pool(self):
        self._do_mock_test(self._do_test_orphan_cluster_delete_without_a_client_pool)

    def _do_test_orphan_cluster_delete_without_a_client_pool(self):
        """A cluster with no client pool still deletes the same way.

        The users are read off the CEPH_USERS object rather than the spec,
        and a cluster that never enabled a pool has none, so the lookup finds
        nothing to revoke and the objects go as they always did.
        """
        from nfs.utils import ceph_users_obj_name

        nfs_mod = Module('nfs', '', '')
        conf = nfs_mod.export_mgr

        with mock.patch.object(conf, '_create_user_key',
                               side_effect=self._create_key_mock()):
            conf.create_export(
                fsal_type='cephfs',
                cluster_id=self.cluster_id,
                fs_name='myfs',
                path='/',
                pseudo_path='/nopool',
                read_only=False,
                squash='none',
            )
        assert conf._rados(self.cluster_id).read_obj(
            ceph_users_obj_name(self.cluster_id)) is None

        cluster = NFSCluster(nfs_mod)
        rm_entities: List[str] = []
        deleted = mock.MagicMock()
        with mock.patch('nfs.cluster.available_clusters', return_value=[]), \
                mock.patch('nfs.rados_utils.NFSRados.has_objects',
                           return_value=True), \
                mock.patch.object(cluster, 'delete_config_obj', deleted), \
                mock.patch.object(nfs_mod, 'check_mon_command',
                                  side_effect=self._auth_rm_mock(rm_entities)):
            cluster.delete_nfs_cluster(self.cluster_id)

        assert rm_entities == []
        deleted.assert_called_once_with(self.cluster_id)

    def test_cephfs_client_pool_teardown_without_a_configured_pool(self):
        self._do_mock_test(
            self._do_test_cephfs_client_pool_teardown_without_a_configured_pool,
            clients_per_pool=3)

    def _do_test_cephfs_client_pool_teardown_without_a_configured_pool(self):
        """A pool is torn down from what is on disk, not from the spec.

        The spec says nothing useful once the users exist. It is read through
        the orchestrator, which reports no pool just as readily when it
        cannot be reached, and `orch rm nfs.x` keeps the CEPH_USERS object,
        so a cluster recreated without clients_per_pool still serves exports
        whose credentials live in it. Deleting the last export of such a
        filesystem has to revoke the whole pool either way: taking the
        no-pool path would revoke slot 0, which the export and the rest of
        the pool authenticate with, and strand the other slots.
        """
        from nfs.export import get_pool_user_id
        from nfs.utils import ceph_users_obj_name

        nfs_mod = Module('nfs', '', '')
        users_obj = ceph_users_obj_name(self.cluster_id)
        pool_ids = [get_pool_user_id(self.cluster_id, 'myfs', i) for i in range(3)]

        # The orchestrator is unreachable, and a cluster recreated without
        # clients_per_pool: indistinguishable to the nfs module, and both
        # have to leave the pool as cleaned up as a healthy cluster does.
        recreated = NFSServiceSpec(service_id=self.cluster_id, clients_per_pool=None)
        for spec in (None, recreated):
            conf = ExportMgr(nfs_mod)
            with mock.patch.object(conf, '_create_user_key',
                                   side_effect=self._create_key_mock()):
                conf.create_export(
                    fsal_type='cephfs',
                    cluster_id=self.cluster_id,
                    fs_name='myfs',
                    path='/',
                    pseudo_path='/pool',
                    read_only=False,
                    squash='none',
                )
            assert conf._rados(self.cluster_id).read_obj(users_obj) is not None

            rm_entities: List[str] = []
            with mock.patch('nfs.export.get_nfs_spec_for_cluster', return_value=spec), \
                    mock.patch.object(nfs_mod, 'check_mon_command',
                                      side_effect=self._auth_rm_mock(rm_entities)):
                conf.delete_export(cluster_id=self.cluster_id, pseudo_path='/pool')

            # Every slot revoked, not just the one the FSAL block names ...
            assert sorted(rm_entities) == sorted(f'client.{u}' for u in pool_ids)
            # ... and the object goes with them rather than being left linked
            # from the common config with nothing behind it.
            assert conf._rados(self.cluster_id).read_obj(users_obj) is None
            conf_raw = conf._rados(self.cluster_id).read_obj(f'conf-nfs.{self.cluster_id}')
            assert users_obj not in conf_raw

    def test_cephfs_client_pool_concurrent_provisioning(self):
        self._do_mock_test(self._do_test_cephfs_client_pool_concurrent_provisioning,
                           clients_per_pool=2)

    def _do_test_cephfs_client_pool_concurrent_provisioning(self):
        """Two filesystems provisioned at once end up with a pool each.

        The pools of every filesystem share one CEPH_USERS object, so a
        provisioning that read it before another one wrote it would drop the
        other's users on its own write.
        """
        import threading
        from nfs.utils import ceph_users_obj_name
        from nfs.ganesha_conf import CephUsers

        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)

        errors = []

        def provision(fs_name):
            try:
                conf._ensure_pool_users_for_fs(self.cluster_id, fs_name, 2)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=provision, args=(fs_name,))
                   for fs_name in ('fs_a', 'fs_b')]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        ceph_users = CephUsers.from_raw(
            conf._rados(self.cluster_id).read_obj(ceph_users_obj_name(self.cluster_id)))
        assert len(ceph_users.users_for_fs('fs_a')) == 2
        assert len(ceph_users.users_for_fs('fs_b')) == 2

    def test_cephfs_client_pool_enable_on_existing_exports(self):
        # The cluster is deliberately mocked without a pool: the exports are
        # made the way a cluster that predates client-pool mode has them.
        self._do_mock_test(self._do_test_cephfs_client_pool_enable_on_existing_exports)

    def _do_test_cephfs_client_pool_enable_on_existing_exports(self):
        """A pool can be enabled and grown on a cluster that already serves."""
        from nfs.export import get_pool_user_id, get_user_id
        from nfs.utils import ceph_users_obj_name
        from nfs.ganesha_conf import CephUsers

        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        created = []

        def mock_create_key(cluster_id, entity, path, fs_name):
            created.append((entity, path, fs_name))
            return f'key-{entity}'

        with mock.patch.object(conf, '_create_user_key', side_effect=mock_create_key):
            for pseudo in ('/brownfield1', '/brownfield2'):
                conf.create_export(
                    fsal_type='cephfs',
                    cluster_id=self.cluster_id,
                    fs_name='myfs',
                    path='/',
                    pseudo_path=pseudo,
                    read_only=False,
                    squash='root',
                )
            # An export that keeps its own client whatever the pool does.
            conf.create_export(
                fsal_type='cephfs',
                cluster_id=self.cluster_id,
                fs_name='myfs',
                path='/volumes',
                pseudo_path='/dedicated',
                read_only=False,
                squash='root',
                cmount_path='/volumes',
            )

        shared_user = f'nfs.{get_user_id(self.cluster_id, "myfs", "/")}'
        dedicated_user = f'nfs.{get_user_id(self.cluster_id, "myfs", "/volumes")}'
        assert conf._fetch_export(self.cluster_id, '/brownfield1').fsal.user_id == shared_user
        assert conf._fetch_export(self.cluster_id, '/brownfield2').fsal.user_id == shared_user
        assert conf._fetch_export(self.cluster_id, '/dedicated').fsal.user_id == dedicated_user
        assert conf._rados(self.cluster_id).read_obj(
            ceph_users_obj_name(self.cluster_id)) is None

        exports_before = {
            pseudo: conf._rados(self.cluster_id).read_obj(f'export-{ex.export_id}')
            for pseudo, ex in (
                (p, conf._fetch_export(self.cluster_id, p))
                for p in ('/brownfield1', '/brownfield2', '/dedicated'))
        }

        # Enable the pool on the running cluster.
        created.clear()
        self.io_mock.notify.reset_mock()
        with mock.patch.object(conf, '_create_user_key', side_effect=mock_create_key):
            grown = conf.reconcile_client_pool(self.cluster_id, 3)

        # The daemons are still configured without a pool, and ganesha refuses
        # a USERS block that is not exactly clients_per_pool wide, so nothing
        # tells them to reread: the orchestrator reconfigure that brings them
        # the new clients_per_pool is what puts the pool to use.
        assert self.io_mock.notify.call_count == 0

        pool_ids = [get_pool_user_id(self.cluster_id, 'myfs', i) for i in range(3)]
        # Slot 0 is the user the exports already carry, so it is adopted
        # rather than replaced, and only two identities are really new.
        assert pool_ids[0] == shared_user
        assert set(pool_ids) <= set(grown)
        assert [c[0] for c in created if c[2] == 'myfs'] == pool_ids
        assert all(c[1] == '/' for c in created)
        # The export with its own cmount_path never joins a pool.
        assert dedicated_user not in grown

        raw = conf._rados(self.cluster_id).read_obj(ceph_users_obj_name(self.cluster_id))
        assert raw is not None
        assert [u.user_id for u in CephUsers.from_raw(raw).users_for_fs('myfs')] == pool_ids
        conf_raw = conf._rados(self.cluster_id).read_obj(f'conf-nfs.{self.cluster_id}')
        assert f'ceph-users-nfs.{self.cluster_id}' in conf_raw

        # No export was rewritten, so no NFS client has to remount.
        for pseudo, before in exports_before.items():
            ex = conf._fetch_export(self.cluster_id, pseudo)
            assert conf._rados(self.cluster_id).read_obj(f'export-{ex.export_id}') == before

        # Re-applying the same size is a no-op.
        created.clear()
        with mock.patch.object(conf, '_create_user_key', side_effect=mock_create_key):
            assert conf.reconcile_client_pool(self.cluster_id, 3) == []
        assert created == []

        # Growing the pool only adds the slots it is short of.
        created.clear()
        self.io_mock.notify.reset_mock()
        with mock.patch.object(conf, '_create_user_key', side_effect=mock_create_key):
            grown = conf.reconcile_client_pool(self.cluster_id, 5)
        # Same again: the daemons still run with clients_per_pool=3.
        assert self.io_mock.notify.call_count == 0
        new_ids = [get_pool_user_id(self.cluster_id, 'myfs', i) for i in (3, 4)]
        assert set(new_ids) <= set(grown)
        assert [c[0] for c in created if c[2] == 'myfs'] == new_ids
        raw = conf._rados(self.cluster_id).read_obj(ceph_users_obj_name(self.cluster_id))
        assert ([u.user_id for u in CephUsers.from_raw(raw).users_for_fs('myfs')]
                == pool_ids + new_ids)

    def test_cephfs_client_pool_move_to_other_fs(self):
        self._do_mock_test(self._do_test_cephfs_client_pool_move_to_other_fs,
                           clients_per_pool=2)

    def _do_test_cephfs_client_pool_move_to_other_fs(self):
        """Moving the last root export off a filesystem keeps it usable.

        The pool the export is moving away from has no other user left, so it
        is torn down, but only once the replacement export is on disk: until
        that write lands the export object still authenticates with the old
        pool.
        """
        from nfs.export import get_pool_user_id
        from nfs.utils import ceph_users_obj_name
        from nfs.ganesha_conf import CephUsers
        from nfs.rados_utils import NFSRados

        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)

        users_obj = ceph_users_obj_name(self.cluster_id)
        fs_a_users = [get_pool_user_id(self.cluster_id, 'fs_a', i) for i in range(2)]

        mock_create_key = self._create_key_mock()

        events: List[str] = []

        mock_check_mon_command = self._auth_rm_mock(events, prefix='auth rm:')

        with mock.patch.object(conf, '_create_user_key', side_effect=mock_create_key):
            conf.create_export(
                fsal_type='cephfs',
                cluster_id=self.cluster_id,
                fs_name='fs_a',
                path='/',
                pseudo_path='/pool',
                read_only=False,
                squash='none',
            )
        export_obj = f'export-{conf._fetch_export(self.cluster_id, "/pool").export_id}'

        move = json.dumps({
            'path': '/',
            'pseudo': '/pool',
            'cluster_id': self.cluster_id,
            'access_type': 'RW',
            'squash': 'none',
            'fsal': {'name': 'CEPH', 'fs_name': 'fs_b'},
        })

        real_update_obj = NFSRados.update_obj

        def recording_update_obj(rados_obj, conf_block, obj, config_obj, should_notify=True):
            events.append(f'write:{obj}')
            return real_update_obj(rados_obj, conf_block, obj, config_obj, should_notify)

        def failing_update_obj(rados_obj, conf_block, obj, config_obj, should_notify=True):
            if obj == export_obj:
                raise Exception('rados write failed')
            return recording_update_obj(rados_obj, conf_block, obj, config_obj, should_notify)

        # A replacement export that cannot be written leaves the pool alone:
        # the export object on disk still needs those credentials.
        with mock.patch.object(conf, '_create_user_key', side_effect=mock_create_key), \
                mock.patch.object(nfs_mod, 'check_mon_command', side_effect=mock_check_mon_command), \
                mock.patch('nfs.export.NFSRados.update_obj', failing_update_obj):
            r = conf.apply_export(self.cluster_id, move)
        assert r.has_error
        # The pool of the filesystem being moved to is provisioned up front,
        # but nothing of the old pool was touched.
        assert events == [f'write:{users_obj}']
        ceph_users = CephUsers.from_raw(
            conf._rados(self.cluster_id).read_obj(users_obj))
        assert [u.user_id for u in ceph_users.users_for_fs('fs_a')] == fs_a_users
        events.clear()

        # Re-read from rados: the failed apply left the cached exports holding
        # the replacement that never made it to disk.
        conf = ExportMgr(nfs_mod)

        with mock.patch.object(conf, '_create_user_key', side_effect=mock_create_key), \
                mock.patch.object(nfs_mod, 'check_mon_command', side_effect=mock_check_mon_command), \
                mock.patch('nfs.export.NFSRados.update_obj', recording_update_obj), \
                mock.patch('nfs.export.restart_nfs_service',
                           side_effect=lambda *a, **kw: events.append('restart')):
            r = conf.apply_export(self.cluster_id, move)
        assert not r.has_error
        # The export is persisted first, then CEPH_USERS drops the pool it
        # left behind, and only then are those users revoked, so cephx is
        # never ahead of the object that names them. Ganesha is sent to
        # reread once all of it is done.
        assert events == [
            f'write:{export_obj}',
            f'write:{users_obj}',
            f'auth rm:client.{fs_a_users[0]}',
            f'auth rm:client.{fs_a_users[1]}',
            'restart',
        ]
        export = conf._fetch_export(self.cluster_id, '/pool')
        assert export.fsal.fs_name == 'fs_b'
        ceph_users = CephUsers.from_raw(
            conf._rados(self.cluster_id).read_obj(users_obj))
        assert not ceph_users.users_for_fs('fs_a')
        assert len(ceph_users.users_for_fs('fs_b')) == 2

    def test_cephfs_client_pool_rejected_apply_provisions_nothing(self):
        self._do_mock_test(
            self._do_test_cephfs_client_pool_rejected_apply_provisions_nothing,
            clients_per_pool=3)

    def _do_test_cephfs_client_pool_rejected_apply_provisions_nothing(self):
        """An apply that is going to be rejected leaves no pool behind.

        Building a cmount_path=/ export provisions its whole client pool, so
        every guard that can turn the apply down has to have run by then.
        """
        from nfs.utils import ceph_users_obj_name

        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        users_obj = ceph_users_obj_name(self.cluster_id)

        created: List = []
        mock_create_key = self._create_key_mock(created)

        conf.create_export(
            fsal_type='rgw',
            cluster_id=self.cluster_id,
            bucket='bucket',
            pseudo_path='/mybucket',
            read_only=False,
            squash='root',
        )
        export_id = conf._fetch_export(self.cluster_id, '/mybucket').export_id

        # Turning that RGW export into a CephFS one is refused ...
        swap_fsal = json.dumps({
            'export_id': export_id,
            'path': '/',
            'pseudo': '/mybucket',
            'cluster_id': self.cluster_id,
            'access_type': 'RW',
            'squash': 'root',
            'fsal': {'name': 'CEPH', 'fs_name': 'myfs'},
        })
        # ... as is an export that cannot pass validation.
        bad_pseudo = json.dumps({
            'path': '/',
            'pseudo': 'not-absolute',
            'cluster_id': self.cluster_id,
            'access_type': 'RW',
            'squash': 'root',
            'fsal': {'name': 'CEPH', 'fs_name': 'myfs'},
        })

        for payload, msg in ((swap_fsal, 'FSAL change not allowed'),
                             (bad_pseudo, 'is invalid')):
            with mock.patch.object(conf, '_create_user_key',
                                   side_effect=mock_create_key):
                r = conf.apply_export(self.cluster_id, payload)
            assert r.has_error
            assert msg in r.changes[0]['msg']

        # No identity was minted and no CEPH_USERS object was written, so
        # nothing has to be cleaned up after either refusal.
        assert created == []
        assert conf._rados(self.cluster_id).read_obj(users_obj) is None
        conf_raw = conf._rados(self.cluster_id).read_obj(f'conf-nfs.{self.cluster_id}')
        assert users_obj not in conf_raw
        # The export that was already there is untouched.
        assert conf._fetch_export(self.cluster_id, '/mybucket').fsal.name == 'RGW'

    def test_cephfs_client_pool_export_user_id_rule(self):
        self._do_mock_test(self._do_test_cephfs_client_pool_export_user_id_rule,
                           clients_per_pool=3)

    def _do_test_cephfs_client_pool_export_user_id_rule(self):
        """Client-pool mode does not widen the FSAL user_id rule.

        Slot 0 of a pool is the id a cmount_path=/ export has anyway, and the
        FSAL always carries that one, so an export naming any other slot is as
        wrong in pool mode as it is without it.
        """
        from nfs.export import get_pool_user_id

        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        slot0 = get_pool_user_id(self.cluster_id, 'myfs', 0)
        slot1 = get_pool_user_id(self.cluster_id, 'myfs', 1)

        def export_json(user_id):
            return json.dumps({
                'path': '/',
                'pseudo': '/pool',
                'cluster_id': self.cluster_id,
                'access_type': 'RW',
                'squash': 'none',
                'fsal': {'name': 'CEPH', 'fs_name': 'myfs', 'user_id': user_id},
            })

        r = conf.apply_export(self.cluster_id, export_json(slot1))
        assert r.has_error
        assert f"user_id must be '{slot0}'" in r.changes[0]['msg']

        r = conf.apply_export(self.cluster_id, export_json(slot0))
        assert not r.has_error
        export = conf._fetch_export(self.cluster_id, '/pool')
        assert export.fsal.user_id == slot0

    def test_cephfs_client_pool_resizes_stale_pool(self):
        self._do_mock_test(self._do_test_cephfs_client_pool_resizes_stale_pool,
                           clients_per_pool=3)

    def _do_test_cephfs_client_pool_resizes_stale_pool(self):
        """A pool left behind at the wrong size is resized, not reused.

        `orch rm nfs.x` leaves the CEPH_USERS object in place, so a cluster
        recreated with another clients_per_pool finds a pool of the old size.
        Ganesha rejects a USERS block that does not hold exactly
        clients_per_pool identities, so handing it back as it is would leave
        the cluster unable to load its configuration.
        """
        from nfs.export import get_pool_user_id
        from nfs.utils import ceph_users_obj_name
        from nfs.ganesha_conf import CephUser, CephUsers

        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        users_obj = ceph_users_obj_name(self.cluster_id)

        def pool_ids(fs_name, count):
            return [get_pool_user_id(self.cluster_id, fs_name, i) for i in range(count)]

        # What a cluster created with --clients-per-pool 5 left behind for
        # shrink_fs, and one created with 2 left behind for grow_fs.
        stale = CephUsers([
            CephUser(user_id, f'old-key-{user_id}', fs_name)
            for fs_name, count in (('shrink_fs', 5), ('grow_fs', 2))
            for user_id in pool_ids(fs_name, count)
        ])
        conf._write_ceph_users(self.cluster_id, stale, link=True)

        rm_entities: List[str] = []

        mock_check_mon_command = self._auth_rm_mock(rm_entities)

        mock_create_key = self._create_key_mock()

        for i, fs_name in enumerate(('shrink_fs', 'grow_fs')):
            with mock.patch.object(conf, '_create_user_key', side_effect=mock_create_key), \
                    mock.patch.object(nfs_mod, 'check_mon_command',
                                      side_effect=mock_check_mon_command):
                conf.create_export(
                    fsal_type='cephfs',
                    cluster_id=self.cluster_id,
                    fs_name=fs_name,
                    path='/',
                    pseudo_path=f'/pool{i}',
                    read_only=False,
                    squash='none',
                )

        ceph_users = CephUsers.from_raw(
            conf._rados(self.cluster_id).read_obj(users_obj))
        # Both pools now hold exactly clients_per_pool identities ...
        for fs_name in ('shrink_fs', 'grow_fs'):
            users = ceph_users.users_for_fs(fs_name)
            assert [u.user_id for u in users] == pool_ids(fs_name, 3)
            # ... with keys re-read from cephx rather than the stale ones.
            assert all(u.secret_access_key == f'key-{u.user_id}' for u in users)
        # Only the slots that the smaller pool no longer has were revoked.
        assert set(rm_entities) == {f'client.{u}' for u in pool_ids('shrink_fs', 5)[3:]}
        # The exports use slot 0 of their resized pool.
        for i, fs_name in enumerate(('shrink_fs', 'grow_fs')):
            export = conf._fetch_export(self.cluster_id, f'/pool{i}')
            assert export.fsal.user_id == get_pool_user_id(self.cluster_id, fs_name, 0)
            assert export.fsal.cephx_key == f'key-{export.fsal.user_id}'

    def test_cephfs_client_pool_reconcile_narrows_retained_pool(self):
        self._do_mock_test(
            self._do_test_cephfs_client_pool_reconcile_narrows_retained_pool,
            clients_per_pool=3)

    def _do_test_cephfs_client_pool_reconcile_narrows_retained_pool(self):
        """A recreated cluster narrows the pool its exports came back with.

        `orch rm nfs.x` keeps both the exports and the CEPH_USERS object, so
        a cluster recreated with a smaller clients_per_pool meets a pool
        wider than it asks for. No export is created again, so the resize in
        _ensure_pool_users_for_fs never runs: the orchestrator reconcile has
        to narrow the pool itself, before the daemons are given the new size.

        otherfs is served by the same cluster and came back the right size,
        so the walk that narrows myfs carries on past it with nothing left to
        retire: the slots myfs gave up are still owed to cephx at the end.
        """
        from nfs.export import get_pool_user_id
        from nfs.utils import ceph_users_obj_name
        from nfs.ganesha_conf import CephUser, CephUsers
        from nfs.rados_utils import NFSRados

        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        users_obj = ceph_users_obj_name(self.cluster_id)
        pool_ids = [get_pool_user_id(self.cluster_id, 'myfs', i) for i in range(5)]
        other_ids = [get_pool_user_id(self.cluster_id, 'otherfs', i) for i in range(3)]

        mock_create_key = self._create_key_mock()
        # myfs is exported first, so it is also the first the reconcile walks.
        for fs_name, pseudo in (('myfs', '/pool'), ('otherfs', '/otherpool')):
            with mock.patch.object(conf, '_create_user_key', side_effect=mock_create_key):
                conf.create_export(
                    fsal_type='cephfs',
                    cluster_id=self.cluster_id,
                    fs_name=fs_name,
                    path='/',
                    pseudo_path=pseudo,
                    read_only=False,
                    squash='none',
                )

        # Widen the pool of myfs to the five slots the removed cluster ran
        # with, the exports of which came back untouched. otherfs is left at
        # the size the recreated cluster asks for.
        retained = CephUsers.from_raw(conf._rados(self.cluster_id).read_obj(users_obj))
        retained.users.extend(
            CephUser(user_id, f'key-{user_id}', 'myfs') for user_id in pool_ids[3:])
        conf._write_ceph_users(self.cluster_id, retained)
        export = conf._fetch_export(self.cluster_id, '/pool')
        export_before = conf._rados(self.cluster_id).read_obj(f'export-{export.export_id}')

        events: List[str] = []
        real_update_obj = NFSRados.update_obj

        def recording_update_obj(rados_obj, conf_block, obj, config_obj, should_notify=True):
            events.append(f'write:{obj}')
            return real_update_obj(rados_obj, conf_block, obj, config_obj, should_notify)

        with mock.patch.object(conf, '_create_user_key', side_effect=mock_create_key), \
                mock.patch.object(nfs_mod, 'check_mon_command',
                                  side_effect=self._auth_rm_mock(events, prefix='auth rm:')), \
                mock.patch('nfs.export.NFSRados.update_obj', recording_update_obj):
            created = conf.reconcile_client_pool(self.cluster_id, 3)
        # Neither pool is added to, myfs only gives slots up and otherfs is
        # already the width it is asked for.
        assert not set(created) & set(pool_ids + other_ids)

        ceph_users = CephUsers.from_raw(conf._rados(self.cluster_id).read_obj(users_obj))
        users = ceph_users.users_for_fs('myfs')
        assert [u.user_id for u in users] == pool_ids[:3]
        # The slots that survive keep the keys the exports authenticate with.
        assert all(u.secret_access_key == f'key-{u.user_id}' for u in users)
        # The filesystem walked after myfs is left exactly as it was.
        assert [u.user_id for u in ceph_users.users_for_fs('otherfs')] == other_ids
        # The narrowed pool is persisted before the slots it dropped are
        # revoked: a crash in between leaves cephx users nothing points at,
        # rather than a CEPH_USERS object naming credentials that are gone.
        # The revocations are owed to myfs, which is not the filesystem the
        # walk ends on, so they only land if the walk collected them.
        assert events == [f'write:{users_obj}'] + [
            f'auth rm:client.{user_id}' for user_id in pool_ids[3:]]
        # No export was rewritten, so no NFS client has to remount.
        assert conf._rados(self.cluster_id).read_obj(
            f'export-{export.export_id}') == export_before

        # The pool is the size it was asked for, so reconciling again is a
        # no-op rather than another round of revocations.
        rm_entities: List[str] = []
        with mock.patch.object(conf, '_create_user_key', side_effect=mock_create_key), \
                mock.patch.object(nfs_mod, 'check_mon_command',
                                  side_effect=self._auth_rm_mock(rm_entities)):
            assert conf.reconcile_client_pool(self.cluster_id, 3) == []
        assert rm_entities == []

    def test_cephfs_client_pool_ignores_non_root_cmount(self):
        self._do_mock_test(self._do_test_cephfs_client_pool_ignores_non_root_cmount,
                           clients_per_pool=2)

    def _do_test_cephfs_client_pool_ignores_non_root_cmount(self):
        from nfs.export import get_pool_user_id, get_user_id
        from nfs.utils import ceph_users_obj_name

        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        created = []

        mock_create_key = self._create_key_mock(created)

        with mock.patch.object(conf, '_create_user_key', side_effect=mock_create_key):
            conf.create_export(
                fsal_type='cephfs',
                cluster_id=self.cluster_id,
                fs_name='myfs',
                path='/volumes',
                pseudo_path='/sub',
                read_only=False,
                squash='none',
                cmount_path='/volumes',
            )

        hashed_user = f'nfs.{get_user_id(self.cluster_id, "myfs", "/volumes")}'
        export = conf._fetch_export(self.cluster_id, '/sub')
        assert export.fsal.user_id == hashed_user
        assert export.fsal.cephx_key == f'key-{hashed_user}'
        assert created == [(hashed_user, '/volumes', 'myfs')]
        assert conf._rados(self.cluster_id).read_obj(
            ceph_users_obj_name(self.cluster_id)) is None

        created.clear()
        with mock.patch.object(conf, '_create_user_key', side_effect=mock_create_key):
            conf.create_export(
                fsal_type='cephfs',
                cluster_id=self.cluster_id,
                fs_name='myfs',
                path='/',
                pseudo_path='/root',
                read_only=False,
                squash='none',
            )

        root_export = conf._fetch_export(self.cluster_id, '/root')
        assert root_export.fsal.user_id == get_pool_user_id(self.cluster_id, 'myfs', 0)
        assert [c[0] for c in created] == [
            get_pool_user_id(self.cluster_id, 'myfs', 0),
            get_pool_user_id(self.cluster_id, 'myfs', 1),
        ]
        export = conf._fetch_export(self.cluster_id, '/sub')
        assert export.fsal.user_id == hashed_user

        rm_entities = []

        mock_check_mon_command = self._auth_rm_mock(rm_entities)

        with mock.patch.object(nfs_mod, 'check_mon_command', side_effect=mock_check_mon_command):
            conf.delete_export(cluster_id=self.cluster_id, pseudo_path='/root')
        assert set(rm_entities) == {
            f'client.{get_pool_user_id(self.cluster_id, "myfs", 0)}',
            f'client.{get_pool_user_id(self.cluster_id, "myfs", 1)}',
        }
        assert conf._fetch_export(self.cluster_id, '/sub').fsal.user_id == hashed_user

        rm_entities.clear()
        with mock.patch.object(nfs_mod, 'check_mon_command', side_effect=mock_check_mon_command):
            conf.delete_export(cluster_id=self.cluster_id, pseudo_path='/sub')
        assert rm_entities == [f'client.{hashed_user}']

    def test_cephfs_client_pool_export_info(self):
        self._do_mock_test(self._do_test_cephfs_client_pool_export_info,
                           clients_per_pool=3)

    def _do_test_cephfs_client_pool_export_info(self):
        """`export info` reports the pool the export draws its handles from."""
        from nfs.export import get_pool_user_id, get_user_id

        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)

        for pseudo, path, cmount in (('/root', '/', '/'),
                                     ('/sub', '/volumes', '/volumes')):
            conf.create_export(
                fsal_type='cephfs',
                cluster_id=self.cluster_id,
                fs_name='myfs',
                path=path,
                pseudo_path=pseudo,
                read_only=False,
                squash='none',
                cmount_path=cmount,
            )

        pool_ids = [get_pool_user_id(self.cluster_id, 'myfs', i) for i in range(3)]
        info = conf.get_export(self.cluster_id, '/root')
        # The FSAL only names slot 0, so the pool is what the export info adds.
        assert info['fsal']['user_id'] == pool_ids[0]
        assert info['client_pool'] == {'fs_name': 'myfs', 'users': pool_ids}
        # Keys are not part of it, no more than the FSAL cephx_key is.
        assert 'Keys' not in json.dumps(info)
        assert not any('key' in u for u in info['client_pool']['users'])

        # An export with its own cmount_path is not part of any pool.
        sub = conf.get_export(self.cluster_id, '/sub')
        assert sub['fsal']['user_id'] == f'nfs.{get_user_id(self.cluster_id, "myfs", "/volumes")}'
        assert 'client_pool' not in sub

        # An RGW export has no CephFS client at all.
        rgw = conf.get_export(self.cluster_id, '/rgw')
        assert rgw['fsal']['name'] == 'RGW'
        assert 'client_pool' not in rgw

    def test_cephfs_client_pool_single_notify_per_change(self):
        self._do_mock_test(self._do_test_cephfs_client_pool_single_notify_per_change,
                           clients_per_pool=2)

    def _do_test_cephfs_client_pool_single_notify_per_change(self):
        """Ganesha is notified once per export change, not once per object.

        A change in client-pool mode writes both the export object and the
        CEPH_USERS object. Only the export object write notifies, and it is
        always the last one, so ganesha rereads both at once and the
        configuration it sees at that point is already the final one.
        """
        from nfs.export import get_pool_user_id
        from nfs.utils import ceph_users_obj_name, conf_obj_name

        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)

        users_obj = ceph_users_obj_name(self.cluster_id)
        conf_obj = conf_obj_name(self.cluster_id)
        pool_users = [get_pool_user_id(self.cluster_id, 'myfs', i) for i in range(2)]

        # What ganesha would reread, captured at the moment it is notified.
        notified_store: List[Dict[str, str]] = []

        def snapshot_on_notify(*args: Any, **kwargs: Any) -> None:
            namespace = self.temp_store.get(self.temp_store_namespace, {})
            notified_store.append({k: o.raw for k, o in namespace.items()})

        self.io_mock.notify.side_effect = snapshot_on_notify

        self.io_mock.notify.reset_mock()
        conf.create_export(
            fsal_type='cephfs',
            cluster_id=self.cluster_id,
            fs_name='myfs',
            path='/',
            pseudo_path='/pool',
            read_only=False,
            squash='none',
        )
        # Both objects were written ...
        assert conf._rados(self.cluster_id).read_obj(users_obj) is not None
        # ... and ganesha was told about it exactly once.
        assert self.io_mock.notify.call_count == 1
        # The users the new export refers to are visible in that single reread.
        assert all(u in notified_store[0][users_obj] for u in pool_users)

        notified_store.clear()
        self.io_mock.notify.reset_mock()
        r = conf.apply_export(self.cluster_id, json.dumps({
            'path': '/',
            'pseudo': '/pool',
            'cluster_id': self.cluster_id,
            'access_type': 'RO',
            'squash': 'none',
            'fsal': {'name': 'CEPH', 'fs_name': 'myfs'},
        }))
        assert len(r.changes) == 1
        assert self.io_mock.notify.call_count == 1

        rm_entities: List[str] = []

        mock_check_mon_command = self._auth_rm_mock(rm_entities)

        notified_store.clear()
        self.io_mock.notify.reset_mock()
        with mock.patch.object(nfs_mod, 'check_mon_command',
                               side_effect=mock_check_mon_command):
            conf.delete_export(cluster_id=self.cluster_id, pseudo_path='/pool')
        # Export object removed and the pool torn down: still a single notify.
        assert conf._rados(self.cluster_id).read_obj(users_obj) is None
        assert self.io_mock.notify.call_count == 1
        assert set(rm_entities) == {f'client.{u}' for u in pool_users}
        # The pool is torn down before the notifying removal, so what ganesha
        # rereads mentions neither the deleted export nor the deleted users.
        snapshot = notified_store[0]
        assert users_obj not in snapshot
        assert users_obj not in snapshot[conf_obj]
        assert not any(u in blob for u in pool_users for blob in snapshot.values())


    def test_cephfs_client_pool_rotate_key(self):
        self._do_mock_test(self._do_test_cephfs_client_pool_rotate_key, clients_per_pool=2)

    def _do_test_cephfs_client_pool_rotate_key(self):
        from nfs.export import get_pool_user_id, get_user_id
        from nfs.utils import ceph_users_obj_name
        from nfs.ganesha_conf import CephUsers

        nfs_mod = Module('nfs', '', '')
        conf = nfs_mod.export_mgr
        cluster = nfs_mod.nfs

        pool0 = get_pool_user_id(self.cluster_id, 'myfs', 0)
        pool1 = get_pool_user_id(self.cluster_id, 'myfs', 1)
        entity0 = f'client.{pool0}'
        entity1 = f'client.{pool1}'
        hashed_user = f'nfs.{get_user_id(self.cluster_id, "myfs", "/volumes")}'
        hashed_entity = f'client.{hashed_user}'

        conf.create_export(
            fsal_type='cephfs',
            cluster_id=self.cluster_id,
            fs_name='myfs',
            path='/',
            pseudo_path='/poolrot',
            read_only=False,
            squash='none',
        )
        conf.create_export(
            fsal_type='cephfs',
            cluster_id=self.cluster_id,
            fs_name='myfs',
            path='/volumes',
            pseudo_path='/sub',
            read_only=False,
            squash='none',
            cmount_path='/volumes',
        )

        mock_create_user_key = self._rotate_key_mock()

        with mock.patch.object(nfs_mod, 'check_mon_command') as check_mon, \
                mock.patch.object(ExportMgr, '_create_user_key', mock_create_user_key), \
                mock.patch('nfs.cluster.redeploy_nfs_service') as redeploy:
            result = cluster.rotate_keys(
                cluster_id=self.cluster_id,
                auth_entities=[entity0],
                key_type='aes256k',
            )

        check_mon.assert_called_once_with({
            'prefix': 'auth rotate',
            'entity': entity0,
            'key_type': 'aes256k',
        })
        redeploy.assert_not_called()
        assert result['rotated'] == [entity0]
        assert result['export_keys'] == [entity0]
        assert result['daemon_keys'] == []
        assert result['service_redeployed'] is False
        # Slot 0 is refreshed both in CEPH_USERS and in the FSAL that carries
        # it, but only the export is reported: updated_exports holds pseudo
        # paths, never CephX ids.
        assert result['updated_exports'] == ['/poolrot']

        export = conf._fetch_export(self.cluster_id, '/poolrot')
        assert export.fsal.user_id == pool0
        assert export.fsal.cephx_key == f'rotated-{pool0}'
        hashed = conf._fetch_export(self.cluster_id, '/sub')
        assert hashed.fsal.user_id == hashed_user
        assert hashed.fsal.cephx_key == 'thekeyforclientabc'

        ceph_users = CephUsers.from_raw(
            conf._rados(self.cluster_id).read_obj(ceph_users_obj_name(self.cluster_id)))
        keys = {u.user_id: u.secret_access_key for u in ceph_users.users}
        assert keys[pool0] == f'rotated-{pool0}'
        assert keys[pool1] == 'thekeyforclientabc'
        # Rotation rewrites an object that the common config already links to:
        # its url must not be appended a second time.
        conf_raw = conf._rados(self.cluster_id).read_obj(f'conf-nfs.{self.cluster_id}')
        assert conf_raw.count(ceph_users_obj_name(self.cluster_id)) == 1

        with mock.patch.object(nfs_mod, 'check_mon_command') as check_mon, \
                mock.patch.object(ExportMgr, '_create_user_key', mock_create_user_key), \
                mock.patch('nfs.cluster.redeploy_nfs_service') as redeploy:
            result = cluster.rotate_keys(
                cluster_id=self.cluster_id,
                auth_entities=[hashed_entity],
            )

        check_mon.assert_called_once_with({
            'prefix': 'auth rotate',
            'entity': hashed_entity,
        })
        redeploy.assert_not_called()
        assert result['export_keys'] == [hashed_entity]
        assert result['updated_exports'] == ['/sub']
        hashed = conf._fetch_export(self.cluster_id, '/sub')
        assert hashed.fsal.cephx_key == f'rotated-{hashed_user}'
        ceph_users = CephUsers.from_raw(
            conf._rados(self.cluster_id).read_obj(ceph_users_obj_name(self.cluster_id)))
        keys = {u.user_id: u.secret_access_key for u in ceph_users.users}
        assert keys[pool0] == f'rotated-{pool0}'
        assert keys[pool1] == 'thekeyforclientabc'

        daemon_entity = 'client.nfs.foo.0.0.host.hash-rgw'
        cluster_entity = 'client.nfs.foo'
        auth_ls = {
            'auth_dump': [
                {'entity': cluster_entity},
                {'entity': daemon_entity},
                {'entity': entity0},
                {'entity': entity1},
                {'entity': hashed_entity},
                {'entity': 'client.nfs.other.fs.deadbeef'},
            ]
        }

        def mock_check_mon(cmd):
            if cmd.get('prefix') == 'auth ls':
                return mock.Mock(stdout=json.dumps(auth_ls), retval=0, stderr='')
            return mock.Mock(stdout='', retval=0, stderr='')

        with mock.patch.object(nfs_mod, 'check_mon_command', side_effect=mock_check_mon) as check_mon, \
                mock.patch.object(ExportMgr, '_create_user_key', mock_create_user_key), \
                mock.patch('nfs.cluster.redeploy_nfs_service') as redeploy:
            result = cluster.rotate_keys(
                cluster_id=self.cluster_id,
                all_daemon_and_export_keys=True,
                key_type='aes256k',
            )

        rotate_calls = [
            c.args[0] for c in check_mon.call_args_list
            if c.args[0].get('prefix') == 'auth rotate'
        ]
        rotated = [c['entity'] for c in rotate_calls]
        assert sorted(rotated) == sorted(
            [cluster_entity, daemon_entity, entity0, entity1, hashed_entity])
        redeploy.assert_called_once_with(nfs_mod, self.cluster_id)
        assert result['service_redeployed'] is True
        assert sorted(result['daemon_keys']) == sorted([cluster_entity, daemon_entity])
        assert sorted(result['export_keys']) == sorted([entity0, entity1, hashed_entity])
        export = conf._fetch_export(self.cluster_id, '/poolrot')
        assert export.fsal.user_id == pool0
        assert export.fsal.cephx_key == f'rotated-{pool0}'
        ceph_users = CephUsers.from_raw(
            conf._rados(self.cluster_id).read_obj(ceph_users_obj_name(self.cluster_id)))
        keys = {u.user_id: u.secret_access_key for u in ceph_users.users}
        assert keys[pool0] == f'rotated-{pool0}'
        assert keys[pool1] == f'rotated-{pool1}'

    def test_cephfs_client_pool_rotate_key_notifies_when_export_fails(self):
        self._do_mock_test(
            self._do_test_cephfs_client_pool_rotate_key_notifies_when_export_fails,
            clients_per_pool=2)

    def _do_test_cephfs_client_pool_rotate_key_notifies_when_export_fails(self):
        """The pool write stays quiet only if an export write follows it.

        Rotating a pool-only slot together with a non-root export defers the
        notification to that export's write. When the export refresh fails the
        pool's new key is persisted with nobody left to announce it, so the
        rotation has to notify ganesha itself.
        """
        import object_format
        from nfs.export import get_pool_user_id, get_user_id
        from nfs.utils import ceph_users_obj_name
        from nfs.ganesha_conf import CephUsers

        nfs_mod = Module('nfs', '', '')
        conf = nfs_mod.export_mgr
        cluster = nfs_mod.nfs

        conf.create_export(
            fsal_type='cephfs',
            cluster_id=self.cluster_id,
            fs_name='myfs',
            path='/',
            pseudo_path='/poolrot',
            read_only=False,
            squash='none',
        )
        conf.create_export(
            fsal_type='cephfs',
            cluster_id=self.cluster_id,
            fs_name='myfs',
            path='/volumes',
            pseudo_path='/sub',
            read_only=False,
            squash='none',
            cmount_path='/volumes',
        )
        pool1 = get_pool_user_id(self.cluster_id, 'myfs', 1)
        hashed_user = f'nfs.{get_user_id(self.cluster_id, "myfs", "/volumes")}'

        mock_create_user_key = self._rotate_key_mock(failing_entity=hashed_user)

        self.io_mock.notify.reset_mock()
        with mock.patch.object(nfs_mod, 'check_mon_command'), \
                mock.patch.object(ExportMgr, '_create_user_key', mock_create_user_key), \
                mock.patch('nfs.cluster.redeploy_nfs_service') as redeploy:
            with pytest.raises(object_format.ErrorResponse) as e:
                cluster.rotate_keys(
                    cluster_id=self.cluster_id,
                    auth_entities=[f'client.{pool1}', f'client.{hashed_user}'],
                )

        # The export that could not be refreshed is reported as stale ...
        assert '/sub' in str(e.value)
        redeploy.assert_not_called()
        # ... and its failed write is exactly the one that was supposed to
        # notify ganesha, so the rotation notifies in its place.
        assert self.io_mock.notify.call_count == 1

        ceph_users = CephUsers.from_raw(
            conf._rados(self.cluster_id).read_obj(ceph_users_obj_name(self.cluster_id)))
        keys = {u.user_id: u.secret_access_key for u in ceph_users.users}
        # The notification is only owed because this key was persisted.
        assert keys[pool1] == f'rotated-{pool1}'
        assert conf._fetch_export(self.cluster_id, '/sub').fsal.cephx_key == \
            'thekeyforclientabc'


class TestNFSClusterIngressPlacement:
    cluster_id = 'mynfs'
    virtual_ip = '192.168.1.100/24'
    nfs_placement = '2 host1 host2'
    ingress_placement = '3 host3 host4 host5'

    def test_create_nfs_cluster_passes_ingress_placement(self):
        mgr = MagicMock()
        cluster = NFSCluster(mgr)
        with mock.patch('nfs.cluster.create_ganesha_pool'), \
                mock.patch.object(cluster, 'create_empty_rados_obj'), \
                mock.patch('nfs.cluster.available_clusters', return_value=[]), \
                mock.patch.object(cluster, '_call_orch_apply_nfs') as mock_apply:
            cluster.create_nfs_cluster(
                cluster_id=self.cluster_id,
                placement='host1',
                virtual_ip=self.virtual_ip,
                ingress=True,
                ingress_placement=self.ingress_placement,
            )
            mock_apply.assert_called_once()
            assert mock_apply.call_args.kwargs['ingress_placement'] == self.ingress_placement


@pytest.mark.parametrize(
    "path,expected",
    [
        ("/foo/bar/baz", "/foo/bar/baz"),
        ("/foo/bar/baz/", "/foo/bar/baz"),
        ("/foo/bar/baz ", "/foo/bar/baz"),
        ("/foo/./bar/baz", "/foo/bar/baz"),
        ("/foo/bar/baz/..", "/foo/bar"),
        ("//foo/bar/baz", "/foo/bar/baz"),
        ("", ""),
    ]
)
def test_normalize_path(path, expected):
    assert normalize_path(path) == expected


def test_ganesha_validate_squash():
    """Check error handling of internal validation function for squash value."""
    from nfs.ganesha_conf import _validate_squash
    from nfs.exception import NFSInvalidOperation

    _validate_squash("root")
    with pytest.raises(NFSInvalidOperation):
        _validate_squash("toot")


def test_ganesha_validate_access_type():
    """Check error handling of internal validation function for access type value."""
    from nfs.ganesha_conf import _validate_access_type
    from nfs.exception import NFSInvalidOperation

    for ok in ("rw", "ro", "none"):
        _validate_access_type(ok)
    with pytest.raises(NFSInvalidOperation):
        _validate_access_type("any")


def test_ceph_users_block_roundtrip():
    from nfs.ganesha_conf import CephUsers, CephUser, GaneshaConfParser, format_block

    users = CephUsers([
        CephUser('nfs.myc.fs_a.80bb977a', 'key0', 'fs_a'),
        CephUser('nfs.myc.fs_a.80bb977a.pool.1', 'key1', 'fs_a'),
        CephUser('nfs.myc.fs_b.deadbeef', 'key2', 'fs_b'),
    ])
    raw = format_block(users.to_block())
    parsed = CephUsers.from_raw(raw)
    assert len(parsed.users_for_fs('fs_a')) == 2
    assert len(parsed.users_for_fs('fs_b')) == 1
    # The pool keeps its order, so slot 0 stays the identity of the exports.
    assert [u.user_id for u in parsed.users_for_fs('fs_a')] == [
        'nfs.myc.fs_a.80bb977a', 'nfs.myc.fs_a.80bb977a.pool.1']
    assert parsed.users_for_fs('fs_a')[0].secret_access_key == 'key0'
    parsed.remove_fs('fs_a')
    assert not parsed.users_for_fs('fs_a')
    assert len(parsed.users_for_fs('fs_b')) == 1

    # One USERS block per filesystem, holding the pool as parallel lists.
    blocks = GaneshaConfParser(raw).parse()
    assert len(blocks) == 1
    assert blocks[0].block_name == 'CEPH_USERS'
    assert [b.block_name for b in blocks[0].blocks] == ['USERS', 'USERS']
    fs_a_block = blocks[0].blocks[0]
    assert fs_a_block.values['filesystem'] == 'fs_a'
    assert fs_a_block.values['userids'] == ['nfs.myc.fs_a.80bb977a', 'nfs.myc.fs_a.80bb977a.pool.1']
    assert fs_a_block.values['keys'] == ['key0', 'key1']
    # A single-user pool writes a scalar stanza, and has to read back as a pool.
    fs_b_block = blocks[0].blocks[1]
    assert fs_b_block.values['userids'] == 'nfs.myc.fs_b.deadbeef'
    assert [u.user_id for u in CephUsers.from_raw(raw).users_for_fs('fs_b')] == [
        'nfs.myc.fs_b.deadbeef']


def test_ceph_users_block_rejects_mismatched_keys():
    """Userids and Keys are positional, so a short Keys list is unreadable.

    Ganesha rejects such a block, and there is no way to tell which identity
    lost its key, so reading one has to fail loudly rather than hand back a
    pool that cannot authenticate.
    """
    from nfs.exception import NFSException
    from nfs.ganesha_conf import CephUsers

    raw = '''
CEPH_USERS {
    USERS {
        Userids = "nfs.myc.cephfs.80bb977a", "nfs.myc.cephfs.80bb977a.pool.1";
        Keys = "AQCkey1==";
        filesystem = "cephfs";
    }
}
'''
    with pytest.raises(NFSException) as e:
        CephUsers.from_raw(raw)
    assert '2 Userids but 1 Keys' in str(e.value)
    # The message has to say what to do about it.
    assert 'ceph-users-nfs.<cluster_id>' in str(e.value)


class TestCephfsClientForMgr:
    @pytest.fixture(autouse=True)
    def clear_cephfs_client_cache(self):
        cephfs_client_for_mgr.cache_clear()
        yield
        cephfs_client_for_mgr.cache_clear()

    def test_cephfs_client_for_mgr_returns_same_instance(self):
        mgr = MagicMock()
        with mock.patch('nfs.utils.CephfsClient') as mock_cephfs_cls:
            mock_cephfs_cls.return_value = MagicMock()
            first = cephfs_client_for_mgr(mgr)
            second = cephfs_client_for_mgr(mgr)
            assert first is second
            mock_cephfs_cls.assert_called_once_with(mgr)

    def test_multiple_earmark_resolvers_share_cached_cephfs_client(self):
        mgr = MagicMock()
        with mock.patch('nfs.utils.CephfsClient') as mock_cephfs_cls:
            mock_cephfs_cls.return_value = MagicMock()
            cached = cephfs_client_for_mgr(mgr)
            r1 = mgr_util.CephFSEarmarkResolver(mgr=mgr, client=cached)
            r2 = mgr_util.CephFSEarmarkResolver(mgr=mgr, client=cephfs_client_for_mgr(mgr))
            assert r1._cephfs_client is r2._cephfs_client
            mock_cephfs_cls.assert_called_once_with(mgr)
