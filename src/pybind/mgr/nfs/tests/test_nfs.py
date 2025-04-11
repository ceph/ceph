# flake8: noqa
import json
import pytest
from typing import Optional, Tuple, Iterator, List, Any

from contextlib import contextmanager
from unittest import mock
from unittest.mock import MagicMock
from mgr_module import MgrModule, NFS_POOL_NAME

from rados import ObjectNotFound

from ceph.deployment.service_spec import NFSServiceSpec
from ceph.utils import with_units_to_int, bytes_to_human
from nfs import Module
from nfs.export import ExportMgr, normalize_path
from nfs.ganesha_conf import GaneshaConfParser, Export
from nfs.qos_conf import (
    RawBlock,
    QOS,
    QOSType,
    QOSParams,
    QOS_REQ_BW_PARAMS,
    QOSBandwidthControl,
    QOSOpsControl,
    QOS_REQ_OPS_PARAMS)
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
    Protocols = 4;
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
    qos_type = 3;
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
        "max_client_read_bw": "4.0MB",
        "max_client_write_bw": "3.0MB",
        "max_export_read_bw": "2.0MB",
        "max_export_write_bw": "2.0MB",
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
        "max_client_read_bw": "4.0MB",
        "max_client_write_bw": "3.0MB",
        "max_export_read_bw": "2.0MB",
        "max_export_write_bw": "2.0MB",
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

        def read(self, _: Optional[int]) -> bytes:
            return self.raw.encode('utf-8')

        def stat(self) -> Tuple[int, None]:
            return len(self.raw), None

    def _ioctx_write_full_mock(self, key: str, content: bytes) -> None:
        if key not in self.temp_store[self.temp_store_namespace]:
            self.temp_store[self.temp_store_namespace][key] = \
                TestNFS.RObject(key, content.decode('utf-8'))
        else:
            self.temp_store[self.temp_store_namespace][key].raw = content.decode('utf-8')

    def _ioctx_remove_mock(self, key: str) -> None:
        del self.temp_store[self.temp_store_namespace][key]

    def _ioctx_list_objects_mock(self) -> List['TestNFS.RObject']:
        r = [obj for _, obj in self.temp_store[self.temp_store_namespace].items()]
        return r

    def _ioctl_stat_mock(self, key):
        return self.temp_store[self.temp_store_namespace][key].stat()

    def _ioctl_read_mock(self, key: str, size: Optional[Any] = None) -> bytes:
        if key not in self.temp_store[self.temp_store_namespace]:
            raise ObjectNotFound
        return self.temp_store[self.temp_store_namespace][key].read(size)

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
    def _mock_orchestrator(self, enable: bool) -> Iterator:
        self.io_mock = MagicMock()
        self.io_mock.set_namespace.side_effect = self._ioctx_set_namespace_mock
        self.io_mock.read = self._ioctl_read_mock
        self.io_mock.stat = self._ioctl_stat_mock
        self.io_mock.list_objects.side_effect = self._ioctx_list_objects_mock
        self.io_mock.write_full.side_effect = self._ioctx_write_full_mock
        self.io_mock.remove_object.side_effect = self._ioctx_remove_mock

        # mock nfs services
        orch_nfs_services = [
            ServiceDescription(spec=NFSServiceSpec(service_id=self.cluster_id))
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
                "recoverybackend": "rados_cluster",
                "minor_versions": [1, 2]
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
           Minor_Versions = 1, 2;
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
        assert export.protocols == [4]
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

    def _do_mock_test(self, func, *args) -> None:
        with self._mock_orchestrator(True):
            func(*args)
            self._reset_temp_store()

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
            'sectype': ["krb5p", "krb5i", "sys", "mtls", "tls"],
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
        assert info["sectype"] == ["krb5p", "krb5i", "sys", "mtls", "tls"]

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
        assert export.protocols == [3, 4]
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
        assert export.protocols == [3, 4]
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
        assert export.protocols == [3, 4]
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
        assert export.protocols == [3, 4]
        assert export.transports == ["TCP"]
        assert export.fsal.name == "CEPH"
        assert export.fsal.user_id == "nfs.foo.myfs.86ca58ef"
        assert export.fsal.cephx_key == "thekeyforclientabc"
        assert len(export.clients) == 1
        assert export.clients[0].squash == 'root'
        assert export.clients[0].access_type == 'rw'
        assert export.clients[0].addresses == ["192.168.1.0/8"]
        assert export.cluster_id == self.cluster_id
    
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
        assert export.protocols == [3, 4]
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
        assert out == {"foo": {"virtual_ip": None, "backend": []}}

    def test_cluster_info(self):
        self._do_mock_test(self._do_test_cluster_info)

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

    def test_qos_from_dict(self):
        qos = QOS.from_dict(self.qos_cluster_dict, True)
        assert qos.enable_qos == True
        assert qos.bw_obj.enable_bw_ctrl == True
        assert isinstance(qos.qos_type, QOSType)
        assert qos.bw_obj.export_writebw == 2000000
        assert qos.bw_obj.export_readbw == 2000000
        assert qos.bw_obj.client_writebw == 3000000
        assert qos.bw_obj.client_readbw == 4000000

        qos = QOS.from_dict(self.qos_export_dict)
        assert qos.enable_qos == True
        assert qos.bw_obj.enable_bw_ctrl == True
        assert qos.qos_type is None
        assert qos.bw_obj.export_writebw == 2000000
        assert qos.bw_obj.export_readbw == 2000000
        assert qos.bw_obj.client_writebw == 3000000
        assert qos.bw_obj.client_readbw == 4000000

    @pytest.mark.parametrize("qos_block, qos_dict, qos_dict_bw_in_bytes", [
        (qos_cluster_block, qos_cluster_dict, qos_cluster_dict_bw_in_bytes),
        (qos_export_block, qos_export_dict, qos_export_dict_bw_in_bytes)
        ])
    def test_qos_from_block(self, qos_block, qos_dict, qos_dict_bw_in_bytes):
        blocks = GaneshaConfParser(qos_block).parse()
        assert isinstance(blocks, list)
        assert len(blocks) == 1
        qos = QOS.from_qos_block(blocks[0], True)
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
            expected_out[QOSParams[key].value] = bytes_to_human(with_units_to_int(params[key]))
        assert out == expected_out
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
        clust_qos_conf = {'cluster_enable_qos': True, 'cluster_enable_bw_control': True, 'cluster_enable_iops_control': False}
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
            expected_out[QOSParams[key].value] = bytes_to_human(with_units_to_int(export_params[key]))
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
        clust_qos_conf = {'cluster_enable_qos': True, 'cluster_enable_bw_control': False, 'cluster_enable_iops_control': True}
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
            bw_out[QOSParams[key].value] = bytes_to_human(with_units_to_int(bw_params[key]))
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
            clust_qos_conf = {'cluster_enable_qos': True, 'cluster_enable_bw_control': True, 'cluster_enable_iops_control': True}
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
            bw_out[QOSParams[key].value] = bytes_to_human(with_units_to_int(export_bw_params[key]))
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
