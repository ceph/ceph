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
from nfs import Module
from nfs.export import ExportMgr, normalize_path
from nfs.ganesha_conf import GaneshaConfParser, Export, RawBlock
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
        user_id = "nfs.foo.1";
        filesystem = "a";
        secret_access_key = "AQCjU+hgjyReLBAAddJa0Dza/ZHqjX5+JiePMA==";
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
        user_id = "nfs.foo.1";
        filesystem = "a";
        secret_access_key = "AQCjU+hgjyReLBAAddJa0Dza/ZHqjX5+JiePMA==";
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

    conf_nfs_foo = f'''
%url "rados://{NFS_POOL_NAME}/{cluster_id}/export-1"

%url "rados://{NFS_POOL_NAME}/{cluster_id}/export-2"'''

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

        assert len(exports) == 2

        self._validate_export_1([e for e in exports if e.export_id == 1][0])
        self._validate_export_2([e for e in exports if e.export_id == 2][0])

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

        # again, but without export_id
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
            'sectype': ["krb5p", "krb5i", "sys"],
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
        assert export.export_id == 3
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
        assert export.export_id == 4
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

    def test_remove_export(self) -> None:
        self._do_mock_test(self._do_test_remove_export)

    def _do_test_remove_export(self) -> None:
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)
        assert len(conf.exports[self.cluster_id]) == 2
        conf.delete_export(cluster_id=self.cluster_id,
                           pseudo_path="/rgw")
        exports = conf.exports[self.cluster_id]
        assert len(exports) == 1
        assert exports[0].export_id == 1

    def test_create_export_rgw_bucket(self):
        self._do_mock_test(self._do_test_create_export_rgw_bucket)

    def _do_test_create_export_rgw_bucket(self):
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)

        ls = conf.list_exports(cluster_id=self.cluster_id)
        assert len(ls) == 2

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
        assert len(ls) == 3

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

    def test_create_export_rgw_bucket_user(self):
        self._do_mock_test(self._do_test_create_export_rgw_bucket_user)

    def _do_test_create_export_rgw_bucket_user(self):
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)

        ls = conf.list_exports(cluster_id=self.cluster_id)
        assert len(ls) == 2

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
        assert len(ls) == 3

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
        assert len(ls) == 2

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
        assert len(ls) == 3

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

    def _do_test_create_export_cephfs(self):
        nfs_mod = Module('nfs', '', '')
        conf = ExportMgr(nfs_mod)

        ls = conf.list_exports(cluster_id=self.cluster_id)
        assert len(ls) == 2

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
        assert len(ls) == 3

        export = conf._fetch_export('foo', '/cephfs2')
        assert export.export_id
        assert export.path == "/"
        assert export.pseudo == "/cephfs2"
        assert export.access_type == "none"
        assert export.squash == "none"
        assert export.protocols == [4]
        assert export.transports == ["TCP"]
        assert export.fsal.name == "CEPH"
        assert export.fsal.user_id == "nfs.foo.3"
        assert export.fsal.cephx_key == "thekeyforclientabc"
        assert len(export.clients) == 1
        assert export.clients[0].squash == 'root'
        assert export.clients[0].access_type == 'rw'
        assert export.clients[0].addresses == ["192.168.1.0/8"]
        assert export.cluster_id == self.cluster_id

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
