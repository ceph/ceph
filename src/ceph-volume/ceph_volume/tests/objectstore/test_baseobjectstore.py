import pytest
import os
from unittest.mock import patch, Mock, call
from ceph_volume.objectstore.baseobjectstore import BaseObjectStore
from ceph_volume.util import system


@patch('ceph_volume.objectstore.baseobjectstore.prepare_utils.create_key', Mock(return_value=['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']))
class TestBaseObjectStore:
    def test_init_dmcrypt(self, factory):
        args = factory(dmcrypt=True)
        bo = BaseObjectStore(args)
        assert bo.encrypted == 1
        assert bo.cephx_lockbox_secret == ['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']
        assert bo.secrets['cephx_lockbox_secret'] == ['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']

    @patch('ceph_volume.process.call', Mock(return_value=(['c6798f59-01'], '', 0)))
    def test_get_ptuuid_ok(self):
        """
        Test that the ptuuid is returned
        """
        assert BaseObjectStore([]).get_ptuuid('/dev/sda') == 'c6798f59-01'

    @patch('ceph_volume.process.call', Mock(return_value=('', '', 0)))
    def test_get_ptuuid_raises_runtime_error(self, capsys):
        """
        Test that the ptuuid is returned
        """
        with pytest.raises(RuntimeError) as error:
            bo = BaseObjectStore([])
            bo.get_ptuuid('/dev/sda')
        stdout, stderr = capsys.readouterr()
        assert 'blkid could not detect a PARTUUID for device: /dev/sda' in stderr
        assert str(error.value) == 'unable to use device'

    @patch.dict('os.environ', {'CEPH_VOLUME_OSDSPEC_AFFINITY': 'foo'})
    def test_get_osdspec_affinity(self):
        assert BaseObjectStore([]).get_osdspec_affinity() == 'foo'

    def test_pre_prepare(self):
        with pytest.raises(NotImplementedError):
            BaseObjectStore([]).pre_prepare()

    def test_prepare_data_device(self):
        with pytest.raises(NotImplementedError):
            BaseObjectStore([]).prepare_data_device('foo', 'bar')

    def test_safe_prepare(self):
        with pytest.raises(NotImplementedError):
            BaseObjectStore([]).safe_prepare(args=None)

    @patch('ceph_volume.util.prepare.create_osd_path')
    @patch('ceph_volume.util.prepare.link_block')
    @patch('ceph_volume.util.prepare.get_monmap')
    @patch('ceph_volume.util.prepare.write_keyring')
    def test_prepare_osd_req(self, m_write_keyring, m_get_monmap, m_link_block, m_create_osd_path):
        bo = BaseObjectStore([])
        bo.osd_id = '123'
        bo.block_device_path = '/dev/foo'
        bo.prepare_osd_req()
        assert m_create_osd_path.mock_calls == [call('123', tmpfs=True)]
        assert m_link_block.mock_calls == [call('/dev/foo', '123')]
        assert m_get_monmap.mock_calls == [call('123')]
        assert m_write_keyring.mock_calls == [call('123', ['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ=='])]

    def test_prepare(self):
        with pytest.raises(NotImplementedError):
            BaseObjectStore([]).prepare()

    def test_prepare_dmcrypt(self):
        with pytest.raises(NotImplementedError):
            BaseObjectStore([]).prepare_dmcrypt()

    def test_cluster_fsid_from_args(self, factory):
        args = factory(cluster_fsid='abcd')
        bo = BaseObjectStore(args)
        assert bo.get_cluster_fsid() == 'abcd'

    def test_cluster_fsid_from_conf(self, conf_ceph_stub, factory):
        args = factory(cluster_fsid=None)
        conf_ceph_stub('[global]\nfsid = abcd-123')
        bo = BaseObjectStore([])
        bo.args = args
        assert bo.get_cluster_fsid() == 'abcd-123'

    @patch('ceph_volume.conf.cluster', 'ceph')
    def test_get_osd_path(self):
        bo = BaseObjectStore([])
        bo.osd_id = '123'
        assert bo.get_osd_path() == '/var/lib/ceph/osd/ceph-123/'

    @patch('ceph_volume.conf.cluster', 'ceph')
    def test_build_osd_mkfs_cmd_base(self):
        bo = BaseObjectStore([])
        bo.osd_path = '/var/lib/ceph/osd/ceph-123/'
        bo.osd_fsid = 'abcd-1234'
        bo.objectstore = 'my-fake-objectstore'
        bo.osd_id = '123'
        bo.monmap = '/etc/ceph/ceph.monmap'
        result = bo.build_osd_mkfs_cmd()

        assert result == ['ceph-osd',
                          '--cluster',
                          'ceph',
                          '--osd-objectstore',
                          'my-fake-objectstore',
                          '--mkfs', '-i', '123',
                          '--monmap',
                          '/etc/ceph/ceph.monmap',
                          '--keyfile', '-',
                          '--osd-data',
                          '/var/lib/ceph/osd/ceph-123/',
                          '--osd-uuid', 'abcd-1234',
                          '--setuser', 'ceph',
                          '--setgroup', 'ceph']

    @patch('ceph_volume.conf.cluster', 'ceph')
    def test_build_osd_mkfs_cmd_disables_discard(self):
        bo = BaseObjectStore([])
        bo.osd_path = '/var/lib/ceph/osd/ceph-123/'
        bo.osd_fsid = 'abcd-1234'
        bo.objectstore = 'bluestore'
        bo.osd_id = '123'
        bo.monmap = '/etc/ceph/ceph.monmap'
        bo.disable_bluestore_discard = True
        result = bo.build_osd_mkfs_cmd()

        assert result == ['ceph-osd',
                          '--cluster',
                          'ceph',
                          '--osd-objectstore',
                          'bluestore',
                          '--mkfs', '-i', '123',
                          '--monmap',
                          '/etc/ceph/ceph.monmap',
                          '--keyfile', '-',
                          '--osd-data',
                          '/var/lib/ceph/osd/ceph-123/',
                          '--osd-uuid', 'abcd-1234',
                          '--setuser', 'ceph',
                          '--setgroup', 'ceph']

    def test_osd_mkfs_ok(self, monkeypatch, fake_call, objectstore):
        args = objectstore(dmcrypt=False)
        bo = BaseObjectStore(args)
        bo.get_osd_path = lambda: '/var/lib/ceph/osd/ceph-123/'
        bo.build_osd_mkfs_cmd = lambda: ['ceph-osd', '--mkfs', 'some', 'fake', 'args']
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        bo.osd_mkfs()
        assert fake_call.calls == [
            {
                'args': (['ceph-osd',
                          '--mkfs',
                          'some',
                          'fake',
                          'args'],),
                'kwargs': {
                    'stdin': ['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ=='],
                    'terminal_verbose': True,
                    'show_command': True,
                    'env': None}
                }
            ]

    def test_osd_mkfs_sets_temp_ceph_conf_when_discard_disabled(self, monkeypatch, tmpfile, objectstore):
        args = objectstore(dmcrypt=False)
        bo = BaseObjectStore(args)
        bo.objectstore = 'bluestore'
        bo.disable_bluestore_discard = True
        bo.get_osd_path = lambda: '/var/lib/ceph/osd/ceph-123/'
        bo.build_osd_mkfs_cmd = lambda: ['ceph-osd', '--mkfs', 'some', 'fake', 'args']
        monkeypatch.setattr(system, 'chown', lambda path: 0)

        # Provide a base config file so the helper can copy and append.
        base_conf = tmpfile(contents='[global]\nfsid = deadbeef\n')
        monkeypatch.setattr('ceph_volume.objectstore.baseobjectstore.conf.path', base_conf)

        captured = {}

        def fake_call(cmd, **kwargs):
            env = kwargs.get('env')
            assert env and 'CEPH_CONF' in env
            conf_path = env['CEPH_CONF']
            captured['conf_path'] = conf_path
            assert os.path.exists(conf_path)
            with open(conf_path, 'r', encoding='utf-8') as fp:
                data = fp.read()
            assert 'bluestore_discard_on_mkfs = false' in data
            return ([], [], 0)

        monkeypatch.setattr('ceph_volume.objectstore.baseobjectstore.process.call', fake_call)

        bo.osd_mkfs()

        # Temp conf should be removed after mkfs returns.
        assert 'conf_path' in captured
        assert not os.path.exists(captured['conf_path'])

    @patch('ceph_volume.process.call', Mock(return_value=([], [], 999)))
    def test_osd_mkfs_fails(self, monkeypatch):
        bo = BaseObjectStore([])
        bo.get_osd_path = lambda: '/var/lib/ceph/osd/ceph-123/'
        bo.build_osd_mkfs_cmd = lambda: ['ceph-osd', '--mkfs', 'some', 'fake', 'args']
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        with pytest.raises(RuntimeError) as error:
            bo.osd_mkfs()
        assert str(error.value) == 'Command failed with exit code 999: ceph-osd --mkfs some fake args'

    @patch('time.sleep', Mock())
    @patch('ceph_volume.process.call', return_value=([], [], 11))
    def test_osd_mkfs_fails_EWOULDBLOCK(self, m_call, monkeypatch, objectstore):
        args = objectstore(dmcrypt=False)
        bo = BaseObjectStore(args)
        bo.get_osd_path = lambda: '/var/lib/ceph/osd/ceph-123/'
        bo.build_osd_mkfs_cmd = lambda: ['ceph-osd', '--mkfs', 'some', 'fake', 'args']
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        bo.osd_mkfs()
        assert m_call.call_count == 5

    def test_activate(self):
        with pytest.raises(NotImplementedError):
            BaseObjectStore([]).activate()

    @patch('ceph_volume.objectstore.baseobjectstore.prepare_utils.create_key', Mock(return_value=['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']))
    def setup_method(self, m_create_key):
        self.b = BaseObjectStore([])
        self.b.osd_mkfs_cmd = ['binary', 'arg1']

    def test_add_objectstore_opts_wal_device_path(self, monkeypatch):
        monkeypatch.setattr('ceph_volume.util.system.chown', lambda path: 0)
        self.b.wal_device_path = '/dev/nvme0n1'
        self.b.add_objectstore_opts()
        assert self.b.osd_mkfs_cmd == ['binary', 'arg1', '--bluestore-block-wal-path', '/dev/nvme0n1']

    def test_add_objectstore_opts_db_device_path(self, monkeypatch):
        monkeypatch.setattr('ceph_volume.util.system.chown', lambda path: 0)
        self.b.db_device_path = '/dev/ssd1'
        self.b.add_objectstore_opts()
        assert self.b.osd_mkfs_cmd == ['binary', 'arg1', '--bluestore-block-db-path', '/dev/ssd1']

    def test_add_objectstore_opts_osdspec_affinity(self, monkeypatch):
        monkeypatch.setattr('ceph_volume.util.system.chown', lambda path: 0)
        self.b.get_osdspec_affinity = lambda: 'foo'
        self.b.add_objectstore_opts()
        assert self.b.osd_mkfs_cmd == ['binary', 'arg1', '--osdspec-affinity', 'foo']