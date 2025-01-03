import pytest
from textwrap import dedent
import json
from ceph_volume.util import prepare
from ceph_volume.util.prepare import system
from ceph_volume import conf
from ceph_volume.tests.conftest import Factory
from ceph_volume import objectstore
from mock.mock import patch


class TestOSDIDAvailable(object):

    def test_false_if_id_is_none(self):
        assert not prepare.osd_id_available(None)

    def test_returncode_is_not_zero(self, monkeypatch):
        monkeypatch.setattr('ceph_volume.process.call', lambda *a, **kw: ('', '', 1))
        with pytest.raises(RuntimeError):
            prepare.osd_id_available(1)

    def test_id_does_exist_but_not_available(self, monkeypatch):
        stdout = dict(nodes=[
            dict(id=0, status="up"),
        ])
        stdout = ['', json.dumps(stdout)]
        monkeypatch.setattr('ceph_volume.process.call', lambda *a, **kw: (stdout, '', 0))
        result = prepare.osd_id_available(0)
        assert not result

    def test_id_does_not_exist(self, monkeypatch):
        stdout = dict(nodes=[
            dict(id=0),
        ])
        stdout = ['', json.dumps(stdout)]
        monkeypatch.setattr('ceph_volume.process.call', lambda *a, **kw: (stdout, '', 0))
        result = prepare.osd_id_available(1)
        assert result

    def test_returns_true_when_id_is_destroyed(self, monkeypatch):
        stdout = dict(nodes=[
            dict(id=0, status="destroyed"),
        ])
        stdout = ['', json.dumps(stdout)]
        monkeypatch.setattr('ceph_volume.process.call', lambda *a, **kw: (stdout, '', 0))
        result = prepare.osd_id_available(0)
        assert result


class TestFormatDevice(object):

    def test_include_force(self, fake_run, monkeypatch):
        monkeypatch.setattr(conf, 'ceph', Factory(get_list=lambda *a, **kw: []))
        prepare.format_device('/dev/sxx')
        flags = fake_run.calls[0]['args'][0]
        assert '-f' in flags

    def test_device_is_always_appended(self, fake_run, conf_ceph):
        conf_ceph(get_list=lambda *a, **kw: [])
        prepare.format_device('/dev/sxx')
        flags = fake_run.calls[0]['args'][0]
        assert flags[-1] == '/dev/sxx'

    def test_extra_flags_are_added(self, fake_run, conf_ceph):
        conf_ceph(get_list=lambda *a, **kw: ['--why-yes'])
        prepare.format_device('/dev/sxx')
        flags = fake_run.calls[0]['args'][0]
        assert '--why-yes' in flags

    def test_default_options(self, conf_ceph_stub, fake_run):
        conf_ceph_stub(dedent("""[global]
        fsid = 1234lkjh1234"""))
        conf.cluster = 'ceph'
        prepare.format_device('/dev/sda1')
        expected = [
            'mkfs', '-t', 'xfs',
            '-f', '-i', 'size=2048',  # default flags
            '/dev/sda1']
        assert expected == fake_run.calls[0]['args'][0]

    def test_multiple_options_are_used(self, conf_ceph_stub, fake_run):
        conf_ceph_stub(dedent("""[global]
        fsid = 1234lkjh1234
        [osd]
        osd mkfs options xfs = -f -i size=1024"""))
        conf.cluster = 'ceph'
        prepare.format_device('/dev/sda1')
        expected = [
            'mkfs', '-t', 'xfs',
            '-f', '-i', 'size=1024',
            '/dev/sda1']
        assert expected == fake_run.calls[0]['args'][0]

    def test_multiple_options_will_get_the_force_flag(self, conf_ceph_stub, fake_run):
        conf_ceph_stub(dedent("""[global]
        fsid = 1234lkjh1234
        [osd]
        osd mkfs options xfs = -i size=1024"""))
        conf.cluster = 'ceph'
        prepare.format_device('/dev/sda1')
        expected = [
            'mkfs', '-t', 'xfs',
            '-f', '-i', 'size=1024',
            '/dev/sda1']
        assert expected == fake_run.calls[0]['args'][0]

    def test_underscore_options_are_used(self, conf_ceph_stub, fake_run):
        conf_ceph_stub(dedent("""[global]
        fsid = 1234lkjh1234
        [osd]
        osd_mkfs_options_xfs = -i size=128"""))
        conf.cluster = 'ceph'
        prepare.format_device('/dev/sda1')
        expected = [
            'mkfs', '-t', 'xfs',
            '-f', '-i', 'size=128',
            '/dev/sda1']
        assert expected == fake_run.calls[0]['args'][0]


class TestOsdMkfsBluestore(object):
    def setup_method(self):
        conf.cluster = 'ceph'

    def test_keyring_is_added(self, fake_call, monkeypatch):
        monkeypatch.setattr(system, 'chown', lambda path: True)
        o = objectstore.baseobjectstore.BaseObjectStore([])
        o.osd_id = '1'
        o.osd_fsid = 'asdf'
        o.osd_mkfs()
        assert '--keyfile' in fake_call.calls[2]['args'][0]

    def test_keyring_is_not_added(self, fake_call, monkeypatch, factory):
        args = factory(dmcrypt=False)
        monkeypatch.setattr(system, 'chown', lambda path: True)
        o = objectstore.bluestore.BlueStore([])
        o.args = args
        o.osd_id = '1'
        o.osd_fsid = 'asdf'
        o.osd_mkfs()
        assert '--keyfile' not in fake_call.calls[0]['args'][0]

    def test_wal_is_added(self, fake_call, monkeypatch, objectstore_bluestore, factory):
        args = factory(dmcrypt=False)
        monkeypatch.setattr(system, 'chown', lambda path: True)
        bs = objectstore_bluestore(objecstore='bluestore',
                        osd_id='1',
                        osd_fid='asdf',
                        wal_device_path='/dev/smm1',
                        cephx_secret='foo',
                        dmcrypt=False)
        bs.args = args
        bs.osd_mkfs()
        assert '--bluestore-block-wal-path' in fake_call.calls[2]['args'][0]
        assert '/dev/smm1' in fake_call.calls[2]['args'][0]

    def test_db_is_added(self, fake_call, monkeypatch, factory):
        args = factory(dmcrypt=False)
        monkeypatch.setattr(system, 'chown', lambda path: True)
        bs = objectstore.bluestore.BlueStore([])
        bs.args = args
        bs.db_device_path = '/dev/smm2'
        bs.osd_mkfs()
        assert '--bluestore-block-db-path' in fake_call.calls[2]['args'][0]
        assert '/dev/smm2' in fake_call.calls[2]['args'][0]


class TestMountOSD(object):

    def test_default_options(self, conf_ceph_stub, fake_run):
        conf_ceph_stub(dedent("""[global]
        fsid = 1234lkjh1234"""))
        conf.cluster = 'ceph'
        prepare.mount_osd('/dev/sda1', 1)
        expected = [
            'mount', '-t', 'xfs', '-o',
            'rw,noatime,inode64', # default flags
            '/dev/sda1', '/var/lib/ceph/osd/ceph-1']
        assert expected == fake_run.calls[0]['args'][0]

    def test_mount_options_are_used(self, conf_ceph_stub, fake_run):
        conf_ceph_stub(dedent("""[global]
        fsid = 1234lkjh1234
        [osd]
        osd mount options xfs = rw"""))
        conf.cluster = 'ceph'
        prepare.mount_osd('/dev/sda1', 1)
        expected = [
            'mount', '-t', 'xfs', '-o',
            'rw',
            '/dev/sda1', '/var/lib/ceph/osd/ceph-1']
        assert expected == fake_run.calls[0]['args'][0]

    def test_multiple_whitespace_options_are_used(self, conf_ceph_stub, fake_run):
        conf_ceph_stub(dedent("""[global]
        fsid = 1234lkjh1234
        [osd]
        osd mount options xfs = rw auto exec"""))
        conf.cluster = 'ceph'
        prepare.mount_osd('/dev/sda1', 1)
        expected = [
            'mount', '-t', 'xfs', '-o',
            'rw,auto,exec',
            '/dev/sda1', '/var/lib/ceph/osd/ceph-1']
        assert expected == fake_run.calls[0]['args'][0]

    def test_multiple_comma_whitespace_options_are_used(self, conf_ceph_stub, fake_run):
        conf_ceph_stub(dedent("""[global]
        fsid = 1234lkjh1234
        [osd]
        osd mount options xfs = rw, auto, exec"""))
        conf.cluster = 'ceph'
        prepare.mount_osd('/dev/sda1', 1)
        expected = [
            'mount', '-t', 'xfs', '-o',
            'rw,auto,exec',
            '/dev/sda1', '/var/lib/ceph/osd/ceph-1']
        assert expected == fake_run.calls[0]['args'][0]

    def test_underscore_mount_options_are_used(self, conf_ceph_stub, fake_run):
        conf_ceph_stub(dedent("""[global]
        fsid = 1234lkjh1234
        [osd]
        osd mount options xfs = rw"""))
        conf.cluster = 'ceph'
        prepare.mount_osd('/dev/sda1', 1)
        expected = [
            'mount', '-t', 'xfs', '-o',
            'rw',
            '/dev/sda1', '/var/lib/ceph/osd/ceph-1']
        assert expected == fake_run.calls[0]['args'][0]


ceph_conf_mount_values = [
    ['rw,', 'auto,' 'exec'],
    ['rw', 'auto', 'exec'],
    [' rw ', ' auto ', ' exec '],
    ['rw,', 'auto,', 'exec,'],
    [',rw ', ',auto ', ',exec,'],
    [',rw,', ',auto,', ',exec,'],
]

string_mount_values = [
    'rw, auto exec ',
    'rw  auto exec',
    ',rw, auto, exec,',
    ' rw  auto exec ',
    ' rw,auto,exec ',
    'rw,auto,exec',
    ',rw,auto,exec,',
    'rw,auto,exec ',
    'rw, auto, exec ',
]


class TestNormalizeFlags(object):
    # a bit overkill since most of this is already tested in prepare.mount_osd
    # tests

    @pytest.mark.parametrize("flags", ceph_conf_mount_values)
    def test_normalize_lists(self, flags):
        result = sorted(prepare._normalize_mount_flags(flags).split(','))
        assert ','.join(result) == 'auto,exec,rw'

    @pytest.mark.parametrize("flags", string_mount_values)
    def test_normalize_strings(self, flags):
        result = sorted(prepare._normalize_mount_flags(flags).split(','))
        assert ','.join(result) == 'auto,exec,rw'

    @pytest.mark.parametrize("flags", ceph_conf_mount_values)
    def test_normalize_extra_flags(self, flags):
        result = prepare._normalize_mount_flags(flags, extras=['discard'])
        assert sorted(result.split(',')) == ['auto', 'discard', 'exec', 'rw']

    @pytest.mark.parametrize("flags", ceph_conf_mount_values)
    def test_normalize_duplicate_extra_flags(self, flags):
        result = prepare._normalize_mount_flags(flags, extras=['rw', 'discard'])
        assert sorted(result.split(',')) == ['auto', 'discard', 'exec', 'rw']

    @pytest.mark.parametrize("flags", string_mount_values)
    def test_normalize_strings_flags(self, flags):
        result = sorted(prepare._normalize_mount_flags(flags, extras=['discard']).split(','))
        assert ','.join(result) == 'auto,discard,exec,rw'

    @pytest.mark.parametrize("flags", string_mount_values)
    def test_normalize_strings_duplicate_flags(self, flags):
        result = sorted(prepare._normalize_mount_flags(flags, extras=['discard','rw']).split(','))
        assert ','.join(result) == 'auto,discard,exec,rw'

@patch('ceph_volume.util.prepare.create_key', return_value='fake-secret')
class TestMkfsBluestore(object):

    def test_non_zero_exit_status(self, m_create_key, stub_call, monkeypatch, objectstore_bluestore):
        conf.cluster = 'ceph'
        monkeypatch.setattr('ceph_volume.util.prepare.system.chown', lambda x: True)
        stub_call(([], [], 1))
        bs = objectstore_bluestore(osd_id='1',
                                   osd_fsid='asdf-1234',
                                   cephx_secret='keyring')
        with pytest.raises(RuntimeError) as error:
            bs.osd_mkfs()
        assert "Command failed with exit code 1" in str(error.value)

    def test_non_zero_exit_formats_command_correctly(self, m_create_key, stub_call, monkeypatch, objectstore_bluestore):
        conf.cluster = 'ceph'
        monkeypatch.setattr('ceph_volume.util.prepare.system.chown', lambda x: True)
        stub_call(([], [], 1))
        bs = objectstore_bluestore(osd_id='1',
                                   osd_fsid='asdf-1234',
                                   cephx_secret='keyring')
        with pytest.raises(RuntimeError) as error:
            bs.osd_mkfs()
        expected = ' '.join([
            'ceph-osd',
            '--cluster',
            'ceph',
            '--osd-objectstore', 'bluestore', '--mkfs',
            '-i', '1', '--monmap', '/var/lib/ceph/osd/ceph-1/activate.monmap',
            '--keyfile', '-', '--osd-data', '/var/lib/ceph/osd/ceph-1/',
            '--osd-uuid', 'asdf-1234',
            '--setuser', 'ceph', '--setgroup', 'ceph'])
        assert expected in str(error.value)
