import pytest
from textwrap import dedent
import json
from ceph_volume.util import prepare
from ceph_volume.util.prepare import system
from ceph_volume import conf
from ceph_volume.tests.conftest import Factory


class TestCheckID(object):

    def test_false_if_id_is_none(self):
        assert not prepare.check_id(None)

    def test_returncode_is_not_zero(self, monkeypatch):
        monkeypatch.setattr('ceph_volume.process.call', lambda *a, **kw: ('', '', 1))
        with pytest.raises(RuntimeError):
            prepare.check_id(1)

    def test_id_does_exist(self, monkeypatch):
        stdout = dict(nodes=[
            dict(id=0),
        ])
        stdout = ['', json.dumps(stdout)]
        monkeypatch.setattr('ceph_volume.process.call', lambda *a, **kw: (stdout, '', 0))
        result = prepare.check_id(0)
        assert result

    def test_id_does_not_exist(self, monkeypatch):
        stdout = dict(nodes=[
            dict(id=0),
        ])
        stdout = ['', json.dumps(stdout)]
        monkeypatch.setattr('ceph_volume.process.call', lambda *a, **kw: (stdout, '', 0))
        result = prepare.check_id(1)
        assert not result

    def test_invalid_osd_id(self, monkeypatch):
        stdout = dict(nodes=[
            dict(id=0),
        ])
        stdout = ['', json.dumps(stdout)]
        monkeypatch.setattr('ceph_volume.process.call', lambda *a, **kw: (stdout, '', 0))
        result = prepare.check_id("foo")
        assert not result


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


mkfs_filestore_flags = [
    'ceph-osd',
    '--cluster',
    '--osd-objectstore', 'filestore',
    '--mkfs',
    '-i',
    '--monmap',
    '--keyfile', '-', # goes through stdin
    '--osd-data',
    '--osd-journal',
    '--osd-uuid',
    '--setuser', 'ceph',
    '--setgroup', 'ceph'
]


class TestOsdMkfsFilestore(object):

    @pytest.mark.parametrize('flag', mkfs_filestore_flags)
    def test_keyring_is_used(self, fake_call, monkeypatch, flag):
        monkeypatch.setattr(system, 'chown', lambda path: True)
        prepare.osd_mkfs_filestore(1, 'asdf', keyring='secret')
        assert flag in fake_call.calls[0]['args'][0]


class TestOsdMkfsBluestore(object):

    def test_keyring_is_added(self, fake_call, monkeypatch):
        monkeypatch.setattr(system, 'chown', lambda path: True)
        prepare.osd_mkfs_bluestore(1, 'asdf', keyring='secret')
        assert '--keyfile' in fake_call.calls[0]['args'][0]

    def test_keyring_is_not_added(self, fake_call, monkeypatch):
        monkeypatch.setattr(system, 'chown', lambda path: True)
        prepare.osd_mkfs_bluestore(1, 'asdf')
        assert '--keyfile' not in fake_call.calls[0]['args'][0]

    def test_wal_is_added(self, fake_call, monkeypatch):
        monkeypatch.setattr(system, 'chown', lambda path: True)
        prepare.osd_mkfs_bluestore(1, 'asdf', wal='/dev/smm1')
        assert '--bluestore-block-wal-path' in fake_call.calls[0]['args'][0]
        assert '/dev/smm1' in fake_call.calls[0]['args'][0]

    def test_db_is_added(self, fake_call, monkeypatch):
        monkeypatch.setattr(system, 'chown', lambda path: True)
        prepare.osd_mkfs_bluestore(1, 'asdf', db='/dev/smm2')
        assert '--bluestore-block-db-path' in fake_call.calls[0]['args'][0]
        assert '/dev/smm2' in fake_call.calls[0]['args'][0]


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
        result = prepare._normalize_mount_flags(flags)
        assert result == 'rw,auto,exec'

    @pytest.mark.parametrize("flags", string_mount_values)
    def test_normalize_strings(self, flags):
        result = prepare._normalize_mount_flags(flags)
        assert result == 'rw,auto,exec'
