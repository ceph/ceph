from ceph_volume.util import prepare
from ceph_volume.util.prepare import system
from ceph_volume import conf
from ceph_volume.tests.conftest import Factory


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
