import pytest
from ceph_volume.devices import lvm


class TestLVM(object):

    def test_main_spits_help_with_no_arguments(self, capsys):
        lvm.main.LVM([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'Use LVM and LVM-based technologies like dmcache to deploy' in stdout

    def test_main_shows_activate_subcommands(self, capsys):
        lvm.main.LVM([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'activate ' in stdout
        assert 'Discover and mount' in stdout

    def test_main_shows_prepare_subcommands(self, capsys):
        lvm.main.LVM([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'prepare ' in stdout
        assert 'Format an LVM device' in stdout


class TestPrepareDevice(object):

    def test_cannot_use_device(self):
        with pytest.raises(RuntimeError) as error:
            lvm.prepare.Prepare([]).prepare_device(
                    '/dev/var/foo', 'data', 'asdf', '0')
        assert 'Cannot use device (/dev/var/foo)' in str(error)
        assert 'A vg/lv path or an existing device is needed' in str(error)

class TestPrepare(object):

    def test_main_spits_help_with_no_arguments(self, capsys):
        lvm.prepare.Prepare([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'Prepare an OSD by assigning an ID and FSID' in stdout

    def test_main_shows_full_help(self, capsys):
        with pytest.raises(SystemExit):
            lvm.prepare.Prepare(argv=['--help']).main()
        stdout, stderr = capsys.readouterr()
        assert 'Use the filestore objectstore' in stdout
        assert 'Use the bluestore objectstore' in stdout
        assert 'A physical device or logical' in stdout


class TestGetJournalLV(object):

    @pytest.mark.parametrize('arg', ['', '///', None, '/dev/sda1'])
    def test_no_journal_on_invalid_path(self, monkeypatch, arg):
        monkeypatch.setattr(lvm.prepare.api, 'get_lv', lambda **kw: False)
        prepare = lvm.prepare.Prepare([])
        assert prepare.get_lv(arg) is None

    def test_no_journal_lv_found(self, monkeypatch):
        # patch it with 0 so we know we are getting to get_lv
        monkeypatch.setattr(lvm.prepare.api, 'get_lv', lambda **kw: 0)
        prepare = lvm.prepare.Prepare([])
        assert prepare.get_lv('vg/lv') == 0


class TestActivate(object):

    def test_main_spits_help_with_no_arguments(self, capsys):
        lvm.activate.Activate([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'Activate OSDs by discovering them with' in stdout

    def test_main_shows_full_help(self, capsys):
        with pytest.raises(SystemExit):
            lvm.activate.Activate(argv=['--help']).main()
        stdout, stderr = capsys.readouterr()
        assert 'optional arguments' in stdout
        assert 'positional arguments' in stdout

