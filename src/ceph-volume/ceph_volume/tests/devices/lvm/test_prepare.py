import pytest
from argparse import Namespace
from ceph_volume.devices import lvm
from unittest.mock import patch

class TestLVM(object):

    def test_main_spits_help_with_no_arguments(self, capsys):
        lvm.main.LVM([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'Use LVM and LVM-based technologies to deploy' in stdout

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


@patch('ceph_volume.util.prepare.create_key', return_value='fake-secret')
class TestPrepare(object):

    def setup_method(self):
        self.p = lvm.prepare.Prepare([])

    def test_main_spits_help_with_no_arguments(self, m_create_key, capsys):
        lvm.prepare.Prepare([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'Prepare an OSD by assigning an ID and FSID' in stdout

    def test_main_shows_full_help(self, m_create_key, capsys):
        with pytest.raises(SystemExit):
            lvm.prepare.Prepare(argv=['--help']).main()
        stdout, stderr = capsys.readouterr()
        assert 'Use the bluestore objectstore' in stdout
        assert 'A physical device or logical' in stdout

    def test_invalid_osd_id_passed(self, m_create_key):
        with pytest.raises(SystemExit):
            lvm.prepare.Prepare(argv=['--osd-id', 'foo']).main()

    def test_seastore_rejects_block_db(self, m_create_key):
        p = lvm.prepare.Prepare([])
        p.args = Namespace(objectstore='seastore', bluestore=False,
                           block_db='/dev/sdb', block_wal=None,
                           seastore_secondary=[])
        with pytest.raises(RuntimeError) as exc:
            p.main()
        assert '--block.db cannot be used with --objectstore seastore' in str(exc.value)

    def test_seastore_rejects_block_wal(self, m_create_key):
        p = lvm.prepare.Prepare([])
        p.args = Namespace(objectstore='seastore', bluestore=False,
                           block_db=None, block_wal='/dev/sdc',
                           seastore_secondary=[])
        with pytest.raises(RuntimeError) as exc:
            p.main()
        assert '--block.wal cannot be used with --objectstore seastore' in str(exc.value)

    def test_bluestore_rejects_seastore_secondary(self, m_create_key):
        p = lvm.prepare.Prepare([])
        p.args = Namespace(objectstore='bluestore', bluestore=False,
                           block_db=None, block_wal=None,
                           seastore_secondary=[('/dev/sdb', 'HDD')])
        with pytest.raises(RuntimeError) as exc:
            p.main()
        assert '--seastore-secondary cannot be used with --objectstore bluestore' in str(exc.value)


class TestActivate(object):

    def test_main_spits_help_with_no_arguments(self, capsys):
        lvm.activate.Activate([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'Activate OSDs by discovering them with' in stdout

    def test_main_shows_full_help(self, capsys):
        with pytest.raises(SystemExit):
            lvm.activate.Activate(argv=['--help']).main()
        stdout, stderr = capsys.readouterr()
        assert 'positional' in stdout
        assert '-h' in stdout or '--help' in stdout
