import pytest
from ceph_volume.devices import lvm


class TestCreate(object):

    def test_main_spits_help_with_no_arguments(self, capsys):
        lvm.create.Create([]).main()
        stdout, stderr = capsys.readouterr()
        assert 'Create an OSD by assigning an ID and FSID' in stdout

    def test_main_shows_full_help(self, capsys):
        with pytest.raises(SystemExit):
            lvm.create.Create(argv=['--help']).main()
        stdout, stderr = capsys.readouterr()
        assert 'Use the filestore objectstore' in stdout
        assert 'Use the bluestore objectstore' in stdout
        assert 'A physical device or logical' in stdout

    def test_excludes_filestore_bluestore_flags(self, capsys):
        with pytest.raises(SystemExit):
            lvm.create.Create(argv=['--data', '/dev/sdfoo', '--filestore', '--bluestore']).main()
        stdout, sterr = capsys.readouterr()
        expected = 'Cannot use --filestore (filestore) with --bluestore (bluestore)'
        assert expected in stdout

    def test_excludes_other_filestore_bluestore_flags(self, capsys):
        with pytest.raises(SystemExit):
            lvm.create.Create(argv=[
                '--bluestore', '--data', '/dev/sdfoo',
                '--journal', '/dev/sf14',
            ]).main()
        stdout, sterr = capsys.readouterr()
        expected = 'Cannot use --bluestore (bluestore) with --journal (filestore)'
        assert expected in stdout

    def test_excludes_block_and_journal_flags(self, capsys):
        with pytest.raises(SystemExit):
            lvm.create.Create(argv=[
                '--bluestore', '--data', '/dev/sdfoo', '--block.db', 'vg/ceph1',
                '--journal', '/dev/sf14',
            ]).main()
        stdout, sterr = capsys.readouterr()
        expected = 'Cannot use --block.db (bluestore) with --journal (filestore)'
        assert expected in stdout
