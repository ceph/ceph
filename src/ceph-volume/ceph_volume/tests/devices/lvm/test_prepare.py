import pytest
from ceph_volume.devices import lvm
from mock.mock import patch, Mock


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

    def test_cannot_use_device(self, factory):
        args = factory(data='/dev/var/foo')
        with pytest.raises(RuntimeError) as error:
            p = lvm.prepare.Prepare([])
            p.args = args
            p.prepare_data_device( 'data', '0')
        assert 'Cannot use device (/dev/var/foo)' in str(error.value)
        assert 'A vg/lv path or an existing device is needed' in str(error.value)


class TestGetClusterFsid(object):

    def test_fsid_is_passed_in(self, factory):
        args = factory(cluster_fsid='aaaa-1111')
        prepare_obj = lvm.prepare.Prepare([])
        prepare_obj.args = args
        assert prepare_obj.get_cluster_fsid() == 'aaaa-1111'

    def test_fsid_is_read_from_ceph_conf(self, factory, conf_ceph_stub):
        conf_ceph_stub('[global]\nfsid = bbbb-2222')
        prepare_obj = lvm.prepare.Prepare([])
        prepare_obj.args = factory(cluster_fsid=None)
        assert prepare_obj.get_cluster_fsid() == 'bbbb-2222'


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

    def test_excludes_filestore_bluestore_flags(self, capsys, device_info):
        device_info()
        with pytest.raises(SystemExit):
            lvm.prepare.Prepare(argv=['--data', '/dev/sdfoo', '--filestore', '--bluestore']).main()
        stdout, stderr = capsys.readouterr()
        expected = 'Cannot use --filestore (filestore) with --bluestore (bluestore)'
        assert expected in stderr

    def test_excludes_other_filestore_bluestore_flags(self, capsys, device_info):
        device_info()
        with pytest.raises(SystemExit):
            lvm.prepare.Prepare(argv=[
                '--bluestore', '--data', '/dev/sdfoo',
                '--journal', '/dev/sf14',
            ]).main()
        stdout, stderr = capsys.readouterr()
        expected = 'Cannot use --bluestore (bluestore) with --journal (filestore)'
        assert expected in stderr

    def test_excludes_block_and_journal_flags(self, capsys, device_info):
        device_info()
        with pytest.raises(SystemExit):
            lvm.prepare.Prepare(argv=[
                '--bluestore', '--data', '/dev/sdfoo', '--block.db', 'vg/ceph1',
                '--journal', '/dev/sf14',
            ]).main()
        stdout, stderr = capsys.readouterr()
        expected = 'Cannot use --block.db (bluestore) with --journal (filestore)'
        assert expected in stderr

    def test_journal_is_required_with_filestore(self, is_root, monkeypatch, device_info):
        monkeypatch.setattr("os.path.exists", lambda path: True)
        device_info()
        with pytest.raises(SystemExit) as error:
            lvm.prepare.Prepare(argv=['--filestore', '--data', '/dev/sdfoo']).main()
        expected = '--journal is required when using --filestore'
        assert expected in str(error.value)

    @patch('ceph_volume.devices.lvm.prepare.api.is_ceph_device')
    def test_safe_prepare_osd_already_created(self, m_is_ceph_device):
        m_is_ceph_device.return_value = True
        with pytest.raises(RuntimeError) as error:
            prepare = lvm.prepare.Prepare(argv=[])
            prepare.args = Mock()
            prepare.args.data = '/dev/sdfoo'
            prepare.get_lv = Mock()
            prepare.safe_prepare()
            expected = 'skipping {}, it is already prepared'.format('/dev/sdfoo')
            assert expected in str(error.value)

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
