import pytest
from ceph_volume.devices import lvm
from ceph_volume.api import lvm as api
from mock.mock import patch, Mock


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
        assert 'Use the bluestore objectstore' in stdout
        assert 'A physical device or logical' in stdout

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

    def test_setup_device_device_name_is_none(self):
        result = lvm.prepare.Prepare([]).setup_device(device_type='data', device_name=None, tags={'ceph.type': 'data'}, size=0, slots=None)
        assert result == ('', '', {'ceph.type': 'data'})

    @patch('ceph_volume.api.lvm.Volume.set_tags')
    @patch('ceph_volume.devices.lvm.prepare.api.get_single_lv')
    def test_setup_device_lv_passed(self, m_get_single_lv, m_set_tags):
        fake_volume = api.Volume(lv_name='lv_foo', lv_path='/fake-path', vg_name='vg_foo', lv_tags='', lv_uuid='fake-uuid')
        m_get_single_lv.return_value = fake_volume
        result = lvm.prepare.Prepare([]).setup_device(device_type='data', device_name='vg_foo/lv_foo', tags={'ceph.type': 'data'}, size=0, slots=None)

        assert result == ('/fake-path', 'fake-uuid', {'ceph.type': 'data',
                                                    'ceph.vdo': '0',
                                                    'ceph.data_uuid': 'fake-uuid',
                                                    'ceph.data_device': '/fake-path'})

    @patch('ceph_volume.devices.lvm.prepare.api.create_lv')
    @patch('ceph_volume.api.lvm.Volume.set_tags')
    @patch('ceph_volume.util.disk.is_device')
    def test_setup_device_device_passed(self, m_is_device, m_set_tags, m_create_lv):
        fake_volume = api.Volume(lv_name='lv_foo', lv_path='/fake-path', vg_name='vg_foo', lv_tags='', lv_uuid='fake-uuid')
        m_is_device.return_value = True
        m_create_lv.return_value = fake_volume
        result = lvm.prepare.Prepare([]).setup_device(device_type='data', device_name='/dev/sdx', tags={'ceph.type': 'data'}, size=0, slots=None)

        assert result == ('/fake-path', 'fake-uuid', {'ceph.type': 'data',
                                                    'ceph.vdo': '0',
                                                    'ceph.data_uuid': 'fake-uuid',
                                                    'ceph.data_device': '/fake-path'})

    @patch('ceph_volume.devices.lvm.prepare.Prepare.get_ptuuid')
    @patch('ceph_volume.devices.lvm.prepare.api.get_single_lv')
    def test_setup_device_partition_passed(self, m_get_single_lv, m_get_ptuuid):
        m_get_single_lv.side_effect = ValueError()
        m_get_ptuuid.return_value = 'fake-uuid'
        result = lvm.prepare.Prepare([]).setup_device(device_type='data', device_name='/dev/sdx', tags={'ceph.type': 'data'}, size=0, slots=None)

        assert result == ('/dev/sdx', 'fake-uuid', {'ceph.type': 'data',
                                                    'ceph.vdo': '0',
                                                    'ceph.data_uuid': 'fake-uuid',
                                                    'ceph.data_device': '/dev/sdx'})

    def test_invalid_osd_id_passed(self):
        with pytest.raises(SystemExit):
            lvm.prepare.Prepare(argv=['--osd-id', 'foo']).main()


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
