import pytest
from ceph_volume.devices import lvm
from ceph_volume.api import lvm as api
from mock.mock import patch, Mock
from ceph_volume import objectstore


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
class TestPrepareDevice(object):

    def test_cannot_use_device(self, m_create_key, factory):
        args = factory(data='/dev/var/foo')
        with pytest.raises(RuntimeError) as error:
            p = lvm.prepare.Prepare([])
            p.objectstore = objectstore.lvmbluestore.LvmBlueStore(args=args)
            p.objectstore.prepare_data_device( 'data', '0')
        assert 'Cannot use device (/dev/var/foo)' in str(error.value)
        assert 'A vg/lv path or an existing device is needed' in str(error.value)

@patch('ceph_volume.util.prepare.create_key', return_value='fake-secret')
class TestGetClusterFsid(object):
    def setup(self):
        self.p = lvm.prepare.Prepare([])

    def test_fsid_is_passed_in(self, m_create_key, factory):
        args = factory(cluster_fsid='aaaa-1111')
        self.p.objectstore = objectstore.lvmbluestore.LvmBlueStore(args)
        assert self.p.objectstore.get_cluster_fsid() == 'aaaa-1111'

    def test_fsid_is_read_from_ceph_conf(self, m_create_key, factory, conf_ceph_stub):
        conf_ceph_stub('[global]\nfsid = bbbb-2222')
        args = factory(cluster_fsid='')
        self.p.objectstore = objectstore.lvmbluestore.LvmBlueStore(args)
        assert self.p.objectstore.get_cluster_fsid() == 'bbbb-2222'


@patch('ceph_volume.util.prepare.create_key', return_value='fake-secret')
class TestPrepare(object):

    def setup(self):
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

    @patch('ceph_volume.api.lvm.is_ceph_device')
    def test_safe_prepare_osd_already_created(self, m_create_key, m_is_ceph_device):
        m_is_ceph_device.return_value = True
        with pytest.raises(RuntimeError) as error:
            self.p.args = Mock()
            self.p.args.data = '/dev/sdfoo'
            self.p.get_lv = Mock()
            self.p.objectstore = objectstore.lvmbluestore.LvmBlueStore(args=self.p.args)
            self.p.objectstore.safe_prepare()
            expected = 'skipping {}, it is already prepared'.format('/dev/sdfoo')
            assert expected in str(error.value)

    def test_setup_device_device_name_is_none(self, m_create_key):
        self.p.objectstore = objectstore.lvmbluestore.LvmBlueStore(args=[])
        result = self.p.objectstore.setup_device(device_type='data',
                                            device_name=None,
                                            tags={'ceph.type': 'data'},
                                            size=0,
                                            slots=None)
        assert result == ('', '', {'ceph.type': 'data'})

    @patch('ceph_volume.api.lvm.Volume.set_tags')
    @patch('ceph_volume.api.lvm.get_single_lv')
    def test_setup_device_lv_passed(self, m_get_single_lv, m_set_tags, m_create_key):
        fake_volume = api.Volume(lv_name='lv_foo', lv_path='/fake-path', vg_name='vg_foo', lv_tags='', lv_uuid='fake-uuid')
        m_get_single_lv.return_value = fake_volume
        self.p.objectstore = objectstore.lvmbluestore.LvmBlueStore(args=[])
        result = self.p.objectstore.setup_device(device_type='data', device_name='vg_foo/lv_foo', tags={'ceph.type': 'data'}, size=0, slots=None)

        assert result == ('/fake-path', 'fake-uuid', {'ceph.type': 'data',
                                                    'ceph.vdo': '0',
                                                    'ceph.data_uuid': 'fake-uuid',
                                                    'ceph.data_device': '/fake-path'})

    @patch('ceph_volume.api.lvm.create_lv')
    @patch('ceph_volume.api.lvm.Volume.set_tags')
    @patch('ceph_volume.util.disk.is_device')
    def test_setup_device_device_passed(self, m_is_device, m_set_tags, m_create_lv, m_create_key):
        fake_volume = api.Volume(lv_name='lv_foo', lv_path='/fake-path', vg_name='vg_foo', lv_tags='', lv_uuid='fake-uuid')
        m_is_device.return_value = True
        m_create_lv.return_value = fake_volume
        self.p.objectstore = objectstore.lvmbluestore.LvmBlueStore(args=[])
        result = self.p.objectstore.setup_device(device_type='data', device_name='/dev/sdx', tags={'ceph.type': 'data'}, size=0, slots=None)

        assert result == ('/fake-path', 'fake-uuid', {'ceph.type': 'data',
                                                    'ceph.vdo': '0',
                                                    'ceph.data_uuid': 'fake-uuid',
                                                    'ceph.data_device': '/fake-path'})

    @patch('ceph_volume.objectstore.baseobjectstore.BaseObjectStore.get_ptuuid')
    @patch('ceph_volume.api.lvm.get_single_lv')
    def test_setup_device_partition_passed(self, m_get_single_lv, m_get_ptuuid, m_create_key):
        m_get_single_lv.side_effect = ValueError()
        m_get_ptuuid.return_value = 'fake-uuid'
        self.p.objectstore = objectstore.lvmbluestore.LvmBlueStore(args=[])
        result = self.p.objectstore.setup_device(device_type='data', device_name='/dev/sdx', tags={'ceph.type': 'data'}, size=0, slots=None)

        assert result == ('/dev/sdx', 'fake-uuid', {'ceph.type': 'data',
                                                    'ceph.vdo': '0',
                                                    'ceph.data_uuid': 'fake-uuid',
                                                    'ceph.data_device': '/dev/sdx'})

    def test_invalid_osd_id_passed(self, m_create_key):
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
