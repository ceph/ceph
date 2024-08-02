import pytest
from mock import patch, Mock, MagicMock, call
from ceph_volume.objectstore.lvmbluestore import LvmBlueStore
from ceph_volume.api.lvm import Volume
from ceph_volume.util import system


class TestLvmBlueStore:
    @patch('ceph_volume.objectstore.lvmbluestore.prepare_utils.create_key', Mock(return_value=['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']))
    def setup_method(self, m_create_key):
        self.lvm_bs = LvmBlueStore([])

    @patch('ceph_volume.conf.cluster', 'ceph')
    @patch('ceph_volume.api.lvm.get_single_lv')
    @patch('ceph_volume.objectstore.lvmbluestore.prepare_utils.create_id', Mock(return_value='111'))
    @patch('ceph_volume.objectstore.lvmbluestore.encryption_utils.create_dmcrypt_key', Mock(return_value='fake-dmcrypt-key'))
    def test_pre_prepare_lv(self, m_get_single_lv, factory):
        args = factory(cluster_fsid='abcd',
                       osd_fsid='abc123',
                       crush_device_class='ssd',
                       osd_id='111',
                       data='vg_foo/lv_foo')
        m_get_single_lv.return_value = Volume(lv_name='lv_foo',
                                              lv_path='/fake-path',
                                              vg_name='vg_foo',
                                              lv_tags='',
                                              lv_uuid='fake-uuid')
        self.lvm_bs.encrypted = True
        self.lvm_bs.args = args
        self.lvm_bs.pre_prepare()
        assert self.lvm_bs.secrets['dmcrypt_key'] == 'fake-dmcrypt-key'
        assert self.lvm_bs.secrets['crush_device_class'] == 'ssd'
        assert self.lvm_bs.osd_id == '111'
        assert self.lvm_bs.block_device_path == '/fake-path'
        assert self.lvm_bs.tags == {'ceph.osd_fsid': 'abc123',
                                    'ceph.osd_id': '111',
                                    'ceph.cluster_fsid': 'abcd',
                                    'ceph.cluster_name': 'ceph',
                                    'ceph.crush_device_class': 'ssd',
                                    'ceph.osdspec_affinity': '',
                                    'ceph.block_device': '/fake-path',
                                    'ceph.block_uuid': 'fake-uuid',
                                    'ceph.cephx_lockbox_secret': '',
                                    'ceph.encrypted': True,
                                    'ceph.vdo': '0'}

    @patch('ceph_volume.objectstore.lvmbluestore.prepare_utils.create_id', Mock(return_value='111'))
    @patch('ceph_volume.objectstore.lvmbluestore.encryption_utils.create_dmcrypt_key', Mock(return_value='fake-dmcrypt-key'))
    def test_pre_prepare_no_lv(self, factory):
        args = factory(cluster_fsid='abcd',
                       osd_fsid='abc123',
                       crush_device_class='ssd',
                       osd_id='111',
                       data='/dev/foo')
        self.lvm_bs.prepare_data_device = lambda x, y: Volume(lv_name='lv_foo',
                                                              lv_path='/fake-path',
                                                              vg_name='vg_foo',
                                                              lv_tags='',
                                                              lv_uuid='fake-uuid')
        self.lvm_bs.encrypted = True
        self.lvm_bs.args = args
        self.lvm_bs.pre_prepare()
        assert self.lvm_bs.secrets['dmcrypt_key'] == 'fake-dmcrypt-key'
        assert self.lvm_bs.secrets['crush_device_class'] == 'ssd'
        assert self.lvm_bs.osd_id == '111'
        assert self.lvm_bs.block_device_path == '/fake-path'
        assert self.lvm_bs.tags == {'ceph.osd_fsid': 'abc123',
                                    'ceph.osd_id': '111',
                                    'ceph.cluster_fsid': 'abcd',
                                    'ceph.cluster_name': None,
                                    'ceph.crush_device_class': 'ssd',
                                    'ceph.osdspec_affinity': '',
                                    'ceph.block_device': '/fake-path',
                                    'ceph.block_uuid': 'fake-uuid',
                                    'ceph.cephx_lockbox_secret': '',
                                    'ceph.encrypted': True,
                                    'ceph.vdo': '0'}

    @patch('ceph_volume.util.disk.is_partition', Mock(return_value=True))
    @patch('ceph_volume.api.lvm.create_lv')
    def test_prepare_data_device(self, m_create_lv, factory):
        args = factory(data='/dev/foo',
                       data_slots=1,
                       data_size=102400)
        self.lvm_bs.args = args
        m_create_lv.return_value = Volume(lv_name='lv_foo',
                                          lv_path='/fake-path',
                                          vg_name='vg_foo',
                                          lv_tags='',
                                          lv_uuid='abcd')
        assert self.lvm_bs.prepare_data_device('block', 'abcd') == m_create_lv.return_value
        assert self.lvm_bs.args.data_size == 102400

    @patch('ceph_volume.util.disk.is_device', Mock(return_value=False))
    @patch('ceph_volume.util.disk.is_partition', Mock(return_value=False))
    def test_prepare_data_device_fails(self, factory):
        args = factory(data='/dev/foo')
        self.lvm_bs.args = args
        with pytest.raises(RuntimeError) as error:
            self.lvm_bs.prepare_data_device('block', 'abcd')
        assert ('Cannot use device (/dev/foo). '
        'A vg/lv path or an existing device is needed') == str(error.value)

    @patch('ceph_volume.api.lvm.is_ceph_device', Mock(return_value=True))
    @patch('ceph_volume.api.lvm.get_single_lv')
    def test_safe_prepare_is_ceph_device(self, m_get_single_lv, factory):
        args = factory(data='/dev/foo')
        self.lvm_bs.args = args
        m_get_single_lv.return_value = Volume(lv_name='lv_foo',
                                              lv_path='/fake-path',
                                              vg_name='vg_foo',
                                              lv_tags='',
                                              lv_uuid='fake-uuid')
        self.lvm_bs.prepare = MagicMock()
        with pytest.raises(RuntimeError) as error:
            self.lvm_bs.safe_prepare(args)
        assert str(error.value) == 'skipping /dev/foo, it is already prepared'

    @patch('ceph_volume.api.lvm.is_ceph_device', Mock(return_value=False))
    @patch('ceph_volume.api.lvm.get_single_lv')
    def test_safe_prepare(self, m_get_single_lv, factory):
        args = factory(data='vg_foo/lv_foo')
        self.lvm_bs.args = args
        m_get_single_lv.return_value = Volume(lv_name='lv_foo',
                                              lv_path='/fake-path',
                                              vg_name='vg_foo',
                                              lv_tags='',
                                              lv_uuid='fake-uuid')
        self.lvm_bs.prepare = MagicMock()
        self.lvm_bs.safe_prepare()
        assert self.lvm_bs.prepare.called

    @patch('ceph_volume.objectstore.lvmbluestore.LvmBlueStore.prepare', Mock(side_effect=Exception))
    @patch('ceph_volume.api.lvm.is_ceph_device', Mock(return_value=False))
    # @patch('ceph_volume.devices.lvm.common.rollback_osd')
    @patch('ceph_volume.objectstore.lvmbluestore.rollback_osd')
    @patch('ceph_volume.api.lvm.get_single_lv')
    def test_safe_prepare_raises_exception(self, m_get_single_lv, m_rollback_osd, factory):
        args = factory(data='/dev/foo')
        self.lvm_bs.args = args
        self.lvm_bs.osd_id = '111'
        m_get_single_lv.return_value = Volume(lv_name='lv_foo',
                                              lv_path='/fake-path',
                                              vg_name='vg_foo',
                                              lv_tags='',
                                              lv_uuid='fake-uuid')
        m_rollback_osd.return_value = MagicMock()
        with pytest.raises(Exception):
            self.lvm_bs.safe_prepare()
        assert m_rollback_osd.mock_calls == [call(self.lvm_bs.args, '111')]

    @patch('ceph_volume.objectstore.baseobjectstore.BaseObjectStore.get_ptuuid', Mock(return_value='c6798f59-01'))
    @patch('ceph_volume.api.lvm.Volume.set_tags', MagicMock())
    @patch('ceph_volume.api.lvm.get_single_lv')
    def test_prepare(self, m_get_single_lv, is_root, factory):
        m_get_single_lv.return_value = Volume(lv_name='lv_foo',
                                              lv_path='/fake-path',
                                              vg_name='vg_foo',
                                              lv_tags='',
                                              lv_uuid='fake-uuid')
        args = factory(data='vg_foo/lv_foo',
                       block_wal='/dev/foo1',
                       block_db='/dev/foo2',
                       block_wal_size=123,
                       block_db_size=123,
                       block_wal_slots=1,
                       block_db_slots=1,
                       )
        self.lvm_bs.args = args
        self.lvm_bs.pre_prepare = lambda: None
        self.lvm_bs.block_lv = MagicMock()
        self.lvm_bs.prepare_osd_req = MagicMock()
        self.lvm_bs.osd_mkfs = MagicMock()
        self.lvm_bs.prepare_dmcrypt = MagicMock()
        self.lvm_bs.secrets['dmcrypt_key'] = 'fake-secret'
        self.lvm_bs.prepare()
        assert self.lvm_bs.wal_device_path == '/dev/foo1'
        assert self.lvm_bs.db_device_path == '/dev/foo2'
        assert self.lvm_bs.block_lv.set_tags.mock_calls == [call({'ceph.type': 'block', 'ceph.vdo': '0', 'ceph.wal_uuid': 'c6798f59-01', 'ceph.wal_device': '/dev/foo1', 'ceph.db_uuid': 'c6798f59-01', 'ceph.db_device': '/dev/foo2'})]
        assert self.lvm_bs.prepare_dmcrypt.called
        assert self.lvm_bs.osd_mkfs.called
        assert self.lvm_bs.prepare_osd_req.called

    def test_prepare_dmcrypt(self):
        self.lvm_bs.secrets = {'dmcrypt_key': 'fake-secret'}
        self.lvm_bs.tags = {'ceph.block_uuid': 'block-uuid1',
                            'ceph.db_uuid': 'db-uuid2',
                            'ceph.wal_uuid': 'wal-uuid3'}
        self.lvm_bs.luks_format_and_open = lambda *a: f'/dev/mapper/{a[3]["ceph."+a[2]+"_uuid"]}'
        self.lvm_bs.prepare_dmcrypt()
        assert self.lvm_bs.block_device_path == '/dev/mapper/block-uuid1'
        assert self.lvm_bs.db_device_path == '/dev/mapper/db-uuid2'
        assert self.lvm_bs.wal_device_path == '/dev/mapper/wal-uuid3'

    @patch('ceph_volume.objectstore.lvmbluestore.encryption_utils.luks_open')
    @patch('ceph_volume.objectstore.lvmbluestore.encryption_utils.luks_format')
    def test_luks_format_and_open(self, m_luks_format, m_luks_open):
        result = self.lvm_bs.luks_format_and_open('key',
                                                  '/dev/foo',
                                                  'block',
                                                  {'ceph.block_uuid': 'block-uuid1'})
        assert result == '/dev/mapper/block-uuid1'

    def test_luks_format_and_open_not_device(self):
        result = self.lvm_bs.luks_format_and_open('key',
                                                  '',
                                                  'block',
                                                  {})
        assert result == ''

    def test_setup_device_is_none(self):
        result = self.lvm_bs.setup_device('block',
                                          None,
                                          {},
                                          1,
                                          1)
        assert result == ('', '', {})

    @patch('ceph_volume.api.lvm.Volume.set_tags', return_value=MagicMock())
    @patch('ceph_volume.util.system.generate_uuid',
           Mock(return_value='d83fa1ca-bd68-4c75-bdc2-464da58e8abd'))
    @patch('ceph_volume.api.lvm.create_lv')
    @patch('ceph_volume.util.disk.is_device', Mock(return_value=True))
    def test_setup_device_is_device(self, m_create_lv, m_set_tags):
        m_create_lv.return_value = Volume(lv_name='lv_foo',
                                          lv_path='/fake-path',
                                          vg_name='vg_foo',
                                          lv_tags='',
                                          lv_uuid='fake-uuid')
        result = self.lvm_bs.setup_device('block',
                                          '/dev/foo',
                                          {},
                                          1,
                                          1)
        assert m_create_lv.mock_calls == [call('osd-block',
                                               'd83fa1ca-bd68-4c75-bdc2-464da58e8abd',
                                               device='/dev/foo',
                                               tags={'ceph.type': 'block',
                                                     'ceph.vdo': '0',
                                                     'ceph.block_device': '/fake-path',
                                                     'ceph.block_uuid': 'fake-uuid'},
                                               slots=1,
                                               size=1)]
        assert result == ('/fake-path',
                         'fake-uuid',
                         {'ceph.type': 'block',
                          'ceph.vdo': '0',
                          'ceph.block_device': '/fake-path',
                          'ceph.block_uuid': 'fake-uuid'
                          })

    @patch('ceph_volume.api.lvm.get_single_lv')
    @patch('ceph_volume.api.lvm.Volume.set_tags', return_value=MagicMock())
    def test_setup_device_is_lv(self, m_set_tags, m_get_single_lv):
        m_get_single_lv.return_value = Volume(lv_name='lv_foo',
                                              lv_path='/fake-path',
                                              vg_name='vg_foo',
                                              lv_tags='',
                                              lv_uuid='fake-uuid')
        result = self.lvm_bs.setup_device('block',
                                          'vg_foo/lv_foo',
                                          {},
                                          1,
                                          1)
        assert result == ('/fake-path',
                         'fake-uuid',
                         {'ceph.type': 'block',
                          'ceph.vdo': '0',
                          'ceph.block_device': '/fake-path',
                          'ceph.block_uuid': 'fake-uuid'
                          })

    @patch('ceph_volume.api.lvm.Volume.set_tags', return_value=MagicMock())
    def test_setup_device_partition(self, m_set_tags):
        self.lvm_bs.get_ptuuid = lambda x: 'c6798f59-01'
        result = self.lvm_bs.setup_device('block',
                                          '/dev/foo1',
                                          {},
                                          1,
                                          1)
        assert result == ('/dev/foo1',
                         'c6798f59-01',
                         {'ceph.type': 'block',
                          'ceph.vdo': '0',
                          'ceph.block_uuid': 'c6798f59-01',
                          'ceph.block_device': '/dev/foo1'})

    def test_get_osd_device_path_lv_block(self):
        lvs = [Volume(lv_name='lv_foo',
                      lv_path='/fake-path',
                      vg_name='vg_foo',
                      lv_tags='ceph.type=block,ceph.block_uuid=fake-block-uuid',
                      lv_uuid='fake-block-uuid')]
        assert self.lvm_bs.get_osd_device_path(lvs, 'block') == '/fake-path'

    @patch('ceph_volume.objectstore.lvmbluestore.encryption_utils.luks_open', MagicMock())
    def test_get_osd_device_path_lv_block_encrypted(self):
        lvs = [Volume(lv_name='lv_foo',
                      lv_path='/fake-path',
                      vg_name='vg_foo',
                      lv_tags='ceph.type=block,ceph.block_uuid=fake-block-uuid,ceph.encrypted=1',
                      lv_uuid='fake-block-uuid')]
        assert self.lvm_bs.get_osd_device_path(lvs, 'block') == '/dev/mapper/fake-block-uuid'

    def test_get_osd_device_path_lv_db(self):
        lvs = [Volume(lv_name='lv_foo-block',
                      lv_path='/fake-block-path',
                      vg_name='vg_foo',
                      lv_tags='ceph.type=block,ceph.block_uuid=fake-block-uuid,ceph.db_uuid=fake-db-uuid',
                      lv_uuid='fake-block-uuid'),
               Volume(lv_name='lv_foo-db',
                      lv_path='/fake-db-path',
                      vg_name='vg_foo_db',
                      lv_tags='ceph.type=db,ceph.block_uuid=fake-block-uuid,ceph.db_uuid=fake-db-uuid',
                      lv_uuid='fake-db-uuid')]
        assert self.lvm_bs.get_osd_device_path(lvs, 'db') == '/fake-db-path'

    def test_get_osd_device_path_no_device_uuid(self):
        lvs = [Volume(lv_name='lv_foo-block',
                      lv_path='/fake-block-path',
                      vg_name='vg_foo',
                      lv_tags='ceph.type=block,ceph.block_uuid=fake-block-uuid',
                      lv_uuid='fake-block-uuid'),
               Volume(lv_name='lv_foo-db',
                      lv_path='/fake-db-path',
                      vg_name='vg_foo_db',
                      lv_tags='ceph.type=db,ceph.block_uuid=fake-block-uuid',
                      lv_uuid='fake-db-uuid')]
        assert not self.lvm_bs.get_osd_device_path(lvs, 'db')

    @patch('ceph_volume.util.disk.get_device_from_partuuid')
    @patch('ceph_volume.objectstore.lvmbluestore.encryption_utils.luks_open', MagicMock())
    def test_get_osd_device_path_phys_encrypted(self, m_get_device_from_partuuid):
        m_get_device_from_partuuid.return_value = '/dev/sda1'
        lvs = [Volume(lv_name='lv_foo-block',
                     lv_path='/fake-block-path',
                     vg_name='vg_foo',
                     lv_tags='ceph.type=block,ceph.block_uuid=fake-block-uuid,ceph.db_uuid=fake-db-uuid,ceph.osd_id=0,ceph.osd_fsid=abcd,ceph.cluster_name=ceph,ceph.encrypted=1',
                     lv_uuid='fake-block-uuid')]
        assert self.lvm_bs.get_osd_device_path(lvs, 'db') == '/dev/mapper/fake-db-uuid'

    @patch('ceph_volume.util.disk.get_device_from_partuuid')
    def test_get_osd_device_path_phys(self, m_get_device_from_partuuid):
        m_get_device_from_partuuid.return_value = '/dev/sda1'
        lvs = [Volume(lv_name='lv_foo-block',
                     lv_path='/fake-block-path',
                     vg_name='vg_foo',
                     lv_tags='ceph.type=block,ceph.block_uuid=fake-block-uuid,ceph.db_uuid=fake-db-uuid,ceph.osd_id=0,ceph.osd_fsid=abcd,ceph.cluster_name=ceph',
                     lv_uuid='fake-block-uuid')]
        self.lvm_bs.get_osd_device_path(lvs, 'db')

    @patch('ceph_volume.util.disk.get_device_from_partuuid')
    def test_get_osd_device_path_phys_raises_exception(self, m_get_device_from_partuuid):
        m_get_device_from_partuuid.return_value = ''
        lvs = [Volume(lv_name='lv_foo-block',
                     lv_path='/fake-block-path',
                     vg_name='vg_foo',
                     lv_tags='ceph.type=block,ceph.block_uuid=fake-block-uuid,ceph.db_uuid=fake-db-uuid,ceph.osd_id=0,ceph.osd_fsid=abcd,ceph.cluster_name=ceph',
                     lv_uuid='fake-block-uuid')]
        with pytest.raises(RuntimeError):
            self.lvm_bs.get_osd_device_path(lvs, 'db')

    def test__activate_raises_exception(self):
        lvs = [Volume(lv_name='lv_foo-db',
                      lv_path='/fake-path',
                      vg_name='vg_foo',
                      lv_tags='ceph.type=db,ceph.db_uuid=fake-db-uuid',
                      lv_uuid='fake-db-uuid')]
        with pytest.raises(RuntimeError) as error:
            self.lvm_bs._activate(lvs)
        assert str(error.value) == 'could not find a bluestore OSD to activate'

    @patch('ceph_volume.objectstore.lvmbluestore.encryption_utils.write_lockbox_keyring', MagicMock())
    @patch('ceph_volume.objectstore.lvmbluestore.encryption_utils.get_dmcrypt_key', MagicMock())
    @patch('ceph_volume.objectstore.lvmbluestore.prepare_utils.create_osd_path')
    @patch('ceph_volume.terminal.success')
    @pytest.mark.parametrize("encrypted", ["ceph.encrypted=0", "ceph.encrypted=1"])
    def test__activate(self,
                       m_success, m_create_osd_path,
                       monkeypatch, fake_run, fake_call, encrypted, conf_ceph_stub):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        monkeypatch.setattr('ceph_volume.configuration.load', lambda: None)
        monkeypatch.setattr('ceph_volume.util.system.path_is_mounted', lambda path: False)
        m_create_osd_path.return_value = MagicMock()
        m_success.return_value = MagicMock()
        lvs = [Volume(lv_name='lv_foo-block',
                      lv_path='/fake-block-path',
                      vg_name='vg_foo',
                      lv_tags=f'ceph.type=block,ceph.db_uuid=fake-db-uuid,ceph.block_uuid=fake-block-uuid,ceph.wal_uuid=fake-wal-uuid,ceph.osd_id=0,ceph.osd_fsid=abcd,ceph.cluster_name=ceph,{encrypted},ceph.cephx_lockbox_secret=abcd',
                      lv_uuid='fake-block-uuid'),
               Volume(lv_name='lv_foo-db',
                      lv_path='/fake-db-path',
                      vg_name='vg_foo_db',
                      lv_tags=f'ceph.type=db,ceph.db_uuid=fake-db-uuid,ceph.block_uuid=fake-block-uuid,ceph.wal_uuid=fake-wal-uuid,ceph.osd_id=0,ceph.osd_fsid=abcd,ceph.cluster_name=ceph,{encrypted},ceph.cephx_lockbox_secret=abcd',
                      lv_uuid='fake-db-uuid'),
               Volume(lv_name='lv_foo-db',
                      lv_path='/fake-db-path',
                      vg_name='vg_foo_db',
                      lv_tags=f'ceph.type=wal,ceph.block_uuid=fake-block-uuid,ceph.wal_uuid=fake-wal-uuid,ceph.db_uuid=fake-db-uuid,ceph.osd_id=0,ceph.osd_fsid=abcd,ceph.cluster_name=ceph,{encrypted},ceph.cephx_lockbox_secret=abcd',
                      lv_uuid='fake-wal-uuid')]
        self.lvm_bs._activate(lvs)
        if encrypted == "ceph.encrypted=0":
            assert fake_run.calls == [{'args': (['ceph-bluestore-tool', '--cluster=ceph',
                                                 'prime-osd-dir', '--dev', '/fake-block-path',
                                                 '--path', '/var/lib/ceph/osd/ceph-0', '--no-mon-config'],),
                                       'kwargs': {}},
                                      {'args': (['ln', '-snf', '/fake-block-path',
                                                 '/var/lib/ceph/osd/ceph-0/block'],),
                                       'kwargs': {}},
                                      {'args': (['ln', '-snf', '/fake-db-path',
                                                 '/var/lib/ceph/osd/ceph-0/block.db'],),
                                       'kwargs': {}},
                                      {'args': (['ln', '-snf', '/fake-db-path',
                                                 '/var/lib/ceph/osd/ceph-0/block.wal'],),
                                       'kwargs': {}},
                                      {'args': (['systemctl', 'enable',
                                                 'ceph-volume@lvm-0-abcd'],),
                                       'kwargs': {}},
                                      {'args': (['systemctl', 'enable', '--runtime', 'ceph-osd@0'],),
                                       'kwargs': {}},
                                      {'args': (['systemctl', 'start', 'ceph-osd@0'],),
                                       'kwargs': {}}]
        else:
            assert fake_run.calls == [{'args': (['ceph-bluestore-tool', '--cluster=ceph',
                                                'prime-osd-dir', '--dev', '/dev/mapper/fake-block-uuid',
                                                '--path', '/var/lib/ceph/osd/ceph-0', '--no-mon-config'],),
                                      'kwargs': {}},
                                      {'args': (['ln', '-snf', '/dev/mapper/fake-block-uuid',
                                                  '/var/lib/ceph/osd/ceph-0/block'],),
                                      'kwargs': {}},
                                      {'args': (['ln', '-snf', '/dev/mapper/fake-db-uuid',
                                                  '/var/lib/ceph/osd/ceph-0/block.db'],),
                                      'kwargs': {}},
                                      {'args': (['ln', '-snf', '/dev/mapper/fake-wal-uuid',
                                                  '/var/lib/ceph/osd/ceph-0/block.wal'],),
                                      'kwargs': {}},
                                      {'args': (['systemctl', 'enable', 'ceph-volume@lvm-0-abcd'],),
                                      'kwargs': {}},
                                      {'args': (['systemctl', 'enable', '--runtime', 'ceph-osd@0'],),
                                      'kwargs': {}},
                                      {'args': (['systemctl', 'start', 'ceph-osd@0'],),
                                      'kwargs': {}}]
        assert m_success.mock_calls == [call('ceph-volume lvm activate successful for osd ID: 0')]

    @patch('ceph_volume.systemd.systemctl.osd_is_active', return_value=False)
    def test_activate_all(self,
                          m_create_key,
                          mock_lvm_direct_report,
                          is_root,
                          factory,
                          fake_run):
        args = factory(no_systemd=True)
        self.lvm_bs.args = args
        self.lvm_bs.activate = MagicMock()
        self.lvm_bs.activate_all()
        assert self.lvm_bs.activate.mock_calls == [call(args,
                                                        osd_id='1',
                                                        osd_fsid='824f7edf-371f-4b75-9231-4ab62a32d5c0'),
                                                   call(args,
                                                        osd_id='0',
                                                        osd_fsid='a0e07c5b-bee1-4ea2-ae07-cb89deda9b27')]

    @patch('ceph_volume.systemd.systemctl.osd_is_active', return_value=False)
    def test_activate_all_no_osd_found(self,
                                       m_create_key,
                                       is_root,
                                       factory,
                                       fake_run,
                                       monkeypatch,
                                       capsys):
        monkeypatch.setattr('ceph_volume.objectstore.lvmbluestore.direct_report', lambda: {})
        args = factory(no_systemd=True)
        self.lvm_bs.args = args
        self.lvm_bs.activate_all()
        stdout, stderr = capsys.readouterr()
        assert "Was unable to find any OSDs to activate" in stderr
        assert "Verify OSDs are present with" in stderr

    @patch('ceph_volume.api.lvm.process.call', Mock(return_value=('', '', 0)))
    @patch('ceph_volume.systemd.systemctl.osd_is_active', return_value=True)
    def test_activate_all_osd_is_active(self,
                                        mock_lvm_direct_report,
                                        is_root,
                                        factory,
                                        fake_run):
        args = factory(no_systemd=False)
        self.lvm_bs.args = args
        self.lvm_bs.activate = MagicMock()
        self.lvm_bs.activate_all()
        assert self.lvm_bs.activate.mock_calls == []

    @patch('ceph_volume.api.lvm.get_lvs')
    def test_activate_osd_id_and_fsid(self,
                                      m_get_lvs,
                                      is_root,
                                      factory):
        args = factory(osd_id='1',
                       osd_fsid='824f7edf',
                       no_systemd=True)
        lvs = [Volume(lv_name='lv_foo',
                      lv_path='/fake-path',
                      vg_name='vg_foo',
                      lv_tags=f'ceph.osd_id={args.osd_id},ceph.osd_fsid={args.osd_fsid}',
                      lv_uuid='fake-uuid')]
        m_get_lvs.return_value = lvs
        self.lvm_bs.args = args
        self.lvm_bs._activate = MagicMock()
        self.lvm_bs.activate()
        assert self.lvm_bs._activate.mock_calls == [call(lvs, True, False)]
        assert m_get_lvs.mock_calls == [call(tags={'ceph.osd_id': '1',
                                                   'ceph.osd_fsid': '824f7edf'})]

    @patch('ceph_volume.api.lvm.get_lvs')
    def test_activate_not_osd_id_and_fsid(self,
                                          m_get_lvs,
                                          is_root,
                                          factory):
        args = factory(no_systemd=True,
                       osd_id=None,
                       osd_fsid='824f7edf')
        lvs = [Volume(lv_name='lv_foo',
                      lv_path='/fake-path',
                      vg_name='vg_foo',
                      lv_tags='',
                      lv_uuid='fake-uuid')]
        m_get_lvs.return_value = lvs
        self.lvm_bs.args = args
        self.lvm_bs._activate = MagicMock()
        self.lvm_bs.activate()
        assert self.lvm_bs._activate.mock_calls == [call(lvs, True, False)]
        assert m_get_lvs.mock_calls == [call(tags={'ceph.osd_fsid': '824f7edf'})]

    def test_activate_osd_id_and_not_fsid(self,
                                          is_root,
                                          factory):
        args = factory(no_systemd=True,
                       osd_id='1',
                       osd_fsid=None)
        self.lvm_bs.args = args
        self.lvm_bs._activate = MagicMock()
        with pytest.raises(RuntimeError) as error:
            self.lvm_bs.activate()
        assert str(error.value) == 'could not activate osd.1, please provide the osd_fsid too'

    def test_activate_not_osd_id_and_not_fsid(self,
                                              is_root,
                                              factory):
        args = factory(no_systemd=True,
                       osd_id=None,
                       osd_fsid=None)
        self.lvm_bs.args = args
        self.lvm_bs._activate = MagicMock()
        with pytest.raises(RuntimeError) as error:
            self.lvm_bs.activate()
        assert str(error.value) == 'Please provide both osd_id and osd_fsid'

    @patch('ceph_volume.api.lvm.get_lvs')
    def test_activate_couldnt_find_osd(self,
                                       m_get_lvs,
                                       is_root,
                                       factory):
        args = factory(osd_id='1',
                       osd_fsid='824f7edf',
                       no_systemd=True)
        lvs = []
        m_get_lvs.return_value = lvs
        self.lvm_bs.args = args
        self.lvm_bs._activate = MagicMock()
        with pytest.raises(RuntimeError) as error:
            self.lvm_bs.activate()
        assert str(error.value) == 'could not find osd.1 with osd_fsid 824f7edf'