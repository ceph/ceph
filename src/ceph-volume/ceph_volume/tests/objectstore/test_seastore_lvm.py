import pytest
from argparse import Namespace
from unittest.mock import patch, Mock
from ceph_volume.objectstore.seastore_lvm import SeastoreLvm
from ceph_volume.api.lvm import Volume
from ceph_volume.util import system


class TestSeastoreLvm:
    @patch('ceph_volume.objectstore.lvm.prepare_utils.create_key',
           Mock(return_value=['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']))
    def setup_method(self, m_create_key):
        args = Namespace(dmcrypt_format_opts=None, dmcrypt_open_opts=None)
        self.lvm = SeastoreLvm(args)

    @patch('ceph_volume.conf.cluster', 'ceph')
    @patch('ceph_volume.api.lvm.get_single_lv')
    @patch('ceph_volume.objectstore.lvm.prepare_utils.create_id', Mock(return_value='111'))
    def test_pre_prepare_lv(self, m_get_single_lv, factory):
        args = factory(objectstore='seastore',
                       cluster_fsid='abcd',
                       osd_fsid='abc123',
                       crush_device_class='ssd',
                       osd_id='111',
                       data='vg_foo/lv_foo')
        m_get_single_lv.return_value = Volume(lv_name='lv_foo',
                                              lv_path='/fake-path',
                                              vg_name='vg_foo',
                                              lv_tags='',
                                              lv_uuid='fake-uuid')
        self.lvm.encrypted = False
        self.lvm.with_tpm = 0
        self.lvm.args = args
        self.lvm.objectstore = 'seastore'
        self.lvm.pre_prepare()
        assert self.lvm.osd_id == '111'
        assert self.lvm.block_device_path == '/fake-path'


    def test_add_objectstore_opts_no_bluestore_paths(self):
        """SeastoreLvm must not add --bluestore-block-wal-path / --bluestore-block-db-path."""
        self.lvm.osd_mkfs_cmd = ['ceph-osd', '--mkfs']
        self.lvm.wal_device_path = '/dev/sdb'
        self.lvm.db_device_path = '/dev/sdc'
        self.lvm.add_objectstore_opts()
        cmd = ' '.join(self.lvm.osd_mkfs_cmd)
        assert '--bluestore-block-wal-path' not in cmd
        assert '--bluestore-block-db-path' not in cmd

    @patch.dict('os.environ', {'CEPH_VOLUME_OSDSPEC_AFFINITY': 'my-spec'})
    def test_add_objectstore_opts_with_affinity(self):
        self.lvm.osd_mkfs_cmd = ['ceph-osd', '--mkfs']
        self.lvm.add_objectstore_opts()
        assert '--osdspec-affinity' in self.lvm.osd_mkfs_cmd
        assert 'my-spec' in self.lvm.osd_mkfs_cmd


    def test_setup_metadata_devices_no_secondaries(self, factory):
        args = factory(seastore_secondary=[])
        self.lvm.args = args
        self.lvm.tags = {}
        self.lvm.setup_metadata_devices()
        assert self.lvm.tags['ceph.seastore_secondary_count'] == 0

    def test_setup_metadata_devices_one_secondary(self, factory):
        args = factory(seastore_secondary=[('/dev/sdb', 'HDD')])
        self.lvm.args = args
        self.lvm.tags = {}
        self.lvm.setup_metadata_devices()
        assert self.lvm.tags['ceph.seastore_secondary_count'] == 1
        assert self.lvm.tags['ceph.seastore_secondary_0_device'] == '/dev/sdb'
        assert self.lvm.tags['ceph.seastore_secondary_0_type'] == 'HDD'
        assert self.lvm.tags['ceph.seastore_secondary_0_id'] == 1

    def test_setup_metadata_devices_two_secondaries(self, factory):
        args = factory(seastore_secondary=[('/dev/sdb', 'HDD'), ('/dev/sdc', 'SSD')])
        self.lvm.args = args
        self.lvm.tags = {}
        self.lvm.setup_metadata_devices()
        assert self.lvm.tags['ceph.seastore_secondary_count'] == 2
        assert self.lvm.tags['ceph.seastore_secondary_1_device'] == '/dev/sdc'
        assert self.lvm.tags['ceph.seastore_secondary_1_type'] == 'SSD'
        assert self.lvm.tags['ceph.seastore_secondary_1_id'] == 2

    def test_setup_metadata_devices_missing_attr(self):
        # args without seastore_secondary attribute at all (e.g. old code paths)
        self.lvm.args = Namespace()  # no seastore_secondary attribute
        self.lvm.tags = {}
        self.lvm.setup_metadata_devices()
        assert self.lvm.tags['ceph.seastore_secondary_count'] == 0


    @patch('ceph_volume.objectstore.seastore.prepare_utils.link_seastore_secondary')
    @patch('ceph_volume.objectstore.baseobjectstore.BaseObjectStore.prepare_osd_req')
    def test_prepare_osd_req_no_secondaries(self, m_super, m_link, factory):
        args = factory(seastore_secondary=[])
        self.lvm.args = args
        self.lvm.osd_id = '0'
        self.lvm.prepare_osd_req()
        m_super.assert_called_once()
        m_link.assert_not_called()

    @patch('ceph_volume.objectstore.seastore.prepare_utils.link_seastore_secondary')
    @patch('ceph_volume.objectstore.baseobjectstore.BaseObjectStore.prepare_osd_req')
    def test_prepare_osd_req_with_secondaries(self, m_super, m_link, factory):
        args = factory(seastore_secondary=[('/dev/sdb', 'HDD'), ('/dev/sdc', 'SSD')])
        self.lvm.args = args
        self.lvm.osd_id = '0'
        self.lvm.prepare_osd_req()
        assert m_link.call_count == 2
        m_link.assert_any_call('/dev/sdb', 'HDD', 1, '0')
        m_link.assert_any_call('/dev/sdc', 'SSD', 2, '0')


    @patch('ceph_volume.objectstore.seastore.prepare_utils.unlink_seastore_secondaries')
    def test_unlink_bs_symlinks_no_block(self, m_unlink, fake_filesystem):
        osd_path = '/var/lib/ceph/osd/ceph-0'
        fake_filesystem.create_dir(osd_path)
        self.lvm.osd_path = osd_path
        self.lvm.unlink_bs_symlinks()
        m_unlink.assert_called_once_with(osd_path)

    @patch('ceph_volume.objectstore.seastore.prepare_utils.unlink_seastore_secondaries')
    def test_unlink_bs_symlinks_removes_block(self, m_unlink, fake_filesystem):
        osd_path = '/var/lib/ceph/osd/ceph-0'
        fake_filesystem.create_dir(osd_path)
        fake_filesystem.create_file(osd_path + '/block')
        self.lvm.osd_path = osd_path
        self.lvm.unlink_bs_symlinks()
        assert not fake_filesystem.exists(osd_path + '/block')
        m_unlink.assert_called_once_with(osd_path)


    def test_get_secondaries_from_lvs_empty(self):
        lvs = [Volume(lv_name='block', lv_path='/dev/vg/lv',
                      vg_name='vg', lv_tags='ceph.type=block',
                      lv_uuid='uuid1')]
        assert self.lvm._get_secondaries_from_lvs(lvs) == []

    def test_get_secondaries_from_lvs_one(self):
        tags = ('ceph.type=block,ceph.seastore_secondary_count=1,'
                'ceph.seastore_secondary_0_device=/dev/sdb,'
                'ceph.seastore_secondary_0_type=HDD,'
                'ceph.seastore_secondary_0_id=1')
        lvs = [Volume(lv_name='block', lv_path='/dev/vg/lv',
                      vg_name='vg', lv_tags=tags, lv_uuid='uuid1')]
        result = self.lvm._get_secondaries_from_lvs(lvs)
        assert result == [('/dev/sdb', 'HDD', 1)]

    def test_get_secondaries_from_lvs_two(self):
        tags = ('ceph.type=block,ceph.seastore_secondary_count=2,'
                'ceph.seastore_secondary_0_device=/dev/sdb,'
                'ceph.seastore_secondary_0_type=HDD,'
                'ceph.seastore_secondary_0_id=1,'
                'ceph.seastore_secondary_1_device=/dev/sdc,'
                'ceph.seastore_secondary_1_type=SSD,'
                'ceph.seastore_secondary_1_id=2')
        lvs = [Volume(lv_name='block', lv_path='/dev/vg/lv',
                      vg_name='vg', lv_tags=tags, lv_uuid='uuid1')]
        result = self.lvm._get_secondaries_from_lvs(lvs)
        assert result == [('/dev/sdb', 'HDD', 1), ('/dev/sdc', 'SSD', 2)]

    def test_get_secondaries_from_lvs_no_block_lv(self):
        lvs = [Volume(lv_name='other', lv_path='/dev/vg/lv',
                      vg_name='vg', lv_tags='ceph.type=db', lv_uuid='uuid1')]
        assert self.lvm._get_secondaries_from_lvs(lvs) == []


    @patch('ceph_volume.objectstore.seastore_lvm.os.makedirs')
    @patch('ceph_volume.objectstore.seastore_lvm.SeastoreLvm.unlink_bs_symlinks')
    @patch('ceph_volume.objectstore.lvm.prepare_utils.create_osd_path')
    @patch('ceph_volume.terminal.success')
    def test__activate_no_secondaries(self,
                                      m_success, m_create_osd_path,
                                      m_unlink, m_makedirs,
                                      monkeypatch, fake_run, fake_call,
                                      conf_ceph_stub, patch_udevdata):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        monkeypatch.setattr('ceph_volume.configuration.load', lambda: None)
        monkeypatch.setattr('ceph_volume.util.system.path_is_mounted', lambda path: False)

        lvs = [Volume(lv_name='lv_foo-block',
                      lv_path='/fake-block-path',
                      vg_name='vg_foo',
                      lv_tags=('ceph.type=block,ceph.osd_id=0,ceph.osd_fsid=abcd,'
                               'ceph.cluster_name=ceph,ceph.encrypted=0,'
                               'ceph.seastore_secondary_count=0'),
                      lv_uuid='fake-block-uuid')]
        self.lvm._activate(lvs)

        # prime-osd-dir must NOT be called for seastore
        run_cmds = [c['args'][0][0] for c in fake_run.calls]
        assert 'ceph-bluestore-tool' not in run_cmds

        # block symlink must be created
        block_ln_calls = [c['args'][0] for c in fake_run.calls
                          if c['args'][0][0] == 'ln'
                          and c['args'][0][2] == '/fake-block-path']
        assert block_ln_calls

        assert m_success.called

    @patch('ceph_volume.objectstore.seastore_lvm.os.makedirs')
    @patch('ceph_volume.objectstore.seastore_lvm.SeastoreLvm.unlink_bs_symlinks')
    @patch('ceph_volume.objectstore.lvm.prepare_utils.create_osd_path')
    @patch('ceph_volume.terminal.success')
    def test__activate_with_secondaries(self,
                                        m_success, m_create_osd_path,
                                        m_unlink, m_makedirs,
                                        monkeypatch, fake_run, fake_call,
                                        conf_ceph_stub, patch_udevdata):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        monkeypatch.setattr('ceph_volume.configuration.load', lambda: None)
        monkeypatch.setattr('ceph_volume.util.system.path_is_mounted', lambda path: False)

        lvs = [Volume(lv_name='lv_foo-block',
                      lv_path='/fake-block-path',
                      vg_name='vg_foo',
                      lv_tags=('ceph.type=block,ceph.osd_id=0,ceph.osd_fsid=abcd,'
                               'ceph.cluster_name=ceph,ceph.encrypted=0,'
                               'ceph.seastore_secondary_count=1,'
                               'ceph.seastore_secondary_0_device=/dev/sdb,'
                               'ceph.seastore_secondary_0_type=HDD,'
                               'ceph.seastore_secondary_0_id=1'),
                      lv_uuid='fake-block-uuid')]
        self.lvm._activate(lvs)

        # secondary directory block.HDD.1 must be created via makedirs
        m_makedirs.assert_called_once_with(
            '/var/lib/ceph/osd/ceph-0/block.HDD.1', exist_ok=True)

        # secondary block symlink must be created
        secondary_ln_calls = [c['args'][0] for c in fake_run.calls
                               if c['args'][0][0] == 'ln'
                               and 'block.HDD.1' in c['args'][0][-1]]
        assert secondary_ln_calls
        assert secondary_ln_calls[0][2] == '/dev/sdb'

    @patch('ceph_volume.objectstore.seastore_lvm.os.makedirs')
    @patch('ceph_volume.objectstore.seastore_lvm.SeastoreLvm.unlink_bs_symlinks')
    @patch('ceph_volume.objectstore.lvm.prepare_utils.create_osd_path')
    @patch('ceph_volume.terminal.success')
    def test__activate_with_two_secondaries(self,
                                            m_success, m_create_osd_path,
                                            m_unlink, m_makedirs,
                                            monkeypatch, fake_run, fake_call,
                                            conf_ceph_stub, patch_udevdata):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        monkeypatch.setattr('ceph_volume.configuration.load', lambda: None)
        monkeypatch.setattr('ceph_volume.util.system.path_is_mounted', lambda path: False)

        lvs = [Volume(lv_name='lv_foo-block',
                      lv_path='/fake-block-path',
                      vg_name='vg_foo',
                      lv_tags=('ceph.type=block,ceph.osd_id=0,ceph.osd_fsid=abcd,'
                               'ceph.cluster_name=ceph,ceph.encrypted=0,'
                               'ceph.seastore_secondary_count=2,'
                               'ceph.seastore_secondary_0_device=/dev/sdb,'
                               'ceph.seastore_secondary_0_type=HDD,'
                               'ceph.seastore_secondary_0_id=1,'
                               'ceph.seastore_secondary_1_device=/dev/sdc,'
                               'ceph.seastore_secondary_1_type=SSD,'
                               'ceph.seastore_secondary_1_id=2'),
                      lv_uuid='fake-block-uuid')]
        self.lvm._activate(lvs)

        # both secondary directories must be created
        assert m_makedirs.call_count == 2
        m_makedirs.assert_any_call(
            '/var/lib/ceph/osd/ceph-0/block.HDD.1', exist_ok=True)
        m_makedirs.assert_any_call(
            '/var/lib/ceph/osd/ceph-0/block.SSD.2', exist_ok=True)

        # both secondary block symlinks must be created with correct targets
        ln_calls = {c['args'][0][-1]: c['args'][0][2]
                    for c in fake_run.calls
                    if c['args'][0][0] == 'ln' and 'block.' in c['args'][0][-1]}
        assert ln_calls['/var/lib/ceph/osd/ceph-0/block.HDD.1/block'] == '/dev/sdb'
        assert ln_calls['/var/lib/ceph/osd/ceph-0/block.SSD.2/block'] == '/dev/sdc'

    def test__activate_raises_exception_no_block_lv(self):
        lvs = [Volume(lv_name='lv_foo-db',
                      lv_path='/fake-path',
                      vg_name='vg_foo',
                      lv_tags='ceph.type=db',
                      lv_uuid='fake-db-uuid')]
        with pytest.raises(RuntimeError) as error:
            self.lvm._activate(lvs)
        assert 'could not find a seastore OSD to activate' in str(error.value)
