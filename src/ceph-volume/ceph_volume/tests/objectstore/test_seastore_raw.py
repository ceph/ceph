import pytest
from argparse import Namespace
from unittest.mock import patch, Mock
from ceph_volume.objectstore.seastore_raw import SeastoreRaw
from ceph_volume.util import system


class TestSeastoreRaw:
    @patch('ceph_volume.objectstore.raw.prepare_utils.create_key',
           Mock(return_value=['AQCee6ZkzhOrJRAAZWSvNC3KdXOpC2w8ly4AZQ==']))
    def setup_method(self, m_create_key):
        args = Namespace(no_tmpfs=False, seastore_secondary=[])
        self.raw = SeastoreRaw(args)


    def test_add_objectstore_opts_no_bluestore_paths(self):
        self.raw.osd_mkfs_cmd = ['ceph-osd', '--mkfs']
        self.raw.wal_device_path = '/dev/sdb'
        self.raw.db_device_path = '/dev/sdc'
        self.raw.add_objectstore_opts()
        cmd = ' '.join(self.raw.osd_mkfs_cmd)
        assert '--bluestore-block-wal-path' not in cmd
        assert '--bluestore-block-db-path' not in cmd

    @patch.dict('os.environ', {'CEPH_VOLUME_OSDSPEC_AFFINITY': 'my-spec'})
    def test_add_objectstore_opts_with_affinity(self):
        self.raw.osd_mkfs_cmd = ['ceph-osd', '--mkfs']
        self.raw.add_objectstore_opts()
        assert '--osdspec-affinity' in self.raw.osd_mkfs_cmd
        assert 'my-spec' in self.raw.osd_mkfs_cmd


    @patch('ceph_volume.objectstore.seastore.prepare_utils.link_seastore_secondary')
    @patch('ceph_volume.objectstore.baseobjectstore.BaseObjectStore.prepare_osd_req')
    def test_prepare_osd_req_no_secondaries(self, m_super, m_link):
        self.raw.osd_id = '0'
        self.raw.prepare_osd_req()
        m_super.assert_called_once()
        m_link.assert_not_called()

    @patch('ceph_volume.objectstore.seastore.prepare_utils.link_seastore_secondary')
    @patch('ceph_volume.objectstore.baseobjectstore.BaseObjectStore.prepare_osd_req')
    def test_prepare_osd_req_with_secondaries(self, m_super, m_link):
        self.raw.osd_id = '0'
        self.raw.args.seastore_secondary = [('/dev/sdb', 'HDD'), ('/dev/sdc', 'SSD')]
        self.raw.prepare_osd_req()
        assert m_link.call_count == 2
        m_link.assert_any_call('/dev/sdb', 'HDD', 1, '0')
        m_link.assert_any_call('/dev/sdc', 'SSD', 2, '0')


    @patch('ceph_volume.objectstore.seastore.prepare_utils.unlink_seastore_secondaries')
    def test_unlink_bs_symlinks_no_block(self, m_unlink, fake_filesystem):
        osd_path = '/var/lib/ceph/osd/ceph-0'
        fake_filesystem.create_dir(osd_path)
        self.raw.osd_path = osd_path
        self.raw.unlink_bs_symlinks()
        m_unlink.assert_called_once_with(osd_path)

    @patch('ceph_volume.objectstore.seastore.prepare_utils.unlink_seastore_secondaries')
    def test_unlink_bs_symlinks_removes_block(self, m_unlink, fake_filesystem):
        osd_path = '/var/lib/ceph/osd/ceph-0'
        fake_filesystem.create_dir(osd_path)
        fake_filesystem.create_file(osd_path + '/block')
        self.raw.osd_path = osd_path
        self.raw.unlink_bs_symlinks()
        assert not fake_filesystem.exists(osd_path + '/block')
        m_unlink.assert_called_once_with(osd_path)


    @patch('ceph_volume.objectstore.seastore_raw.SeastoreRaw.unlink_bs_symlinks')
    @patch('ceph_volume.objectstore.seastore_raw.prepare_utils.create_osd_path')
    @patch('ceph_volume.objectstore.seastore_raw.prepare_utils.link_block')
    @patch('ceph_volume.objectstore.seastore_raw.prepare_utils.link_seastore_secondary')
    @patch('ceph_volume.terminal.success')
    def test__activate_no_secondaries(self,
                                      m_success, m_link_sec, m_link_block,
                                      m_create_osd_path, m_unlink,
                                      monkeypatch, conf_ceph_stub):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        monkeypatch.setattr('ceph_volume.util.system.path_is_mounted', lambda path: False)

        self.raw.block_device_path = '/dev/sda'
        self.raw.args.seastore_secondary = []
        self.raw._activate('0', 'abcd-1234')

        m_link_block.assert_called_once_with('/dev/sda', '0')
        m_link_sec.assert_not_called()
        m_success.assert_called_once()

    @patch('ceph_volume.objectstore.seastore_raw.SeastoreRaw.unlink_bs_symlinks')
    @patch('ceph_volume.objectstore.seastore_raw.prepare_utils.create_osd_path')
    @patch('ceph_volume.objectstore.seastore_raw.prepare_utils.link_block')
    @patch('ceph_volume.objectstore.seastore_raw.prepare_utils.link_seastore_secondary')
    @patch('ceph_volume.terminal.success')
    def test__activate_with_secondaries(self,
                                        m_success, m_link_sec, m_link_block,
                                        m_create_osd_path, m_unlink,
                                        monkeypatch, conf_ceph_stub):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        monkeypatch.setattr('ceph_volume.util.system.path_is_mounted', lambda path: False)

        self.raw.block_device_path = '/dev/sda'
        self.raw.args.seastore_secondary = [('/dev/sdb', 'HDD'), ('/dev/sdc', 'SSD')]
        self.raw._activate('0', 'abcd-1234')

        m_link_block.assert_called_once_with('/dev/sda', '0')
        assert m_link_sec.call_count == 2
        m_link_sec.assert_any_call('/dev/sdb', 'HDD', 1, '0')
        m_link_sec.assert_any_call('/dev/sdc', 'SSD', 2, '0')

    @patch('ceph_volume.objectstore.seastore_raw.SeastoreRaw.unlink_bs_symlinks')
    @patch('ceph_volume.objectstore.seastore_raw.prepare_utils.create_osd_path')
    @patch('ceph_volume.objectstore.seastore_raw.prepare_utils.link_block')
    @patch('ceph_volume.terminal.success')
    def test__activate_skips_bluestore_tool(self,
                                            m_success, m_link_block,
                                            m_create_osd_path, m_unlink,
                                            monkeypatch, conf_ceph_stub, fake_run):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        monkeypatch.setattr('ceph_volume.util.system.path_is_mounted', lambda path: False)

        self.raw.block_device_path = '/dev/sda'
        self.raw.args.seastore_secondary = []
        self.raw._activate('0', 'abcd-1234')

        run_cmds = [c['args'][0][0] for c in fake_run.calls]
        assert 'ceph-bluestore-tool' not in run_cmds

    # ------------------------------------------------------------------
    # Tests for the activate() override that bypasses direct_report().
    # SeaStore devices cannot be discovered via ceph-bluestore-tool
    # show-label, so the raw activate path must accept explicit
    # --device / --osd-id / --osd-uuid from the caller.
    # ------------------------------------------------------------------

    def _setup_activate(self, monkeypatch, **overrides):
        """Populate self.raw with args and matching instance attrs for activate()."""
        defaults = dict(
            no_tmpfs=False,
            seastore_secondary=[],
            devices=['/dev/sda'],
            osd_id='0',
            osd_fsid='abcd-1234',
        )
        defaults.update(overrides)
        monkeypatch.setenv('CEPH_VOLUME_SKIP_NEEDS_ROOT', '1')
        self.raw.args = Namespace(**defaults)
        self.raw.devices = self.raw.args.devices
        self.raw.osd_id = self.raw.args.osd_id
        self.raw.osd_fsid = self.raw.args.osd_fsid

    @patch('ceph_volume.objectstore.seastore_raw.SeastoreRaw._activate')
    def test_activate_calls__activate_with_explicit_ids(self,
                                                         m_activate,
                                                         monkeypatch):
        self._setup_activate(monkeypatch)
        self.raw.activate()
        m_activate.assert_called_once_with('0', 'abcd-1234')
        assert self.raw.block_device_path == '/dev/sda'

    def test_activate_requires_devices(self, monkeypatch):
        self._setup_activate(monkeypatch, devices=[])
        with pytest.raises(RuntimeError, match=r'--device'):
            self.raw.activate()

    def test_activate_requires_osd_id(self, monkeypatch):
        self._setup_activate(monkeypatch, osd_id='')
        with pytest.raises(RuntimeError, match=r'--osd-id'):
            self.raw.activate()

    def test_activate_requires_osd_fsid(self, monkeypatch):
        self._setup_activate(monkeypatch, osd_fsid='')
        with pytest.raises(RuntimeError, match=r'--osd-uuid'):
            self.raw.activate()

    @patch('ceph_volume.objectstore.seastore_raw.SeastoreRaw.unlink_bs_symlinks')
    @patch('ceph_volume.objectstore.seastore_raw.prepare_utils.create_osd_path')
    @patch('ceph_volume.objectstore.seastore_raw.prepare_utils.link_block')
    @patch('ceph_volume.objectstore.seastore_raw.prepare_utils.link_seastore_secondary')
    @patch('ceph_volume.terminal.success')
    def test_activate_propagates_secondaries_to__activate(self,
                                                           m_success,
                                                           m_link_sec,
                                                           m_link_block,
                                                           m_create_path,
                                                           m_unlink,
                                                           monkeypatch,
                                                           conf_ceph_stub):
        conf_ceph_stub('[global]\nfsid=asdf-lkjh')
        monkeypatch.setattr(system, 'chown', lambda path: 0)
        monkeypatch.setattr('ceph_volume.util.system.path_is_mounted',
                            lambda path: False)
        self._setup_activate(
            monkeypatch,
            seastore_secondary=[('/dev/sdb', 'HDD'), ('/dev/sdc', 'SSD')],
        )
        self.raw.activate()

        m_link_block.assert_called_once_with('/dev/sda', '0')
        assert m_link_sec.call_count == 2
        m_link_sec.assert_any_call('/dev/sdb', 'HDD', 1, '0')
        m_link_sec.assert_any_call('/dev/sdc', 'SSD', 2, '0')
