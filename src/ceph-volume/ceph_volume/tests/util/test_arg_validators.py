import argparse
import pytest
import os
from ceph_volume import exceptions, process
from ceph_volume.util import arg_validators, device
from mock.mock import patch, MagicMock


class TestOSDPath(object):

    def setup_method(self):
        self.validator = arg_validators.OSDPath()

    def test_is_not_root(self, monkeypatch):
        monkeypatch.setattr(os, 'getuid', lambda: 100)
        with pytest.raises(exceptions.SuperUserError):
            self.validator('')

    def test_path_is_not_a_directory(self, is_root, monkeypatch, fake_filesystem):
        fake_file = fake_filesystem.create_file('/tmp/foo')
        monkeypatch.setattr(arg_validators.disk, 'is_partition', lambda x: False)
        validator = arg_validators.OSDPath()
        with pytest.raises(argparse.ArgumentError):
            validator(fake_file.path)

    def test_files_are_missing(self, is_root, tmpdir, monkeypatch):
        tmppath = str(tmpdir)
        monkeypatch.setattr(arg_validators.disk, 'is_partition', lambda x: False)
        validator = arg_validators.OSDPath()
        with pytest.raises(argparse.ArgumentError) as error:
            validator(tmppath)
        assert 'Required file (ceph_fsid) was not found in OSD' in str(error.value)


class TestExcludeGroupOptions(object):

    def setup_method(self):
        self.parser = argparse.ArgumentParser()

    def test_flags_in_one_group(self):
        argv = ['<prog>', '--filestore', '--bar']
        filestore_group = self.parser.add_argument_group('filestore')
        bluestore_group = self.parser.add_argument_group('bluestore')
        filestore_group.add_argument('--filestore')
        bluestore_group.add_argument('--bluestore')
        result = arg_validators.exclude_group_options(
            self.parser,
            ['filestore', 'bluestore'],
            argv=argv
        )
        assert result is None

    def test_flags_in_no_group(self):
        argv = ['<prog>', '--foo', '--bar']
        filestore_group = self.parser.add_argument_group('filestore')
        bluestore_group = self.parser.add_argument_group('bluestore')
        filestore_group.add_argument('--filestore')
        bluestore_group.add_argument('--bluestore')
        result = arg_validators.exclude_group_options(
            self.parser,
            ['filestore', 'bluestore'],
            argv=argv
        )
        assert result is None

    def test_flags_conflict(self, capsys):
        argv = ['<prog>', '--filestore', '--bluestore']
        filestore_group = self.parser.add_argument_group('filestore')
        bluestore_group = self.parser.add_argument_group('bluestore')
        filestore_group.add_argument('--filestore')
        bluestore_group.add_argument('--bluestore')

        arg_validators.exclude_group_options(
            self.parser, ['filestore', 'bluestore'], argv=argv
        )
        stdout, stderr = capsys.readouterr()
        assert 'Cannot use --filestore (filestore) with --bluestore (bluestore)' in stderr


class TestValidDevice(object):
    @patch('ceph_volume.util.disk.has_bluestore_label', return_value=False)
    def test_path_is_valid(self, m_has_bs_label,
                           fake_call, patch_bluestore_label,
                           device_info, monkeypatch):
        monkeypatch.setattr('ceph_volume.util.device.Device.exists', lambda: True)
        lsblk = {"TYPE": "disk", "NAME": "sda"}
        device_info(lsblk=lsblk)
        result = device.ValidDevice('/dev/sda').check_device()
        assert result.path == '/dev/sda'

    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=False)
    def test_path_is_invalid(self, m_has_bs_label,
                             fake_call, patch_bluestore_label,
                             device_info):
        lsblk = {"TYPE": "disk", "NAME": "sda"}
        device_info(lsblk=lsblk)
        with pytest.raises(RuntimeError):
            assert device.ValidDevice('/device/does/not/exist').check_device()

    @patch('ceph_volume.util.device.Device')
    @patch('ceph_volume.util.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.api.lvm.get_single_lv', return_value=None)
    def test_dev_has_partitions(self, m_get_single_lv, m_has_bs_label, mocked_device, fake_call):
        mocked_device.return_value = MagicMock(
            exists=True,
            has_partitions=True,
        )
        with pytest.raises(RuntimeError):
            assert device.ValidDevice('/dev/foo').check_device()

class TestValidZapDevice(object):
    @patch('ceph_volume.util.device.Device')
    @patch('ceph_volume.util.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.api.lvm.get_single_lv', return_value=None)
    def test_device_has_partition(self,  m_get_single_lv, m_has_bs_label, mocked_device):
        mocked_device.return_value = MagicMock(
            used_by_ceph=False,
            exists=True,
            has_partitions=True,
            has_gpt_headers=False,
            has_fs=False
        )
        with pytest.raises(RuntimeError):
            assert device.ValidZapDevice('/dev/foo').check_device()

    @patch('ceph_volume.util.device.Device')
    @patch('ceph_volume.util.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.api.lvm.get_single_lv', return_value=None)
    def test_device_has_no_partition(self,  m_get_single_lv, m_has_bs_label, mocked_device, monkeypatch):
        monkeypatch.setattr('ceph_volume.util.device.get_osd_ids_up', lambda: [123])
        monkeypatch.setattr('ceph_volume.util.disk.is_locked_raw_device', lambda dev_path: False)
        mocked_device.return_value = MagicMock(
            has_bluestore_label=False,
            used_by_ceph=False,
            exists=True,
            has_partitions=False,
            has_gpt_headers=False,
            has_fs=False
        )
        assert device.ValidZapDevice('/dev/foo').check_device()

    @patch('ceph_volume.util.device.Device')
    @patch('ceph_volume.util.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.api.lvm.get_single_lv', return_value=None)
    def test_device_is_locked(self,  m_get_single_lv, m_has_bs_label, mocked_device, monkeypatch):
        monkeypatch.setattr('ceph_volume.util.device.get_osd_ids_up', lambda: [123])
        monkeypatch.setattr('ceph_volume.util.disk.is_locked_raw_device', lambda dev_path: True)
        mocked_device.return_value = MagicMock(
            has_bluestore_label=False,
            used_by_ceph=False,
            exists=True,
            has_partitions=False,
            has_gpt_headers=False,
            has_fs=False
        )
        with pytest.raises(RuntimeError):
            assert device.ValidZapDevice('/dev/foo').check_device()

class TestValidDataDevice(object):
    @patch('ceph_volume.util.device.Device')
    @patch('ceph_volume.util.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.api.lvm.get_single_lv', return_value=None)
    def test_device_used_by_ceph(self,  m_get_single_lv, m_has_bs_label, mocked_device, fake_call):
        mocked_device.return_value = MagicMock(
            used_by_ceph=True,
            exists=True,
            has_partitions=False,
            has_gpt_headers=False
        )
        with pytest.raises(SystemExit):
            assert device.ValidDataDevice('/dev/foo').check_device()

    @patch('ceph_volume.util.device.Device')
    @patch('ceph_volume.util.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.api.lvm.get_single_lv', return_value=None)
    def test_device_has_fs(self,  m_get_single_lv, m_has_bs_label, mocked_device, fake_call):
        mocked_device.return_value = MagicMock(
            used_by_ceph=False,
            exists=True,
            has_partitions=False,
            has_gpt_headers=False,
            has_fs=True
        )
        with pytest.raises(RuntimeError):
            assert device.ValidDataDevice('/dev/foo').check_device()

    @patch('ceph_volume.util.device.Device')
    @patch('ceph_volume.util.disk.has_bluestore_label', return_value=True)
    @patch('ceph_volume.api.lvm.get_single_lv', return_value=None)
    def test_device_has_bs_signature(self,  m_get_single_lv, m_has_bs_label, mocked_device, fake_call):
        mocked_device.return_value = MagicMock(
            used_by_ceph=False,
            exists=True,
            has_partitions=False,
            has_gpt_headers=False,
            has_fs=False
        )
        with pytest.raises(RuntimeError):
            assert device.ValidDataDevice('/dev/foo').check_device()

class TestValidRawDevice(object):
    @patch('ceph_volume.util.device.Device')
    @patch('ceph_volume.util.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.util.disk.blkid')
    @patch('ceph_volume.api.lvm.get_single_lv', return_value=None)
    def test_dmcrypt_device_already_prepared(self,  m_get_single_lv, m_blkid, m_has_bs_label, mocked_device, fake_call, monkeypatch):
        def mock_call(cmd, **kw):
            return ('', '', 1)
        monkeypatch.setattr(process, 'call', mock_call)
        m_blkid.return_value = {'UUID': '8fd92779-ad78-437c-a06f-275f7170fa74', 'TYPE': 'crypto_LUKS'}
        mocked_device.return_value = MagicMock(
            used_by_ceph=False,
            exists=True,
            has_partitions=False,
            has_gpt_headers=False,
            has_fs=False
        )
        with pytest.raises(SystemExit):
            assert device.ValidRawDevice('/dev/foo').check_device()

    @patch('ceph_volume.util.device.Device')
    @patch('ceph_volume.util.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.api.lvm.get_single_lv', return_value=None)
    def test_device_already_prepared(self,  m_get_single_lv, m_has_bs_label, mocked_device, fake_call):
        mocked_device.return_value = MagicMock(
            used_by_ceph=False,
            exists=True,
            has_partitions=False,
            has_gpt_headers=False,
            has_fs=False
        )
        with pytest.raises(SystemExit):
            assert device.ValidRawDevice('/dev/foo').check_device()

    @patch('ceph_volume.util.device.Device')
    @patch('ceph_volume.util.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.api.lvm.get_single_lv', return_value=None)
    def test_device_not_prepared(self,  m_get_single_lv, m_has_bs_label, mocked_device, fake_call, monkeypatch):
        def mock_call(cmd, **kw):
            return ('', '', 1)
        monkeypatch.setattr(process, 'call', mock_call)
        mocked_device.return_value = MagicMock(
            used_by_ceph=False,
            exists=True,
            has_partitions=False,
            has_gpt_headers=False,
            has_fs=False
        )
        assert device.ValidRawDevice('/dev/foo').check_device()

    @patch('ceph_volume.util.device.Device')
    @patch('ceph_volume.util.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.api.lvm.get_single_lv', return_value=None)
    def test_device_has_partition(self,  m_get_single_lv, m_has_bs_label, mocked_device, fake_call, monkeypatch):
        def mock_call(cmd, **kw):
            return ('', '', 1)
        monkeypatch.setattr(process, 'call', mock_call)
        mocked_device.return_value = MagicMock(
            used_by_ceph=False,
            exists=True,
            has_partitions=True,
            has_gpt_headers=False,
            has_fs=False
        )
        with pytest.raises(RuntimeError):
            assert device.ValidRawDevice('/dev/foo').check_device()

class TestValidBatchDevice(object):
    @patch('ceph_volume.util.device.Device')
    @patch('ceph_volume.util.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.api.lvm.get_single_lv', return_value=None)
    def test_device_is_partition(self,  m_get_single_lv, m_has_bs_label, mocked_device, fake_call):
        mocked_device.return_value = MagicMock(
            used_by_ceph=False,
            exists=True,
            has_partitions=False,
            has_gpt_headers=False,
            has_fs=False,
            is_partition=True
        )
        with pytest.raises(RuntimeError):
            assert device.ValidBatchDevice('/dev/foo').check_device()

    @patch('ceph_volume.util.device.Device')
    @patch('ceph_volume.util.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.api.lvm.get_single_lv', return_value=None)
    def test_device_is_not_partition(self,  m_get_single_lv, m_has_bs_label, mocked_device, fake_call, fake_filesystem):
        mocked_device.return_value = MagicMock(
            used_by_ceph=False,
            exists=True,
            has_partitions=False,
            has_gpt_headers=False,
            has_fs=False,
            is_partition=False
        )
        assert device.ValidBatchDevice('/dev/foo').check_device()

class TestValidBatchDataDevice(object):
    @patch('ceph_volume.util.device.Device')
    @patch('ceph_volume.util.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.api.lvm.get_single_lv', return_value=None)
    def test_device_is_partition(self,  m_get_single_lv, m_has_bs_label, mocked_device, fake_call, fake_filesystem):
        mocked_device.return_value = MagicMock(
            used_by_ceph=False,
            exists=True,
            has_partitions=False,
            has_gpt_headers=False,
            has_fs=False,
            is_partition=True
        )
        with pytest.raises(RuntimeError):
            assert device.ValidBatchDataDevice('/dev/foo').check_device()

    @patch('ceph_volume.util.device.Device')
    @patch('ceph_volume.util.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.api.lvm.get_single_lv', return_value=None)
    def test_device_is_not_partition(self,  m_get_single_lv, m_has_bs_label, mocked_device, fake_call):
        mocked_device.return_value = MagicMock(
            used_by_ceph=False,
            exists=True,
            has_partitions=False,
            has_gpt_headers=False,
            has_fs=False,
            is_partition=False
        )
        assert device.ValidBatchDataDevice('/dev/foo').check_device()


class TestValidFraction(object):

    def setup_method(self):
        self.validator = arg_validators.ValidFraction()

    def test_fraction_is_valid(self, fake_call):
        result = self.validator('0.8')
        assert result == 0.8

    def test_fraction_not_float(self, fake_call):
        with pytest.raises(ValueError):
            self.validator('xyz')

    def test_fraction_is_nan(self, fake_call):
        with pytest.raises(argparse.ArgumentError):
            self.validator('NaN')

    def test_fraction_is_negative(self, fake_call):
        with pytest.raises(argparse.ArgumentError):
            self.validator('-1.0')

    def test_fraction_is_zero(self, fake_call):
        with pytest.raises(argparse.ArgumentError):
            self.validator('0.0')

    def test_fraction_is_greater_one(self, fake_call):
        with pytest.raises(argparse.ArgumentError):
            self.validator('1.1')
