import argparse
import pytest
import os
from ceph_volume import exceptions, process
from ceph_volume.util import arg_validators
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

    def setup_method(self, fake_filesystem):
        self.validator = arg_validators.ValidDevice()

    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=False)
    def test_path_is_valid(self, m_has_bs_label,
                           fake_call, patch_bluestore_label,
                           device_info, monkeypatch):
        monkeypatch.setattr('ceph_volume.util.device.Device.exists', lambda: True)
        lsblk = {"TYPE": "disk", "NAME": "sda"}
        device_info(lsblk=lsblk)
        result = self.validator('/dev/sda')
        assert result.path == '/dev/sda'

    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=False)
    def test_path_is_invalid(self, m_has_bs_label,
                             fake_call, patch_bluestore_label,
                             device_info):
        lsblk = {"TYPE": "disk", "NAME": "sda"}
        device_info(lsblk=lsblk)
        with pytest.raises(argparse.ArgumentError):
            self.validator('/device/does/not/exist')

    @patch('ceph_volume.util.arg_validators.Device')
    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.api.lvm.get_single_lv', return_value=None)
    def test_dev_has_partitions(self, m_get_single_lv, m_has_bs_label, mocked_device, fake_call):
        mocked_device.return_value = MagicMock(
            exists=True,
            has_partitions=True,
        )
        with pytest.raises(RuntimeError):
            self.validator('/dev/foo')

class TestValidZapDevice(object):
    def setup_method(self):
        self.validator = arg_validators.ValidZapDevice()

    @patch('ceph_volume.util.arg_validators.Device')
    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.api.lvm.get_single_lv', return_value=None)
    def test_device_has_partition(self,  m_get_single_lv, m_has_bs_label, mocked_device):
        mocked_device.return_value = MagicMock(
            used_by_ceph=False,
            exists=True,
            has_partitions=True,
            has_gpt_headers=False,
            has_fs=False
        )
        self.validator.zap = False
        with pytest.raises(RuntimeError):
            assert self.validator('/dev/foo')

    @patch('ceph_volume.util.arg_validators.Device')
    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.api.lvm.get_single_lv', return_value=None)
    def test_device_has_no_partition(self,  m_get_single_lv, m_has_bs_label, mocked_device):
        mocked_device.return_value = MagicMock(
            used_by_ceph=False,
            exists=True,
            has_partitions=False,
            has_gpt_headers=False,
            has_fs=False
        )
        self.validator.zap = False
        assert self.validator('/dev/foo')

class TestValidDataDevice(object):
    def setup_method(self):
        self.validator = arg_validators.ValidDataDevice()

    @patch('ceph_volume.util.arg_validators.Device')
    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.api.lvm.get_single_lv', return_value=None)
    def test_device_used_by_ceph(self,  m_get_single_lv, m_has_bs_label, mocked_device, fake_call):
        mocked_device.return_value = MagicMock(
            used_by_ceph=True,
            exists=True,
            has_partitions=False,
            has_gpt_headers=False
        )
        with pytest.raises(SystemExit):
            self.validator.zap = False
            self.validator('/dev/foo')

    @patch('ceph_volume.util.arg_validators.Device')
    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=False)
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
            self.validator.zap = False
            self.validator('/dev/foo')

    @patch('ceph_volume.util.arg_validators.Device')
    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=True)
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
            self.validator.zap = False
            self.validator('/dev/foo')

class TestValidRawDevice(object):
    def setup_method(self):
        self.validator = arg_validators.ValidRawDevice()

    @patch('ceph_volume.util.arg_validators.Device')
    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=False)
    @patch('ceph_volume.util.arg_validators.disk.blkid')
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
            self.validator.zap = False
            self.validator('/dev/foo')

    @patch('ceph_volume.util.arg_validators.Device')
    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=False)
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
            self.validator.zap = False
            self.validator('/dev/foo')

    @patch('ceph_volume.util.arg_validators.Device')
    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=False)
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
        self.validator.zap = False
        assert self.validator('/dev/foo')

    @patch('ceph_volume.util.arg_validators.Device')
    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=False)
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
        self.validator.zap = False
        with pytest.raises(RuntimeError):
            assert self.validator('/dev/foo')

class TestValidBatchDevice(object):
    def setup_method(self):
        self.validator = arg_validators.ValidBatchDevice()

    @patch('ceph_volume.util.arg_validators.Device')
    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=False)
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
        with pytest.raises(argparse.ArgumentError):
            self.validator.zap = False
            self.validator('/dev/foo')

    @patch('ceph_volume.util.arg_validators.Device')
    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=False)
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
        self.validator.zap = False
        assert self.validator('/dev/foo')

class TestValidBatchDataDevice(object):
    def setup_method(self):
        self.validator = arg_validators.ValidBatchDataDevice()

    @patch('ceph_volume.util.arg_validators.Device')
    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=False)
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
        with pytest.raises(argparse.ArgumentError):
            self.validator.zap = False
            assert self.validator('/dev/foo')

    @patch('ceph_volume.util.arg_validators.Device')
    @patch('ceph_volume.util.arg_validators.disk.has_bluestore_label', return_value=False)
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
        self.validator.zap = False
        assert self.validator('/dev/foo')


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
