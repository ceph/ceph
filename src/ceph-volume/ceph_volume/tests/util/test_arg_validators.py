import argparse
import pytest
import os
from ceph_volume import exceptions
from ceph_volume.util import arg_validators


class TestOSDPath(object):

    def setup(self):
        self.validator = arg_validators.OSDPath()

    def test_is_not_root(self, monkeypatch):
        monkeypatch.setattr(os, 'getuid', lambda: 100)
        with pytest.raises(exceptions.SuperUserError):
            self.validator('')

    def test_path_is_not_a_directory(self, is_root, tmpfile, monkeypatch):
        monkeypatch.setattr(arg_validators.disk, 'is_partition', lambda x: False)
        validator = arg_validators.OSDPath()
        with pytest.raises(argparse.ArgumentError):
            validator(tmpfile())

    def test_files_are_missing(self, is_root, tmpdir, monkeypatch):
        tmppath = str(tmpdir)
        monkeypatch.setattr(arg_validators.disk, 'is_partition', lambda x: False)
        validator = arg_validators.OSDPath()
        with pytest.raises(argparse.ArgumentError) as error:
            validator(tmppath)
        assert 'Required file (ceph_fsid) was not found in OSD' in str(error.value)


class TestExcludeGroupOptions(object):

    def setup(self):
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

    def setup(self):
        self.validator = arg_validators.ValidDevice()

    def test_path_is_valid(self, fake_call):
        result = self.validator('/')
        assert result.abspath == '/'

    def test_path_is_invalid(self, fake_call):
        with pytest.raises(argparse.ArgumentError):
            self.validator('/device/does/not/exist')


class TestValidFraction(object):

    def setup(self):
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

