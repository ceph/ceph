import pytest
import argparse
from ceph_volume import exceptions
from ceph_volume.util import arg_validators


invalid_lv_paths = [
    '', 'lv_name', '/lv_name', 'lv_name/',
    '/dev/lv_group/lv_name'
]


class TestLVPath(object):

    def setup(self):
        self.validator = arg_validators.LVPath()

    @pytest.mark.parametrize('path', invalid_lv_paths)
    def test_no_slash_is_an_error(self, path):
        with pytest.raises(argparse.ArgumentError):
            self.validator(path)

    def test_is_valid(self):
        path = 'vg/lv'
        assert self.validator(path) == path

    def test_abspath_is_valid(self):
        path = '/'
        assert self.validator(path) == path


class TestOSDPath(object):

    def setup(self):
        self.validator = arg_validators.OSDPath()

    def test_is_not_root(self):
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
        assert 'Required file (ceph_fsid) was not found in OSD' in str(error)
