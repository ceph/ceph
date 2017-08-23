import pytest
import argparse
from ceph_volume.util import arg_validators


invalid_lv_paths = [
    '', 'lv_name', '///', '/lv_name', 'lv_name/',
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
