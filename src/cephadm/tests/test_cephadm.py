import argparse
import mock
import os
import sys
import unittest

import pytest

if sys.version_info >= (3, 3):
    from importlib.machinery import SourceFileLoader
    cd = SourceFileLoader('cephadm', 'cephadm').load_module()
else:
    import imp
    cd = imp.load_source('cephadm', 'cephadm')

class TestCephAdm(object):
    def test_is_fsid(self):
        assert not cd.is_fsid('no-uuid')

    def test__get_parser_image(self):
        args = cd._parse_args(['--image', 'foo', 'version'])
        assert args.image == 'foo'

    @mock.patch.dict(os.environ,{'CEPHADM_IMAGE':'bar'})
    def test__get_parser_image_with_envvar(self):
        args = cd._parse_args(['version'])
        assert args.image == 'bar'

    def test_CustomValidation(self):
        assert cd._parse_args(['deploy', '--name', 'mon.a', '--fsid', 'fsid'])

        with pytest.raises(SystemExit):
            cd._parse_args(['deploy', '--name', 'wrong', '--fsid', 'fsid'])

    @pytest.mark.parametrize("test_input, expected", [
        ("podman version 1.6.2", (1,6,2)),
        ("podman version 1.6.2-stable2", (1,6,2)),
    ])
    def test_parse_podman_version(self, test_input, expected):
        assert cd._parse_podman_version(test_input) == expected

    def test_parse_podman_version_invalid(self):
        with pytest.raises(ValueError) as res:
            cd._parse_podman_version('podman version inval.id')
        assert 'inval' in str(res.value)
