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

class TestCephAdm(unittest.TestCase):
    def test_is_fsid(self):
        self.assertFalse(cd.is_fsid('no-uuid'))

    def test__get_parser_image(self):
        p = cd._get_parser()
        args = p.parse_args(['--image', 'foo', 'version'])
        assert args.image == 'foo'

    @mock.patch.dict(os.environ,{'CEPHADM_IMAGE':'bar'})
    def test__get_parser_image_with_envvar(self):
        p = cd._get_parser()
        args = p.parse_args(['version'])
        assert args.image == 'bar'

    def test_CustomValidation(self):
        p = cd._get_parser()
        assert p.parse_args(['deploy', '--name', 'mon.a', '--fsid', 'fsid'])

        with pytest.raises(SystemExit):
            p.parse_args(['deploy', '--name', 'wrong', '--fsid', 'fsid'])
