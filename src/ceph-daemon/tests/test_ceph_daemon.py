import imp
import unittest
import mock
import os


cd = imp.load_source("ceph-daemon", "ceph-daemon")


class TestCephDaemon(unittest.TestCase):
    def test_is_fsid(self):
        self.assertFalse(cd.is_fsid('no-uuid'))

    def test__get_parser_image(self):
        p = cd._get_parser()
        args = p.parse_args(['--image', 'foo', 'version'])
        assert args.image == 'foo'

    @mock.patch.dict(os.environ,{'CEPH_DAEMON_IMAGE':'bar'})
    def test__get_parser_image_with_envvar(self):
        p = cd._get_parser()
        args = p.parse_args(['version'])
        assert args.image == 'bar'
