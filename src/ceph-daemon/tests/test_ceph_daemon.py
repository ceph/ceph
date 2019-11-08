import imp
import unittest


cd = imp.load_source("ceph-daemon", "ceph-daemon")


class TestCephDaemon(unittest.TestCase):
    def test_is_fsid(self):
        self.assertFalse(cd.is_fsid('no-uuid'))
