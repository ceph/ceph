import logging

from tasks.cephfs.xfstests_dev import XFSTestsDev

log = logging.getLogger(__name__)

class TestCopyRange(XFSTestsDev):
    def test_copy_range_001(self):
        self.runFstest('ceph/001', (30*60)) # 30mins timeout
    def test_copy_range_002(self):
        self.runFstest('ceph/002', 60)
    def test_copy_range_003(self):
        self.runFstest('ceph/003', 60)
