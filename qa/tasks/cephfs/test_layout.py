import logging

from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

class TestInheritedLayout(CephFSTestCase):

    def test_layout_root(self):
        self.mount_a.run_shell(["mkdir", "a"])
        layout = self.mount_a.getfattr("a", "ceph.dir.layout") 
        log.info(layout)

    def test_layout(self):
        self.mount_a.run_shell(["mkdir", "-p", "a/b"])
        self.mount_a.setfattr("a", "ceph.dir.layout.stripe_count", "2")
        layout_a = self.mount_a.getfattr("a", "ceph.dir.layout")
        log.info(layout_a)
        layout_b = self.mount_a.getfattr("b", "ceph.dir.layout")
        log.info(layout_b)

        
        
        
