from logging import getLogger

from io import BytesIO
from tasks.cephfs.xfstests_dev import XFSTestsDev

log = getLogger(__name__)

class TestACLs(XFSTestsDev):

    def test_acls(self):
        from tasks.cephfs.fuse_mount import FuseMount
        from tasks.cephfs.kernel_mount import KernelMount

        # TODO: make xfstests-dev compatible with ceph-fuse. xfstests-dev
        # remounts CephFS before running tests using kernel, so ceph-fuse
        # mounts are never actually testsed.
        if isinstance(self.mount_a, FuseMount):
            log.info('client is fuse mounted')
            self.skipTest('Requires kernel client; xfstests-dev not '\
                          'compatible with ceph-fuse ATM.')
        elif isinstance(self.mount_a, KernelMount):
            log.info('client is kernel mounted')

        self.mount_a.client_remote.run(args=['sudo', './check',
            'generic/099'], cwd=self.repo_path, stdout=BytesIO(),
            stderr=BytesIO(), timeout=30, check_status=True,
            label='running tests for ACLs from xfstests-dev')
