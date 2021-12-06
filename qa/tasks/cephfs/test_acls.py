from logging import getLogger

from io import StringIO
from tasks.cephfs.xfstests_dev import XFSTestsDev


log = getLogger(__name__)


class TestACLs(XFSTestsDev):

    def test_acls(self):
        from tasks.cephfs.fuse_mount import FuseMount
        from tasks.cephfs.kernel_mount import KernelMount

        if isinstance(self.mount_a, FuseMount):
            log.info('client is fuse mounted')
        elif isinstance(self.mount_a, KernelMount):
            log.info('client is kernel mounted')

        # XXX: check_status is set to False so that we can check for command's
        # failure on our own (since this command doesn't set right error code
        # and error message in some cases) and print custom log messages
        # accordingly.
        proc = self.mount_a.client_remote.run(args=['sudo', 'env', 'DIFF_LENGTH=0',
            './check', 'generic/099'], cwd=self.xfstests_repo_path, stdout=StringIO(),
            stderr=StringIO(), timeout=30, check_status=False,omit_sudo=False,
            label='running tests for ACLs from xfstests-dev')

        if proc.returncode != 0:
            log.info('Command failed.')
        log.info(f'Command return value: {proc.returncode}')
        stdout, stderr = proc.stdout.getvalue(), proc.stderr.getvalue()
        log.info(f'Command stdout -\n{stdout}')
        log.info(f'Command stderr -\n{stderr}')

        self.assertEqual(proc.returncode, 0)
        success_line = 'Passed all 1 tests'
        self.assertIn(success_line, stdout)
