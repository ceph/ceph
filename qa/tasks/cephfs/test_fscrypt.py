from logging import getLogger

from io import StringIO
from tasks.cephfs.xfstests_dev import XFSTestsDev


log = getLogger(__name__)


class TestFscrypt(XFSTestsDev):

    def setup_xfsprogs_devs(self):
        self.install_xfsprogs = True

    def test_fscrypt_encrypt(self):
        # XXX: check_status is set to False so that we can check for command's
        # failure on our own (since this command doesn't set right error code
        # and error message in some cases) and print custom log messages
        # accordingly.
        proc = self.mount_a.client_remote.run(args=['sudo', 'env', 'DIFF_LENGTH=0',
            './check', '-g', 'encrypt'], cwd=self.xfstests_repo_path, stdout=StringIO(),
            stderr=StringIO(), timeout=900, check_status=False, omit_sudo=False,
            label='running tests for encrypt from xfstests-dev')

        if proc.returncode != 0:
            log.info('Command failed.')
        log.info(f'Command return value: {proc.returncode}')
        stdout, stderr = proc.stdout.getvalue(), proc.stderr.getvalue()
        log.info(f'Command stdout -\n{stdout}')
        log.info(f'Command stderr -\n{stderr}')

        # Currently only the 395,396,397,421,429,435,440,580,593,595 and 598
        # of the 26 test cases will be actually ran, all the others will be
        # skipped for now because of not supporting features in kernel or kceph.
        self.assertEqual(proc.returncode, 0)
        self.assertIn('Passed all 26 tests', stdout)

    def test_fscrypt_dummy_encryption_with_quick_group(self):
        self.write_local_config('test_dummy_encryption')

        # XXX: check_status is set to False so that we can check for command's
        # failure on our own (since this command doesn't set right error code
        # and error message in some cases) and print custom log messages
        # accordingly. This will take a long time and set the timeout to 3 hours.
        proc = self.mount_a.client_remote.run(args=['sudo', 'env', 'DIFF_LENGTH=0',
            './check', '-g', 'quick', '-E', './ceph.exclude'], cwd=self.xfstests_repo_path,
            stdout=StringIO(), stderr=StringIO(), timeout=10800, check_status=False,
            omit_sudo=False, label='running tests for dummy_encryption from xfstests-dev')

        if proc.returncode != 0:
            log.info('Command failed.')
        log.info(f'Command return value: {proc.returncode}')
        stdout, stderr = proc.stdout.getvalue(), proc.stderr.getvalue()
        log.info(f'Command stdout -\n{stdout}')
        log.info(f'Command stderr -\n{stderr}')

        # Currently, many test cases will be skipped due to unsupported features,
        # but still will be marked as successful.
        self.assertEqual(proc.returncode, 0)
        self.assertIn('Passed all ', stdout)
