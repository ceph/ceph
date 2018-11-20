import logging
from StringIO import StringIO
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.fuse_mount import FuseMount
from teuthology.exceptions import CommandFailedError

log = logging.getLogger(__name__)


class TestCephFSShell(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    py_version = 'python'

    def setUp(self):
        CephFSTestCase.setUp(self)
        self.py_version = self.ctx.config.get('overrides', {}).get('python', 'python')
        log.info("using python version: {}".format(self.py_version))

    def _cephfs_shell(self, cmd, opts=None):
        args = ["cephfs-shell", "-c", self.mount_a.config_path]
        if opts is not None:
            args.extend(opts)
        args.extend(("--", cmd))
        log.info("Running command: {}".format(" ".join(args)))
        status = self.mount_a.client_remote.run(args=args, stdout=StringIO())
        return status.stdout.getvalue().strip()

    def test_help(self):
        """
        Test that help outputs commands.
        """

        o = self._cephfs_shell("help")

        log.info("output:\n{}".format(o))
