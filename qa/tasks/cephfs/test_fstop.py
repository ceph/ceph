import logging
import json

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from teuthology.exceptions import CommandFailedError
from teuthology.contextutil import safe_while

log = logging.getLogger(__name__)


class TestFSTop(CephFSTestCase):
    CLIENTS_REQUIRED = 2

    def setUp(self):
        super(TestFSTop, self).setUp()
        self._enable_mgr_stats_plugin()

    def tearDown(self):
        self._disable_mgr_stats_plugin()
        super(TestFSTop, self).tearDown()

    def _enable_mgr_stats_plugin(self):
        return self.get_ceph_cmd_stdout("mgr", "module", "enable", "stats")

    def _disable_mgr_stats_plugin(self):
        return self.get_ceph_cmd_stdout("mgr", "module", "disable", "stats")

    def _fstop_dump(self, *args):
        return self.mount_a.run_shell(['cephfs-top',
                                       '--id=admin',
                                       *args]).stdout.getvalue()

    def _get_metrics(self, verifier_callback, trials, *args):
        metrics = None
        done = False
        with safe_while(sleep=1, tries=trials, action='wait for metrics') as proceed:
            while proceed():
                metrics = json.loads(self._fstop_dump(*args))
                done = verifier_callback(metrics)
                if done:
                    break
        return done, metrics

    # TESTS
    def test_fstop_non_existent_cluster(self):
        try:
            self.mount_a.run_shell(['cephfs-top',
                                    '--cluster=hpec',
                                    '--id=admin',
                                    '--selftest'])
        except CommandFailedError:
            pass
        else:
            raise RuntimeError('expected cephfs-top command to fail.')

    def test_fstop(self):
        try:
            self.mount_a.run_shell(['cephfs-top',
                                    '--id=admin',
                                    '--selftest'])
        except CommandFailedError:
            raise RuntimeError('cephfs-top --selftest failed')

    def test_dump(self):
        """
        Tests 'cephfs-top --dump' output is valid
        """
        def verify_fstop_metrics(metrics):
            clients = metrics.get('filesystems').get(self.fs.name, {})
            if str(self.mount_a.get_global_id()) in clients and \
               str(self.mount_b.get_global_id()) in clients:
                return True
            return False

        # validate
        valid, metrics = self._get_metrics(verify_fstop_metrics, 30, '--dump')
        log.debug("metrics={0}".format(metrics))
        self.assertTrue(valid)

    def test_dumpfs(self):
        """
        Tests 'cephfs-top --dumpfs' output is valid
        """
        newfs_name = "cephfs_b"

        def verify_fstop_metrics(metrics):
            clients = metrics.get(newfs_name, {})
            if self.fs.name not in metrics and \
               str(self.mount_b.get_global_id()) in clients:
                return True
            return False

        # umount mount_b, mount another filesystem on it and use --dumpfs filter
        self.mount_b.umount_wait()

        self.run_ceph_cmd("fs", "flag", "set", "enable_multiple", "true",
                          "--yes-i-really-mean-it")

        # create a new filesystem
        fs_b = self.mds_cluster.newfs(name=newfs_name, create=True)

        # mount cephfs_b on mount_b
        self.mount_b.mount_wait(cephfs_name=fs_b.name)

        # validate
        valid, metrics = self._get_metrics(verify_fstop_metrics, 30,
                                           '--dumpfs={}'.format(newfs_name))
        log.debug("metrics={0}".format(metrics))

        # restore mount_b
        self.mount_b.umount_wait()
        self.mount_b.mount_wait(cephfs_name=self.fs.name)

        self.assertTrue(valid)
