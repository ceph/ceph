import logging
import time
from datetime import datetime
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

class TestRstats(CephFSTestCase):
    MDSS_REQUIRED = 3
    CLIENTS_REQUIRED = 1

    def test_rstats_flushed_to(self):
        """
        When 'get rstats' ASOK command indicates rstats dirtied before a given
        time are propagated to root inode. Check if root inode's rstat reflects
        the change.
        """
        def _timestr_to_epoch(s):
            try:
                t = datetime.strptime(s[0:s.find('+')], '%Y-%m-%dT%H:%M:%S.%f')
                return float(datetime.strftime(t, '%s.%f'))
            except ValueError:
                return float(0);

        def _is_rstats_flushed(start):
            flushed_to = self.fs.rank_tell(['get', 'rstats'])['flushed_to']
            return _timestr_to_epoch(flushed_to) >= start

        def _run_test(path):
            self.mount_a.run_shell(['touch', path])
            self.mount_a.run_shell(['sync', path])
            stat = self.mount_a.stat(path)
            ctime = stat['st_ctime'];

            start = _timestr_to_epoch(self.fs.rank_tell(['get', 'rstats'])['mds_time'])
            self.wait_until_true(lambda : _is_rstats_flushed(start), 120)

            rctime = _timestr_to_epoch(self.fs.rank_tell(['get', 'rstats'])['rctime'])
            self.assertGreaterEqual(rctime + 0.1, ctime)

        path = 'd1/d2/d3/d4/d5/d6/d7/d8'
        self.mount_a.run_shell(['mkdir', '-p', path])
        self.mount_a.run_shell(['sync', path])
        time.sleep(10)

        # single mds
        _run_test(path)

        # multiple mds
        self.fs.set_max_mds(3)
        status = self.fs.wait_for_daemons()

        self.mount_a.setfattr("d1/d2", "ceph.dir.pin", "1")
        self.mount_a.setfattr("d1/d2/d3/d4", "ceph.dir.pin", "2")
        self.mount_a.setfattr("d1/d2/d3/d4/d5/d6", "ceph.dir.pin", "0")
        self._wait_subtrees(status, 0, [('/d1/d2', 1), ('/d1/d2/d3/d4', 2), ('/d1/d2/d3/d4/d5/d6', 0)])

        _run_test(path);
