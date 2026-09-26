from io import StringIO
from logging import getLogger
from textwrap import dedent
import os
import time

from tasks.ceph_test_case import TestTimeoutError
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = getLogger(__name__)


class TestReaddirCachePressure(CephFSTestCase):
    """
    How the MDS serves a client walking a large directory, such as a du or
    find, while its cache is under pressure.
    """

    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 1

    # A readdir reply is bounded by max_bytes, which puts about 1000 of these
    # entries in a page: a walk of this directory takes several replies.
    WALK_FILES = 6000
    WALK_PAUSE_AT = 1500

    CONFIGS = ('mds_cache_memory_limit',
               'mds_cache_reservation',
               'mds_readdir_keep_complete_interval',
               'mds_readdir_withhold_caps_over_limit',
               'mds_max_caps_per_client',
               'mds_session_cap_acquisition_throttle')

    def tearDown(self):
        for key in self.CONFIGS:
            self.config_rm('mds', key)
        super().tearDown()

    def _mds_perf(self, section, key):
        return self.fs.rank_asok(['perf', 'dump', section])[section][key]

    def _is_complete(self, path):
        dirs = self.fs.rank_tell(['dump', 'dir', path])
        return 'complete' in dirs[0]['states']

    def _cache_bytes(self):
        return self.fs.rank_tell(['cache', 'status'])['pool']['bytes']

    def _populate(self, path, count):
        self.mount_a.run_shell_payload(f"""
set -e
mkdir -p {path}
cd {path}
seq 1 {count} | sed 's/^/f/' | xargs -r touch
""")
        self.fs.rank_asok(['flush', 'journal'])
        # Leave the dentries in the MDS cache with no client caps on them.
        self.mount_a.umount_wait()
        self.mount_a.mount_wait()

    def _start_walk(self, path):
        """
        Read path in a background process, pausing partway through: the
        process creates ctl/paused once it has read WALK_PAUSE_AT entries,
        then waits for ctl/go before reading the rest, and prints the number
        of entries it read.
        """
        self.mount_a.run_shell(["mkdir", "-p", "ctl"])
        mnt = self.mount_a.hostfs_mntpt
        pyscript = dedent(f"""
            import os
            import time

            n = 0
            with os.scandir("{os.path.join(mnt, path)}") as it:
                for e in it:
                    n += 1
                    if n == {self.WALK_PAUSE_AT}:
                        open("{os.path.join(mnt, 'ctl/paused')}", "w").close()
                        while not os.path.exists("{os.path.join(mnt, 'ctl/go')}"):
                            time.sleep(0.5)
            print(n)
            """)
        p = self.mount_a._run_python(pyscript)
        self.mount_a.background_procs.append(p)
        self.mount_a.wait_for_visible("ctl/paused")
        return p

    def _finish_walk(self, p):
        self.mount_a.run_shell(["touch", "ctl/go"])
        p.wait()
        self.assertEqual(int(p.stdout.getvalue().strip()), self.WALK_FILES)

    def _squeeze_cache(self):
        """
        Leave the cache over its reservation, so that it is trimmed, but
        within mds_cache_memory_limit.
        """
        size = self._cache_bytes()
        self.config_set('mds', 'mds_cache_reservation', '0.5')
        self.config_set('mds', 'mds_cache_memory_limit', str(int(size * 1.3)))

    def _release_cache(self):
        self.config_rm('mds', 'mds_cache_memory_limit')
        self.config_rm('mds', 'mds_cache_reservation')

    def test_walk_keeps_dir_complete(self):
        """
        That trimming for cache pressure leaves alone a dirfrag that a
        readdir is partway through, so that the walk does not have to fetch
        it again.
        """
        # Room to squeeze the cache and resume the walk within the interval.
        self.config_set('mds', 'mds_readdir_keep_complete_interval', '60')
        self._populate("big", self.WALK_FILES)
        self.assertTrue(self._is_complete("big"))

        p = self._start_walk("big")
        spared = self._mds_perf('mds', 'dir_trim_spared')
        refetched = self._mds_perf('mds', 'dir_readdir_refetch')

        self._squeeze_cache()
        self.wait_until_true(
            lambda: self._mds_perf('mds', 'dir_trim_spared') > spared,
            timeout=30)
        self.assertTrue(self._is_complete("big"),
                        "big lost its complete state to trimming")
        self._release_cache()

        self._finish_walk(p)
        self.assertEqual(self._mds_perf('mds', 'dir_readdir_refetch'),
                         refetched,
                         "the walk fetched big again")

    def test_walk_refetches_when_not_kept(self):
        """
        That with mds_readdir_keep_complete_interval = 0, trimming for cache
        pressure makes a walk partway through a dirfrag fetch it again, and
        that the walk still returns every entry exactly once.
        """
        self.config_set('mds', 'mds_readdir_keep_complete_interval', '0')
        self._populate("big", self.WALK_FILES)

        p = self._start_walk("big")
        refetched = self._mds_perf('mds', 'dir_readdir_refetch')

        self._squeeze_cache()
        self.wait_until_true(lambda: not self._is_complete("big"), timeout=30)
        self._release_cache()

        self._finish_walk(p)
        self.assertGreater(self._mds_perf('mds', 'dir_readdir_refetch'),
                           refetched)

    def _caps_added(self):
        # mds_mem.cap+ is refreshed on the MDS tick.
        return self._mds_perf('mds_mem', 'cap+')

    def _list_caps_added(self, path):
        """The number of caps added by listing path from a fresh mount."""
        self.mount_a.umount_wait()
        self.mount_a.mount_wait()
        tick = float(self.fs.get_config('mds_tick_interval', service_type='mds'))
        time.sleep(tick * 2)
        before = self._caps_added()
        self.mount_a.run_shell(["find", path, "-maxdepth", "1"],
                               stdout=StringIO())
        time.sleep(tick * 2)
        return self._caps_added() - before

    def test_withhold_caps_over_limit(self):
        """
        That with mds_readdir_withhold_caps_over_limit, a readdir served
        while the cache is over mds_cache_memory_limit gives out no new caps,
        and that one served otherwise, or with the option unset, does.
        """
        files = 500
        self._populate("d", files)

        self.config_set('mds', 'mds_readdir_withhold_caps_over_limit', 'true')
        self.assertGreaterEqual(self._list_caps_added("d"), files)

        # Far below what the cache already holds.
        self.config_set('mds', 'mds_cache_memory_limit', '16384')
        self.assertLess(self._list_caps_added("d"), files // 10)

        self.config_set('mds', 'mds_readdir_withhold_caps_over_limit', 'false')
        self.assertGreaterEqual(self._list_caps_added("d"), files)

    def test_throttled_readdir_holds_no_locks(self):
        """
        That a readdir held back by the cap acquisition throttle does not
        hold the locks on its path while it waits, so that it does not
        block a rename of the directory being read.
        """
        files = 600
        self._populate("big", files)
        self.mount_b.run_shell(["mkdir", "-p", "p/d"])
        self.mount_b.run_shell(["touch", "p/d/f1", "p/d/f2"])

        # One readdir of big leaves the session acquiring caps several times
        # faster than allowed, and holding more than it may.
        self.config_set('mds', 'mds_max_caps_per_client', '1')
        self.config_set('mds', 'mds_session_cap_acquisition_throttle',
                        str(files // 6))
        self.mount_a.run_shell(["find", "big", "-maxdepth", "1"],
                               stdout=StringIO())

        throttled = self._mds_perf('mds_server', 'cap_acquisition_throttle')
        ls = self.mount_a.run_shell(["ls", "-f", "p/d"], wait=False,
                                    stdout=StringIO())
        self.wait_until_true(
            lambda: self._mds_perf('mds_server', 'cap_acquisition_throttle') > throttled,
            timeout=30)

        mv = self.mount_b.run_shell(["mv", "p/d", "p/e"], wait=False)
        try:
            self.wait_until_true(lambda: mv.finished, timeout=15, period=1)
        except TestTimeoutError:
            self.fail("rename blocked behind a cap-throttled readdir")
        self.assertEqual(mv.exitstatus, 0)

        # Let the readdir through.
        self.config_rm('mds', 'mds_session_cap_acquisition_throttle')
        ls.wait()
        self.assertEqual(sorted(ls.stdout.getvalue().split()),
                         ['.', '..', 'f1', 'f2'])
