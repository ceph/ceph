"""
Which nowsync kernel clients the MDS builds a lock cache for and issues
Dc and Du to, so that they can create and unlink files asynchronously.

Tests: https://tracker.ceph.com/issues/82472
"""
import logging
import re
from io import StringIO

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.kernel_mount import KernelMount

log = logging.getLogger(__name__)


class TestAsyncDirops(CephFSTestCase):
    CLIENTS_REQUIRED = 2

    def setUp(self):
        super().setUp()
        if not isinstance(self.mount_a, KernelMount):
            self.skipTest("async dirops need the kernel client")
        # the lock cache lines read below are logged at 10 and 20
        self.config_set('mds', 'debug_mds', '20')
        self.addCleanup(self.config_rm, 'mds', 'debug_mds')
        self.mount_b.umount_wait()
        self.mount_a.umount_wait()

    @staticmethod
    def _file_caps(caps):
        m = re.search(r'F([a-z]*)', caps)
        return set(m.group(1)) if m else set()

    def _caps(self, ino):
        info = self.fs.mds_asok(['dump', 'inode', hex(ino)])
        caps = [(c['issued'], c['wanted']) for c in info['client_caps']]
        log.info(f"caps on {hex(ino)}: {caps}")
        return caps

    def _mds_log(self):
        name = self.fs.get_rank(rank=0)['name']
        log_file = self.fs.rank_tell(['config', 'get', 'log_file'],
                                     rank=0)['log_file']
        return self.fs.mon_manager.find_remote('mds', name), log_file

    def _log_mark(self):
        """
        The current end of rank 0's log, so that later counts only see what
        this test logs: every test recreates the file system, so the first
        directory a test makes has the same inode number each time.
        """
        self.fs.rank_tell(['log', 'flush'], rank=0)
        remote, log_file = self._mds_log()
        return int(remote.sh(['stat', '-c', '%s', log_file]).strip())

    def _log_count(self, mark, pattern):
        """Lines of rank 0's log after `mark` that match the ERE `pattern`."""
        self.fs.rank_tell(['log', 'flush'], rank=0)
        remote, log_file = self._mds_log()
        p = remote.run(args=['sh', '-c', f"tail -c +{mark + 1} {log_file} | grep -c -E '{pattern}'"],
                       stdout=StringIO(), check_status=False)
        self.assertIn(p.exitstatus, (0, 1), f"grep failed on {log_file}")
        return int(p.stdout.getvalue().strip() or 0)

    def _lock_cache_counts(self, mark, ino):
        """
        For the directory `ino`: how many creates reached create_lock_cache()
        (a control: zero would mean the other counts prove nothing), and how
        many lock caches were built or reused.
        """
        tried = self._log_count(mark, rf"create_lock_cache: for client\.[0-9]+/create on \[inode {hex(ino)} ")
        built = self._log_count(mark, rf"create_lock_cache: created MDLockCache\(o=[a-z]+ diri={hex(ino)} ")
        found = self._log_count(mark, rf"found lock cache .* on \[inode {hex(ino)} ")
        log.info(f"create_lock_cache on {hex(ino)}: tried {tried}, built {built}, reused {found}")
        self.assertGreater(tried, 0, "no create reached create_lock_cache()")
        return built, found

    def _request_counts(self, mark, op, prefix):
        """
        Of the `op` requests in rank 0's log after `mark` for files named
        `prefix` and a number, how many the client sent async, and how many
        there were in all.
        """
        # one line per request: the dmclock scheduler logs it a second time
        req = rf"\.server handle_client_request client_request\(client\.[0-9]+:[0-9]+ {op} [^#]*#[^ ]+/{prefix}[0-9]+ "
        total = self._log_count(mark, req)
        sent_async = self._log_count(mark, req + ".* ASYNC caller_uid=")
        log.info(f"{op} of {prefix}*: {sent_async} async of {total}")
        return sent_async, total

    def test_lone_client(self):
        """
        That a nowsync client alone in a directory gets Fx with Dc, then Fx
        with Du, and that it then sends its creates and unlinks there as
        async requests.
        """
        self.mount_a.mount_wait(mntopts=["nowsync"])
        self.mount_a.run_shell(["mkdir", "d"])
        ino = self.mount_a.path_to_ino("d")

        def issued():
            caps = self._caps(ino)
            return self._file_caps(caps[0][0]) if caps else set()

        # The client asks for Dc (Fc) and Du (Fr) once it has used the
        # directory, so the first operations of each kind may still wait.
        n = iter(range(1000))
        def create():
            self.mount_a.run_shell(["touch", f"d/f{next(n)}"])
            return {"x", "c"} <= issued()
        self.wait_until_true(create, timeout=60)

        mark = self._log_mark()
        self.mount_a.run_shell_payload("for i in $(seq 30); do touch d/g$i; done; "
                                       "for i in $(seq 20); do touch d/u$i; done")
        rest = [f"d/g{i}" for i in range(1, 31)]
        def unlink():
            self.mount_a.run_shell(["rm", rest.pop()])
            return {"x", "r"} <= issued()
        self.wait_until_true(unlink, timeout=60)

        # The client unlinks a file async only if it knows that the dentry is
        # the primary link, which it learns from the MDS's dentry leases.
        self.mount_a.run_shell_payload("for i in $(seq 20); do rm d/u$i; done")
        for op, cap in (("create", "Dc"), ("unlink", "Du")):
            sent_async, total = self._request_counts(mark, op, "u")
            self.assertEqual(total, 20, f"not every {op} of u* is in the MDS log")
            self.assertGreater(sent_async, 0, f"no {op} was sent async while the client held Fx and {cap}")

    def test_shared_dir(self):
        """
        That the MDS builds no lock cache for two nowsync clients that want
        Dc and Du on a directory they share: neither can hold Fx there, so
        neither could be given Dc or Du.
        """
        self.mount_a.mount_wait(mntopts=["nowsync"])
        self.mount_b.mount_wait(mntopts=["nowsync"])
        self.mount_a.run_shell(["mkdir", "d"])
        # take a cap on d from the second client before the first one works there
        self.mount_b.stat("d")
        ino = self.mount_a.path_to_ino("d")
        mark = self._log_mark()

        wanted = set()
        for i in range(30):
            self.mount_a.run_shell(["touch", f"d/a{i}"])
            self.mount_b.run_shell(["touch", f"d/b{i}"])
            self.mount_a.run_shell(["rm", f"d/b{i}"])
            self.mount_b.run_shell(["rm", f"d/a{i}"])
            caps = self._caps(ino)
            self.assertEqual(len(caps), 2)
            for issued, want in caps:
                self.assertFalse(self._file_caps(issued) & {"x", "c", "r"},
                                 f"a client got Fx, Dc or Du on a shared directory: {issued}")
                wanted |= self._file_caps(want)

        # a client that never wants Dc or Du never gets a lock cache tried
        self.assertTrue(wanted & {"c", "r"}, "neither client wanted Dc or Du")
        built, found = self._lock_cache_counts(mark, ino)
        self.assertEqual((built, found), (0, 0), "the MDS built a lock cache on a shared directory")

    def test_wsync_lone_client(self):
        """
        That the MDS builds no lock cache for a client that holds Fx on a
        directory but doesn't want Dc or Du there: one mounted with wsync.
        """
        self.mount_a.mount_wait(mntopts=["wsync"])
        self.mount_a.run_shell(["mkdir", "d"])
        ino = self.mount_a.path_to_ino("d")
        mark = self._log_mark()

        held_fx = False
        for i in range(30):
            self.mount_a.run_shell(["touch", f"d/f{i}"])
            caps = self._caps(ino)
            self.assertEqual(len(caps), 1)
            issued = self._file_caps(caps[0][0])
            self.assertFalse(issued & {"c", "r"}, f"a wsync client got Dc or Du: {caps[0][0]}")
            held_fx |= "x" in issued
        self.assertTrue(held_fx, "the client never held Fx, so the wanted check wasn't reached")
        built, found = self._lock_cache_counts(mark, ino)
        self.assertEqual((built, found), (0, 0), "the MDS built a lock cache for a wsync client")
