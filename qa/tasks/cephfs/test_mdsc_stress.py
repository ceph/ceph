"""
Stress the CephFS MDS request dispatch paths with a metadata load
generator and concurrent MDS failovers (double-failover chaos).

The load generator is qa/workunits/fs/mdsc_stress/mdsc_stress.c: a
POSIX-only program that needs nothing but a mounted cephfs.  It runs a
weighted metadata operation mix (write-heavy, fsync-light so that
plenty of unsafe requests are replayed on reconnect), publishes every
in-flight operation in a per-worker slot so a watchdog thread can
report operations that never come back (the userspace symptom of a
lost wakeup), and repeatedly SIGKILLs and restarts "victim" processes
so that requests are aborted while the MDS dispatch paths drain their
wait lists.  Each mount type is supported: kernel client and ceph-fuse
(ceph.dir.pin, victims and watchdog all work on both).

The chaos pattern is two distinct ranks failed back-to-back, so the
second session replacement lands while the first is still being
processed and the client's request collectors overlap, plus an
occasional small batch of ceph.dir.pin re-rolls so requests get
forwarded between ranks.  A broken kernel-side request collection
shows up as list_add/list_del corruption or an endless collector
loop.

Verdict per test:
  - the load generator's exit code: 1 = an operation was still in
    flight when the run ended (suspected lost wakeup), 2 = unexpected
    errno, 3 = setup error
  - "HUNG:" lines in the load log
  - on kernel client jobs additionally: kernel splats in the client's
    dmesg during the window (list corruption, KASAN/UAF, lockdep
    reports, hung task warnings, ...)

errnos are classified by the load generator itself: races between
workers (ENOENT/EEXIST/...) are benign, session teardown errors during
chaos (EIO/ESTALE/ETIMEDOUT/...) are expected, anything else fails the
test.
"""
import logging
import os
import re
import time
from io import StringIO
from random import randint, sample

from teuthology.exceptions import CommandFailedError

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.kernel_mount import KernelMount

log = logging.getLogger(__name__)

SPLAT_RE = re.compile(
    r"BUG:|KASAN|UBSAN|KCSAN|WARNING:|general protection fault|"
    r"kernel NULL pointer|Oops|list_del corruption|list_add corruption|"
    r"list_add double add|refcount_t:|blocked for more than|"
    r"possible circular locking|possible recursive locking|"
    r"trying to register non-static key|held lock freed|"
    r"suspicious RCU usage|soft lockup|detected stalls|"
    r"slab-out-of-bounds|use-after-free|double free|Kernel panic|"
    r"Objects remaining")


class TestMdscStress(CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 3

    # test tree inside the mountpoint; the load generator creates one
    # subdirectory per worker (w00..) and one per victim (v00..) below it
    TEST_DIR = "mdsc_stress"
    LOAD_SRC = "/tmp/mdsc_stress.c"
    LOAD_BIN = "/tmp/mdsc_stress"

    THREADS = 8
    VICTIMS = 4
    WATCHDOG_SECS = 120  # report operations in flight longer than this
    REPORT_SECS = 30

    LOAD_ONLY_SECS = 90  # smoke test: load without chaos
    CHAOS_SECS = 240  # double-failover chaos duration
    SETTLE_SECS = 60  # grace after the cluster is healthy again

    def setUp(self):
        super().setUp()
        self._load_pid = None

        # the load generator is a plain C file in the checked-out tree:
        # copy it onto the client and compile it there
        src = os.path.normpath(os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "..",
            "workunits", "fs", "mdsc_stress", "mdsc_stress.c"))
        with open(src) as f:
            code = f.read()
        self.mount_a.client_remote.write_file(self.LOAD_SRC, code)
        self.mount_a.client_remote.run(
            args=["cc", "-O2", "-g", "-pthread", "-Wall", "-Wextra",
                  "-Wno-format-truncation", "-o", self.LOAD_BIN,
                  self.LOAD_SRC],
            omit_sudo=True)

    def tearDown(self):
        # make sure no load generator (or victim) survives the test
        self._signal_load("KILL")
        self._load_pid = None
        # a victim that was killed mid-operation may still hold the
        # directory for a moment, so retry once before giving up
        for attempt in range(2):
            try:
                self.mount_a.run_shell(
                    ["rm", "-rf", "--", self.TEST_DIR, "mdsc_stress.log",
                     "mdsc_stress.rc", "mdsc_stress.pid"],
                    timeout=300)
                break
            except CommandFailedError:
                if attempt == 0:
                    time.sleep(5)
                else:
                    log.warning("could not clean up %s on %s", self.TEST_DIR,
                                self.mount_a.client_remote.hostname)
        super().tearDown()

    # ------------------------------------------------------------------
    # load generator control
    # ------------------------------------------------------------------

    def _start_load(self, seconds=0, kill_range="200:2000"):
        """
        Start the load generator in the background on the client.

        seconds=0 means "run until signalled"; the wrapper records the
        exit code in mdsc_stress.rc.  The load generator runs under
        setsid as its own session and process-group leader, so the pid
        file names the binary itself (its worker threads and victim
        children live in the same group).  Signalling the wrapper
        subshell instead would stop the rc writer and orphan the load.
        """
        self.mount_a.run_shell(["mkdir", "-p", "--", self.TEST_DIR])
        # drop leftovers from an aborted run, or the poll below may
        # pick up a stale pid from a dead wrapper
        self.mount_a.run_shell(
            ["rm", "-f", "--", "mdsc_stress.pid", "mdsc_stress.rc"])
        cmd = (f"( setsid {self.LOAD_BIN} -d {self.TEST_DIR} -t {self.THREADS} "
               f"-k {self.VICTIMS} -K {kill_range} -s {seconds} "
               f"-w {self.WATCHDOG_SECS} -i {self.REPORT_SECS} -C & "
               f"child=$!; echo $child > mdsc_stress.pid; "
               f"wait $child; echo $? > mdsc_stress.rc "
               f") > mdsc_stress.log 2>&1 &")
        self.mount_a.run_shell(["bash", "-c", cmd])
        # the pid file is written by the wrapper for its setsid child,
        # i.e. the load generator itself (setsid execs, pid unchanged)
        for _ in range(30):
            try:
                pid = self.mount_a.run_shell(
                    ["cat", "mdsc_stress.pid"],
                    timeout=30).stdout.getvalue().strip()
                self._load_pid = int(pid)
                break
            except (CommandFailedError, ValueError):
                time.sleep(0.5)
        else:
            raise RuntimeError(
                "load generator did not start:\n" + self._load_log())
        log.info("load generator running on %s (pid %d, threads=%d, "
                 "victims=%d)",
                 self.mount_a.client_remote.hostname, self._load_pid,
                 self.THREADS, self.VICTIMS)
        # let the worker threads and victims come up before the first
        # chaos hit
        time.sleep(10)
        if not self._load_running():
            self._load_pid = None
            raise RuntimeError(
                "load generator exited during startup:\n" + self._load_log())

    def _signal_load(self, sig):
        if self._load_pid is None:
            return
        # TERM/KILL go to the process group (the load generator is the
        # leader, its victim children are in the same group); SIGUSR1
        # is for the load generator only (in-flight dump).
        target = (f"-- -{self._load_pid}" if sig in ("TERM", "KILL")
                  else str(self._load_pid))
        self.mount_a.run_shell(
            ["bash", "-c", f"kill -{sig} {target} || true"],
            timeout=30)

    def _load_running(self):
        try:
            self.mount_a.run_shell(
                ["bash", "-c", f"kill -0 {self._load_pid}"], timeout=30)
            return True
        except CommandFailedError:
            return False

    def _wait_load_exit(self, timeout):
        self.wait_until_true(lambda: not self._load_running(),
                             timeout=timeout)

    def _stop_load(self):
        """
        SIGTERM the load and wait for the summary; escalate to KILL.
        Raises RuntimeError if even SIGKILL does not end it - a process
        that survives SIGKILL is stuck in an uninterruptible kernel
        wait, which is exactly the hang this test exists to find.
        """
        if self._load_pid is None:
            return
        self._signal_load("TERM")
        try:
            self._wait_load_exit(120)
        except RuntimeError:
            log.warning("load generator did not exit after SIGTERM; "
                        "sending SIGKILL")
            self._signal_load("KILL")
            try:
                self._wait_load_exit(60)
            except RuntimeError:
                pid = self._load_pid
                # leave _load_pid set so tearDown makes one last attempt
                raise RuntimeError(
                    f"load generator (pid {pid}) survived SIGKILL: stuck "
                    "in an uninterruptible kernel wait") from None
        self._load_pid = None

    def _load_rc(self):
        """Read the load generator's exit code (written by the wrapper)."""
        def written():
            try:
                self.mount_a.run_shell(
                    ["test", "-s", "mdsc_stress.rc"], timeout=30)
                return True
            except CommandFailedError:
                return False

        try:
            self.wait_until_true(written, timeout=120)
        except RuntimeError:
            return None
        return int(self.mount_a.run_shell(
            ["cat", "mdsc_stress.rc"]).stdout.getvalue().strip())

    def _load_log(self):
        try:
            return self.mount_a.run_shell(
                ["cat", "mdsc_stress.log"], timeout=300).stdout.getvalue()
        except CommandFailedError:
            return ""

    def _load_verdict(self, rc):
        """Turn the load outcome into a list of failure strings."""
        problems = []
        txt = self._load_log()

        # keep the interesting lines in the teuthology log
        for line in txt.splitlines():
            if re.search(r"^(RESULT|REASON|SLOW|HUNG|INFLIGHT|"
                         r"UNEXPECTED|WARNING)", line):
                log.info("load: %s", line)

        if rc is None:
            problems.append("load generator produced no exit code")
        elif rc == 1:
            problems.append("load exit 1: worker(s) stuck in an MDS "
                            "request (suspected lost wakeup)")
        elif rc == 2:
            problems.append("load exit 2: unexpected errno")
        elif rc == 3:
            problems.append("load exit 3: usage/setup error")
        elif rc != 0:
            problems.append(f"load exited with {rc} (crash/signal?)")

        if "HUNG:" in txt:
            problems.append("HUNG operations in the load log")

        return problems

    # ------------------------------------------------------------------
    # kernel-side oracle (kernel client jobs only)
    # ------------------------------------------------------------------

    def _is_kernel_mount(self):
        return isinstance(self.mount_a, KernelMount)

    def _dmesg_baseline(self):
        out = self.mount_a.client_remote.run(
            args=["sudo", "bash", "-c", "dmesg | wc -l"],
            stdout=StringIO(), omit_sudo=True).stdout.getvalue()
        return int(out.strip())

    def _dmesg_splats(self, baseline):
        out = self.mount_a.client_remote.run(
            args=["sudo", "bash", "-c", f"dmesg | tail -n +{baseline + 1}"],
            stdout=StringIO(), omit_sudo=True).stdout.getvalue()
        return [line for line in out.splitlines() if SPLAT_RE.search(line)]

    # ------------------------------------------------------------------
    # chaos: two ranks failed back-to-back
    # ------------------------------------------------------------------

    def _grow_to_two_ranks(self):
        self.fs.set_max_mds(2)
        self.fs.wait_for_daemons()
        ranks = [r["rank"] for r in self.fs.get_ranks()]
        if len(ranks) != 2:
            raise RuntimeError(f"expected 2 active ranks, got {ranks}")
        return ranks

    def _repin(self):
        """
        Re-pin a small random batch of worker dirs: cross-rank
        forwards need auth movement, but re-pinning everything every
        cycle is a migration storm that wedges the migrator under
        load.
        """
        for _ in range(min(8, self.THREADS)):
            d = f"{self.TEST_DIR}/w{randint(0, self.THREADS - 1):02d}"
            try:
                self.mount_a.setfattr(d, "ceph.dir.pin", str(randint(0, 1)))
            except CommandFailedError:
                pass
        time.sleep(randint(45, 90))

    def _chaos_double_failover(self, seconds):
        """
        Fail two distinct ranks back-to-back for the whole duration, so
        the second session replacement lands while the first is still
        being processed.  Wait for a healthy cluster between cycles so
        the next pair of failures does not interrupt an ongoing
        recovery (which wedges subtree migration), and re-pin a small
        batch of dirs every few cycles.
        """
        ranks = self._grow_to_two_ranks()
        start = time.time()
        cycles = 0
        while time.time() - start < seconds:
            a, b = sample(ranks, 2)
            self.fs.rank_fail(rank=a)
            time.sleep(randint(1, 3))
            self.fs.rank_fail(rank=b)
            self.fs.wait_for_daemons(timeout=300)
            if cycles % 3 == 0:
                self._repin()
            cycles += 1
            time.sleep(randint(3, 6))
        log.info("chaos: %d double-failover cycles", cycles)

    def _settle_and_dump(self):
        """
        Let the cluster settle while the load keeps running, then ask
        the load which operations are still in flight.  Anything still
        blocked once the cluster is healthy again is a lost wakeup.
        """
        time.sleep(self.SETTLE_SECS // 2)
        self._signal_load("USR1")
        time.sleep(self.SETTLE_SECS // 2)
        self._signal_load("USR1")
        time.sleep(5)

    # ------------------------------------------------------------------
    # the tests
    # ------------------------------------------------------------------

    def test_load_only(self):
        """
        Smoke test: metadata load without cluster chaos must finish on
        its own, with no HUNG operation and no unexpected errno.
        """
        self._start_load(seconds=self.LOAD_ONLY_SECS,
                         kill_range="1000:2000")
        try:
            self._wait_load_exit(self.LOAD_ONLY_SECS + 240)
        except RuntimeError:
            self._signal_load("KILL")
        problems = self._load_verdict(self._load_rc())
        self.assertFalse(problems, "; ".join(problems))

    def test_double_failover(self):
        """
        Back-to-back MDS failovers while metadata load is in flight,
        so overlapping session replacements collect the same requests
        concurrently, plus occasional pin re-rolls for cross-rank
        forwards.
        """
        dmesg_base = self._dmesg_baseline() if self._is_kernel_mount() \
            else None

        self._start_load(seconds=0)
        self._chaos_double_failover(self.CHAOS_SECS)
        self._settle_and_dump()
        try:
            self._stop_load()
            problems = self._load_verdict(self._load_rc())
        except RuntimeError as e:
            # unkillable load: report it together with everything else
            # that went wrong instead of losing the verdicts below
            problems = [str(e)]
            problems += self._load_verdict(self._load_rc())

        if dmesg_base is not None:
            splats = self._dmesg_splats(dmesg_base)
            for line in splats:
                log.error("kernel splat on client: %s", line)
            if splats:
                problems.append(
                    f"{len(splats)} kernel splat(s) on the client "
                    "(see teuthology.log)")

        self.assertFalse(problems, "; ".join(problems))
