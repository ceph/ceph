"""
Readers must not get Fr while another client still holds Fb with unflushed data.
"""

import hashlib
import logging
import time
from textwrap import dedent

from teuthology.orchestra import run

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.fuse_mount import FuseMount

log = logging.getLogger(__name__)

SIZE = 64 * 1024 * 1024


def pattern(tag):
    unit = ("filelock-mix %s\n" % tag).encode()
    return (unit * (SIZE // len(unit) + 1))[:SIZE]


class TestFilelockMix(CephFSTestCase):
    CLIENTS_REQUIRED = 3
    MDSS_REQUIRED = 1

    # Buffered write, rename into place, then keep the file open for writing until stdin closes.
    HOLDER = dedent("""
        import os, sys
        path, size, tag = sys.argv[1], int(sys.argv[2]), sys.argv[3]
        unit = ("filelock-mix %s\\n" % tag).encode()
        tmp = path + ".tmp"
        fd = os.open(tmp, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o644)
        os.write(fd, (unit * (size // len(unit) + 1))[:size])
        os.rename(tmp, path)
        open(path + ".ready", "w").close()
        sys.stdin.read()
        os.close(fd)
        """)

    READER = dedent("""
        import hashlib, sys
        with open(sys.argv[1], "rb") as f:
            data = f.read()
        zero = bytes(4096)
        nz = sum(1 for i in range(0, len(data), 4096) if data[i:i + 4096] == zero)
        print(hashlib.sha256(data).hexdigest(), len(data), nz)
        """)

    def _slow_flush(self, mount):
        """Make the writer's flush much slower than a reader's read of the same bytes."""
        mount.umount_wait()
        if isinstance(mount, FuseMount):
            section = "client.{0}".format(mount.client_id)
            self.config_set(section, "objecter_inflight_op_bytes", 64 * 1024)
            self.addCleanup(self.config_rm, section, "objecter_inflight_op_bytes")
            mount.mount_wait()
        else:
            mount.mount_wait(mntopts=["wsize=4096"])

    def test_setattr_then_read_sees_written_data(self):
        """
        A client buffers a large write to a file, renames it into place and keeps it open, so it still wants Fw
        and holds Fb with unflushed data. Another client updates the file's timestamps; the setattr xlocks the
        file lock, which passes through LOCK, where the writer keeps Fc and Fb. While the xlock is still held
        (the MDS journal is paused), a reader's open asks for Fr. When the xlock ends one client wants Fw and
        another Fr, so the file lock goes to MIX. LOCK used to go straight to MIX, granting Fr while the writer
        was still flushing, so readers read objects that weren't written yet and got zeros.
        """
        writer, toucher, late = self.mount_a, self.mount_b, self.mount_c
        self._slow_flush(writer)
        tag = str(time.time())
        expected = hashlib.sha256(pattern(tag)).hexdigest()
        name = "mixfile"

        holder = writer.run_shell(["python3", "-c", self.HOLDER, name, str(SIZE), tag],
                                  wait=False, stdin=run.PIPE, timeout=None)
        self.wait_until_true(
            lambda: writer.run_shell(["test", "-e", name + ".ready"], check_status=False).exitstatus == 0,
            timeout=120, period=1)

        try:
            # Journal running: the writer flushes its dirty metadata but keeps Fb and the data.
            toucher.run_shell(["touch", "-c", name])
            time.sleep(1)

            self.fs.rank_asok(["config", "set", "mds_log_pause", "true"])
            try:
                # Returns on the unsafe reply; the xlock stays held until the journal commits.
                toucher.run_shell(["touch", "-c", name], timeout=30)
                rb = toucher.run_shell(["python3", "-c", self.READER, name], wait=False)
                time.sleep(1)
            finally:
                self.fs.rank_asok(["config", "set", "mds_log_pause", "false"])
            time.sleep(0.1)
            rc = late.run_shell(["python3", "-c", self.READER, name], wait=False)

            for who, p in (("toucher", rb), ("late reader", rc)):
                p.wait()
                got, length, zero_blocks = p.stdout.getvalue().split()
                log.info("%s read %s bytes, %s all-zero 4 KiB blocks", who, length, zero_blocks)
                self.assertEqual(int(zero_blocks), 0, "%s read unflushed data as zeros" % who)
                self.assertEqual(got, expected, "%s read the wrong data" % who)
        finally:
            holder.stdin.close()
            holder.wait()
