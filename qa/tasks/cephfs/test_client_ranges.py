"""
Writeable ranges (client_ranges) handed out on open, with and without
mds_defer_open_client_range.
"""

import logging
import os
from textwrap import dedent

from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)


class TestDeferOpenClientRange(CephFSTestCase):
    MDSS_REQUIRED = 2
    CLIENTS_REQUIRED = 2

    def setUp(self):
        super().setUp()
        self.addCleanup(self.config_rm, 'mds', 'mds_defer_open_client_range')

    def _set_defer(self, on):
        self.config_set('mds', 'mds_defer_open_client_range', on)
        self._wait_defer(on)

    def _wait_defer(self, on):
        # the active MDS takes the value from the config database
        want = 'true' if on else 'false'
        self.wait_until_true(
            lambda: self.fs.rank_asok(
                ['config', 'get', 'mds_defer_open_client_range']
            )['mds_defer_open_client_range'] == want,
            timeout=30)

    def _ranges(self, ino):
        inode = self.fs.rank_asok(['dump', 'inode', str(ino)])
        return [r['client'] for r in inode['client_ranges']]

    def _open_rdwr(self, name, mount=None, tag=None):
        """
        Open <name> O_RDWR on <mount> (default mount_a) in the background
        and keep it open.  The process creates <tag>.ready once the file
        is open, writes 4 KiB once <tag>.go exists, then creates
        <tag>.written.  <tag> defaults to <name>.
        """
        mount = mount or self.mount_a
        tag = tag or name
        path = os.path.join(mount.hostfs_mntpt, name)
        mark = os.path.join(mount.hostfs_mntpt, tag)
        pyscript = dedent("""
            import fcntl, os, sys, time
            fcntl.fcntl(sys.stdin, fcntl.F_SETFL,
                        fcntl.fcntl(sys.stdin, fcntl.F_GETFL) | os.O_NONBLOCK)
            fd = os.open("{path}", os.O_RDWR)
            open("{mark}.ready", "w").close()
            wrote = False
            while True:
                if not wrote and os.path.exists("{mark}.go"):
                    os.pwrite(fd, b"x" * 4096, 0)
                    open("{mark}.written", "w").close()
                    wrote = True
                try:
                    if os.read(0, 4096) == b"":
                        break
                except BlockingIOError:
                    pass
                time.sleep(0.5)
            os.close(fd)
            """).format(path=path, mark=mark)
        p = mount._run_python(pyscript)
        mount.background_procs.append(p)
        self.wait_until_true(
            lambda: mount.run_shell(['ls', tag + '.ready'],
                                    check_status=False).exitstatus == 0,
            timeout=30)
        return p

    def _go(self, tag, mount=None):
        mount = mount or self.mount_a
        mount.run_shell(['touch', tag + '.go'])
        self.wait_until_true(
            lambda: mount.run_shell(['ls', tag + '.written'],
                                    check_status=False).exitstatus == 0,
            timeout=60)

    def _new_file(self, name):
        # an existing, closed file with no writeable range left
        self.mount_a.run_shell(['sh', '-c', f'echo data > {name}'])
        self.mount_a.umount_wait()
        self.mount_a.mount_wait()
        ino = self.mount_a.path_to_ino(name)
        self.assertEqual(self._ranges(ino), [])
        return ino

    def test_open_rdwr_gives_range(self):
        """
        Without the option an O_RDWR open gets a writeable range at once.
        """
        self._set_defer(False)
        ino = self._new_file('f')
        self._open_rdwr('f')
        self.assertEqual(self._ranges(ino), [self.mount_a.get_global_id()])

    def test_defer_open_rdwr_no_range(self):
        """
        With the option an O_RDWR open that does not write gets no
        writeable range; the first write asks for one and gets it.
        """
        self._set_defer(True)
        ino = self._new_file('f')
        self._open_rdwr('f')
        self.assertEqual(self._ranges(ino), [])
        self._go('f')
        self.assertEqual(self._ranges(ino), [self.mount_a.get_global_id()])
        self.mount_a.kill_background()
        self.assertEqual(self.mount_a.stat('f')['st_size'], 4096)

    def test_defer_open_rdwr_failover(self):
        """
        With the option, a client that opened O_RDWR without a range
        before an MDS failover still gets one when it first writes.
        """
        self._set_defer(True)
        ino = self._new_file('f')
        self._open_rdwr('f')
        self.assertEqual(self._ranges(ino), [])

        self.fs.rank_fail()
        self.fs.wait_for_daemons()
        self._wait_defer(True)
        self.assertEqual(self._ranges(ino), [])

        self._go('f')
        self.assertEqual(self._ranges(ino), [self.mount_a.get_global_id()])
        self.mount_a.kill_background()
        self.assertEqual(self.mount_a.stat('f')['st_size'], 4096)

    def test_defer_two_clients_only_writer_gets_range(self):
        """
        With the option, two clients that open the same file O_RDWR get no
        writeable range; when one of them writes, only that client gets a
        range, and the other gets one only when it writes too.
        """
        self._set_defer(True)
        ino = self._new_file('f')
        self._open_rdwr('f', self.mount_a, 'fa')
        self._open_rdwr('f', self.mount_b, 'fb')
        self.assertEqual(self._ranges(ino), [])

        self._go('fa', self.mount_a)
        self.assertEqual(self._ranges(ino), [self.mount_a.get_global_id()])

        self._go('fb', self.mount_b)
        self.assertEqual(sorted(self._ranges(ino)),
                         sorted([self.mount_a.get_global_id(),
                                 self.mount_b.get_global_id()]))
        self.mount_a.kill_background()
        self.mount_b.kill_background()
        self.assertEqual(self.mount_a.stat('f')['st_size'], 4096)
