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
    CLIENTS_REQUIRED = 1

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

    def _open_rdwr(self, name):
        """
        Open <name> O_RDWR in the background and keep it open.  The
        process creates <name>.ready once the file is open, writes 4 KiB
        once <name>.go exists, then creates <name>.written.
        """
        path = os.path.join(self.mount_a.hostfs_mntpt, name)
        pyscript = dedent("""
            import fcntl, os, sys, time
            fcntl.fcntl(sys.stdin, fcntl.F_SETFL,
                        fcntl.fcntl(sys.stdin, fcntl.F_GETFL) | os.O_NONBLOCK)
            fd = os.open("{path}", os.O_RDWR)
            open("{path}.ready", "w").close()
            wrote = False
            while True:
                if not wrote and os.path.exists("{path}.go"):
                    os.pwrite(fd, b"x" * 4096, 0)
                    open("{path}.written", "w").close()
                    wrote = True
                try:
                    if os.read(0, 4096) == b"":
                        break
                except BlockingIOError:
                    pass
                time.sleep(0.5)
            os.close(fd)
            """).format(path=path)
        p = self.mount_a._run_python(pyscript)
        self.mount_a.background_procs.append(p)
        self.wait_until_true(
            lambda: self.mount_a.run_shell(['ls', name + '.ready'],
                                           check_status=False).exitstatus == 0,
            timeout=30)
        return p

    def _go(self, name):
        self.mount_a.run_shell(['touch', name + '.go'])
        self.wait_until_true(
            lambda: self.mount_a.run_shell(['ls', name + '.written'],
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
