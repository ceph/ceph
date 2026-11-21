"""
Lazy I/O with several clients sharing a file.

With lazy I/O (the lazyio mount option of the kernel client, or
client_force_lazyio for ceph-fuse) the MDS issues LAZYIO along with the
read and write caps, so that clients sharing a file for writing keep
buffering and caching it.  Each client may then hold stale bytes around
what it wrote: whatever it writes back must still not overwrite what the
other clients wrote, and what it caches must become visible once the
writer has synced and the reader drops its cache.
"""

import base64
import logging
import random
from textwrap import dedent

from teuthology.exceptions import CommandFailedError

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.fuse_mount import FuseMount

log = logging.getLogger(__name__)

PAGE = 4096
# CEPH_IOC_LAZYIO, _IO(0x97, 4)
CEPH_IOC_LAZYIO = 0x9704


def file_caps(caps):
    """The file caps ("scrwbl"...) in an MDS cap string like pAsLsXsFscrl"""
    i = caps.find('F')
    if i < 0:
        return ''
    j = i + 1
    while j < len(caps) and caps[j].islower():
        j += 1
    return caps[i + 1:j]


class TestLazyIO(CephFSTestCase):
    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 2

    def setUp(self):
        super().setUp()
        self.fuse = isinstance(self.mount_a, FuseMount)
        for m in (self.mount_a, self.mount_b):
            m.umount_wait()
        try:
            for m in (self.mount_a, self.mount_b):
                self._mount(m)
        except CommandFailedError:
            if not self.fuse:
                self.skipTest("kernel client without the lazyio mount option")
            raise
        self.mount_a.run_shell(["mkdir", "lazyio"])

    def tearDown(self):
        if self.fuse:
            for m in (self.mount_a, self.mount_b):
                self.config_rm(f'client.{m.client_id}', 'client_force_lazyio')
        super().tearDown()

    # -- helpers --

    def _mount(self, m, lazy=True):
        if self.fuse:
            self.config_set(f'client.{m.client_id}', 'client_force_lazyio',
                            lazy)
            m.mount_wait()
        else:
            m.mount_wait(mntopts=['lazyio'] if lazy else [])

    def _remount(self, m, lazy=True):
        m.umount_wait()
        self._mount(m, lazy)

    def _remount_all(self):
        # drop what the clients cache, to read back what the OSDs hold
        for m in (self.mount_a, self.mount_b):
            self._remount(m)

    def _prelude(self, mount):
        """
        Python run on a client: signal()/wait_for() hand over between the
        two clients through flag files in the shared directory.
        """
        return dedent(f"""
            import ctypes, fcntl, mmap, os, time

            D = "{mount.mountpoint}/lazyio"

            def path(name):
                return os.path.join(D, name)

            def lazy_open(name, flags=os.O_RDWR | os.O_CREAT):
                return os.open(path(name), flags, 0o644)

            def signal(name):
                os.close(os.open(path(name), os.O_WRONLY | os.O_CREAT, 0o644))

            def wait_for(name, timeout=120):
                for _ in range(timeout * 10):
                    if name in os.listdir(D):
                        return
                    time.sleep(0.1)
                raise RuntimeError("timed out waiting for " + name)
            """)

    def _start(self, mount, script):
        return mount._run_python(self._prelude(mount) + dedent(script))

    def _run(self, mount, script):
        return mount.run_python(self._prelude(mount) + dedent(script))

    def _run_pair(self, script_a, script_b):
        """Run script_a on mount_a and script_b on mount_b, side by side."""
        a = self._start(self.mount_a, script_a)
        try:
            self._run(self.mount_b, script_b)
        finally:
            a.wait()

    def _signal(self, name):
        self.mount_a.run_shell(["touch", f"lazyio/{name}"])

    def _wait_for(self, *names, timeout=120):
        self.wait_until_true(
            lambda: set(names) <= set(self.mount_a.ls("lazyio")),
            timeout=timeout, period=1)

    def _write(self, name, data):
        self._run(self.mount_a, f"""
            import base64
            fd = lazy_open("{name}", os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
            os.write(fd, base64.b64decode("{base64.b64encode(data).decode()}"))
            os.fsync(fd)
            os.close(fd)
            """)

    def _read(self, mount, name):
        out = mount.run_python(dedent(f"""
            import base64, sys
            with open("{mount.mountpoint}/lazyio/{name}", "rb") as f:
                sys.stdout.write(base64.b64encode(f.read()).decode())
            """))
        return base64.b64decode(out)

    def _check(self, name, expect, mount=None):
        data = self._read(mount or self.mount_a, name)
        if data == expect:
            return
        n = min(len(data), len(expect))
        bad = next((i for i in range(n) if data[i] != expect[i]), n)
        self.fail(f"{name}: {len(data)} bytes, want {len(expect)}; first "
                  f"difference at {bad}: {data[bad:bad + 8]!r}, "
                  f"want {expect[bad:bad + 8]!r}")

    def _caps(self, name):
        """
        The MDS's view of the file: {client id: (issued, wanted)}, the
        filelock state and the loner.
        """
        ino = self.mount_a.path_to_ino(f"lazyio/{name}")
        d = self.fs.rank_asok(['dump', 'inode', str(ino)])
        caps = {c['client_id']: (c['issued'], c['wanted'])
                for c in d['client_caps']}
        return caps, d['filelock'].get('state'), d.get('loner')

    def _wait_for_caps(self, name, cond, timeout=60):
        """Wait until cond(caps, state, loner) holds; return that view"""
        view = []

        def ok():
            view[:] = self._caps(name)
            return cond(*view)
        try:
            self.wait_until_true(ok, timeout=timeout, period=2)
        except RuntimeError:
            self.fail(f"{name}: caps never as expected: {view}")
        return view

    def _client_ids(self):
        return self.mount_a.get_global_id(), self.mount_b.get_global_id()

    # -- caps --

    def _both_open(self, name, check, ioctl=False):
        """Hold name open for writing on both clients while check() runs."""
        lazy = f"fcntl.ioctl(fd, {CEPH_IOC_LAZYIO})" if ioctl else ""
        a = self._start(self.mount_a, f"""
            fd = lazy_open("{name}")
            {lazy}
            os.pwrite(fd, b"a", 0)
            signal("a_open")
            wait_for("checked")
            os.close(fd)
            """)
        b = self._start(self.mount_b, f"""
            wait_for("a_open")
            fd = lazy_open("{name}")
            {lazy}
            os.pwrite(fd, b"b", 1)
            signal("b_open")
            wait_for("checked")
            os.close(fd)
            """)
        try:
            self._wait_for("b_open")
            return check()
        finally:
            self._signal("checked")
            a.wait()
            b.wait()

    def _both_get_lazyio(self, name):
        ida, idb = self._client_ids()
        return self._wait_for_caps(
            name, lambda caps, state, loner:
            state == "mix" and
            all('l' in file_caps(caps.get(i, ('', ''))[0])
                for i in (ida, idb)))

    def test_lazy_writers_get_lazyio(self):
        """
        Two clients with the file open for writing put the filelock in
        MIX, where BUFFER and CACHE are not allowed; both get LAZYIO.
        """
        self._write("f", b"x" * PAGE)
        self._both_open("f", lambda: self._both_get_lazyio("f"))

    def test_non_lazy_writers_get_no_lazyio(self):
        """Without lazy I/O, MIX leaves both clients without LAZYIO."""
        for m in (self.mount_a, self.mount_b):
            self._remount(m, lazy=False)
        ida, idb = self._client_ids()
        self._write("f", b"x" * PAGE)
        caps, state, loner = self._both_open("f", lambda: self._wait_for_caps(
            "f", lambda caps, state, loner:
            state == "mix" and ida in caps and idb in caps))
        for i in (ida, idb):
            self.assertFalse(set('bl') & set(file_caps(caps[i][0])), caps)

    def test_lazy_and_non_lazy_opener(self):
        """
        Only the lazy opener of a file in MIX gets LAZYIO, and their
        disjoint writes both reach the file.
        """
        self._remount(self.mount_b, lazy=False)
        ida, idb = self._client_ids()
        self._write("f", b"x" * PAGE)
        caps, state, loner = self._both_open("f", lambda: self._wait_for_caps(
            "f", lambda caps, state, loner:
            state == "mix" and idb in caps and
            'l' in file_caps(caps.get(ida, ('', ''))[0])))
        self.assertNotIn('l', file_caps(caps[idb][0]), caps)

        self._run_pair("""
            fd = lazy_open("g")
            signal("a_open")
            wait_for("b_open")
            os.pwrite(fd, b"A" * 3000, 0)
            signal("a_written")
            wait_for("b_written")
            os.fsync(fd)
            os.close(fd)
            """, """
            wait_for("a_open")
            fd = lazy_open("g")
            signal("b_open")
            wait_for("a_written")
            os.pwrite(fd, b"B" * 3000, 3000)
            os.fsync(fd)
            signal("b_written")
            os.close(fd)
            """)
        self._remount_all()
        self._check("g", b"A" * 3000 + b"B" * 3000)

    def test_lazyio_ioctl(self):
        """CEPH_IOC_LAZYIO turns lazy I/O on for one open file."""
        if self.fuse:
            self.skipTest("ceph-fuse has no CEPH_IOC_LAZYIO")
        for m in (self.mount_a, self.mount_b):
            self._remount(m, lazy=False)
        self._write("f", b"x" * PAGE)
        self._both_open("f", lambda: self._both_get_lazyio("f"), ioctl=True)

    def test_lazy_non_loner_keeps_lazyio_in_excl(self):
        """
        A lazy client that wants nothing but LAZYIO (e.g. it closed the
        file and keeps its cache) is no candidate for the loner, so another
        client that opens the file for writing becomes the loner and the
        filelock goes EXCL.  The lazy client keeps LAZYIO, and with it its
        cache.  https://tracker.ceph.com/issues/80231
        """
        ida, idb = self._client_ids()
        self._write("f", b"x" * 65536)
        self._run(self.mount_a, """
            fd = lazy_open("f", os.O_RDONLY)
            os.read(fd, 65536)
            os.close(fd)
            """)

        def lazyio_only(caps, state, loner):
            fw = file_caps(caps.get(ida, ('', '-'))[1])
            return 'l' in fw and not set('rw') & set(fw)
        try:
            self.wait_until_true(lambda: lazyio_only(*self._caps("f")),
                                 timeout=90, period=3)
        except RuntimeError:
            self.skipTest("the client does not keep wanting LAZYIO alone "
                          f"after closing the file: {self._caps('f')}")

        b = self._start(self.mount_b, """
            fd = lazy_open("f")
            os.pwrite(fd, b"b", 0)
            wait_for("checked")
            os.close(fd)
            """)
        try:
            caps, state, loner = self._wait_for_caps(
                "f", lambda caps, state, loner:
                state == "excl" and loner == idb)
            self.assertIn('l', file_caps(caps.get(ida, ('', ''))[0]), caps)
        finally:
            self._signal("checked")
            b.wait()

    # -- data written by several clients --

    def test_interleaved_unaligned_records(self):
        """
        Two clients write interleaved records that are not page multiples
        (io500 ior-hard's 47008 bytes) into one file, so most pages hold
        the end of one client's record and the start of the other's.  The
        second client to write back must not overwrite the first one's
        records with the stale bytes of its cache.
        """
        rec, nrec = 47008, 32
        writer = """
            for i in range({me}, {nrec}, 2):
                os.pwrite(fd, bytes([65 + i % 26]) * {rec}, i * {rec})
            """

        # Both clients open the file before either writes, as the
        # processes of an MPI job do.  a writes its records first, b then
        # writes its own around them with a's still in a's cache, and a
        # writes back before b does.
        self._run_pair(dedent("""
            fd = lazy_open("shared")
            signal("a_open")
            wait_for("b_open")
            """) + dedent(writer).format(me=0, nrec=nrec, rec=rec)
            + dedent("""
            signal("a_written")
            wait_for("b_written")
            os.fsync(fd)
            signal("a_synced")
            wait_for("b_synced")
            os.close(fd)
            """), dedent("""
            wait_for("a_open")
            fd = lazy_open("shared")
            signal("b_open")
            wait_for("a_written")
            """) + dedent(writer).format(me=1, nrec=nrec, rec=rec)
            + dedent("""
            signal("b_written")
            wait_for("a_synced")
            os.fsync(fd)
            signal("b_synced")
            os.close(fd)
            """))

        self._remount_all()
        expect = b"".join(bytes([65 + i % 26]) * rec for i in range(nrec))
        self._check("shared", expect)

    def test_same_page_disjoint_bytes(self):
        """
        Two clients write alternate 512-byte pieces of the same pages, so
        every page holds bytes of both.
        """
        piece, pages = 512, 4
        n = pages * PAGE // piece
        writer = """
            for i in range({me}, {n}, 2):
                os.pwrite(fd, bytes([65 + {me}]) * {piece}, i * {piece})
            """
        self._run_pair(dedent("""
            fd = lazy_open("f")
            signal("a_open")
            wait_for("b_open")
            """) + dedent(writer).format(me=0, n=n, piece=piece)
            + dedent("""
            signal("a_written")
            wait_for("b_written")
            os.fsync(fd)
            signal("a_synced")
            os.close(fd)
            """), dedent("""
            wait_for("a_open")
            fd = lazy_open("f")
            signal("b_open")
            wait_for("a_written")
            """) + dedent(writer).format(me=1, n=n, piece=piece)
            + dedent("""
            signal("b_written")
            wait_for("a_synced")
            os.fsync(fd)
            os.close(fd)
            """))
        self._remount_all()
        self._check("f", b"".join(bytes([65 + i % 2]) * piece
                                  for i in range(n)))

    def test_random_disjoint_records(self):
        """
        Two clients write disjoint records of random sizes, some across
        object boundaries, in random order and at the same time, and sync
        at the end.
        """
        rnd = random.Random(80231)
        obj = 4 << 20
        recs, off = [], obj - 300000
        while off < obj + 300000:
            size = rnd.randrange(1, 3 * PAGE)
            recs.append((off, size, rnd.randrange(2)))
            off += size
        mine = {0: [], 1: []}
        for o, s, me in recs:
            mine[me].append((o, s))
        for m in mine.values():
            rnd.shuffle(m)

        def writer(me):
            return dedent(f"""
                for off, size in {mine[me]}:
                    os.pwrite(fd, bytes([65 + {me} + off % 13 * 2]) * size,
                              off)
                """)
        self._run_pair(dedent("""
            fd = lazy_open("f")
            signal("a_open")
            wait_for("b_open")
            """) + writer(0) + dedent("""
            os.fsync(fd)
            signal("a_synced")
            wait_for("b_synced")
            os.close(fd)
            """), dedent("""
            wait_for("a_open")
            fd = lazy_open("f")
            signal("b_open")
            """) + writer(1) + dedent("""
            os.fsync(fd)
            signal("b_synced")
            wait_for("a_synced")
            os.close(fd)
            """))
        self._remount_all()
        expect = b"\0" * recs[0][0] + b"".join(
            bytes([65 + me + off % 13 * 2]) * size for off, size, me in recs)
        self._check("f", expect)

    def test_write_beyond_other_clients_eof(self):
        """
        b writes right after a's bytes in a's page while the file is still
        empty to b: b's page is zeros up to its write, which must not
        overwrite a's bytes.
        """
        self._run_pair("""
            fd = lazy_open("f")
            signal("a_open")
            wait_for("b_open")
            os.pwrite(fd, b"A" * 100, 0)
            signal("a_written")
            wait_for("b_written")
            os.fsync(fd)
            signal("a_synced")
            os.close(fd)
            """, """
            wait_for("a_open")
            fd = lazy_open("f")
            signal("b_open")
            wait_for("a_written")
            os.pwrite(fd, b"B" * 100, 100)
            signal("b_written")
            wait_for("a_synced")
            os.fsync(fd)
            os.close(fd)
            """)
        self._remount_all()
        self._check("f", b"A" * 100 + b"B" * 100)

    def test_two_writes_with_a_gap_in_one_page(self):
        """
        a writes twice into a page, with b's bytes in between: a must not
        write back b's bytes as they are in its cache.
        """
        self._write("f", b"P" * PAGE)
        self._run_pair("""
            fd = lazy_open("f")
            signal("a_open")
            wait_for("b_open")
            os.pread(fd, 4096, 0)
            os.pwrite(fd, b"A" * 100, 100)
            signal("a_written")
            wait_for("b_synced")
            os.pwrite(fd, b"A" * 100, 3000)
            os.fsync(fd)
            os.close(fd)
            """, """
            wait_for("a_open")
            fd = lazy_open("f")
            signal("b_open")
            wait_for("a_written")
            os.pwrite(fd, b"B" * 1000, 1000)
            os.fsync(fd)
            signal("b_synced")
            os.close(fd)
            """)
        self._remount_all()
        expect = bytearray(b"P" * PAGE)
        expect[100:200] = b"A" * 100
        expect[1000:2000] = b"B" * 1000
        expect[3000:3100] = b"A" * 100
        self._check("f", bytes(expect))

    def _truncate_below_dirty_range(self, b_truncates, extend):
        """
        With the file open on both clients, a caches a page, b rewrites it,
        a writes 32 bytes at the end of it with the rest of the page stale
        in its cache, and the file is truncated below those bytes.  a must
        not write back the stale bytes in front of them.
        """
        size, tail, cut = 4 * PAGE, PAGE + 3000, PAGE + 1000
        self._write("f", b"P" * size)
        self._run_pair(f"""
            fd = lazy_open("f")
            signal("a_open")
            wait_for("b_open")
            os.pread(fd, {size}, 0)
            signal("a_cached")
            wait_for("b_rewritten")
            os.pwrite(fd, b"A" * 32, {tail})
            signal("a_dirtied")
            if not {b_truncates}:
                os.ftruncate(fd, {cut})
                signal("truncated")
            wait_for("truncated")
            if {extend}:
                os.pwrite(fd, b"E" * 9, {cut} + 7)
            os.fsync(fd)
            os.close(fd)
            """, f"""
            wait_for("a_open")
            fd = lazy_open("f")
            signal("b_open")
            wait_for("a_cached")
            os.pwrite(fd, b"Q" * {size}, 0)
            os.fsync(fd)
            signal("b_rewritten")
            wait_for("a_dirtied")
            if {b_truncates}:
                os.ftruncate(fd, {cut})
                signal("truncated")
            os.close(fd)
            """)
        self._remount_all()
        expect = b"Q" * cut
        if extend:
            expect += b"\0" * 7 + b"E" * 9
        self._check("f", expect)

    def test_truncate_below_dirty_range(self):
        self._truncate_below_dirty_range(b_truncates=False, extend=False)

    def test_truncate_below_dirty_range_extend(self):
        self._truncate_below_dirty_range(b_truncates=False, extend=True)

    def test_other_client_truncates_below_dirty_range(self):
        self._truncate_below_dirty_range(b_truncates=True, extend=False)

    def test_other_client_truncates_below_dirty_range_extend(self):
        self._truncate_below_dirty_range(b_truncates=True, extend=True)

    def _stale_page_then(self, b_op):
        """
        a caches a page of P, b rewrites it to Q, a writes 32 bytes at its
        offset 3000, then b runs b_op on the page and a syncs.
        """
        self._write("f", b"P" * PAGE)
        self._run_pair("""
            fd = lazy_open("f")
            signal("a_open")
            wait_for("b_open")
            os.pread(fd, 4096, 0)
            signal("a_cached")
            wait_for("b_rewritten")
            os.pwrite(fd, b"A" * 32, 3000)
            signal("a_dirtied")
            wait_for("b_done")
            os.fsync(fd)
            os.close(fd)
            """, dedent("""
            wait_for("a_open")
            fd = lazy_open("f")
            signal("b_open")
            wait_for("a_cached")
            os.pwrite(fd, b"Q" * 4096, 0)
            os.fsync(fd)
            signal("b_rewritten")
            wait_for("a_dirtied")
            """) + dedent(b_op) + dedent("""
            os.close(fd)
            signal("b_done")
            """))
        self._remount_all()

    def test_punch_hole_next_to_dirty_range(self):
        """b punches a hole in the page a holds stale with a dirty range."""
        self._stale_page_then("""
            libc = ctypes.CDLL(None, use_errno=True)
            libc.fallocate.argtypes = [ctypes.c_int, ctypes.c_int,
                                       ctypes.c_long, ctypes.c_long]
            # FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE
            if libc.fallocate(fd, 3, 0, 1000) != 0:
                raise OSError(ctypes.get_errno(), "fallocate")
            """)
        expect = bytearray(b"Q" * PAGE)
        expect[0:1000] = b"\0" * 1000
        expect[3000:3032] = b"A" * 32
        self._check("f", bytes(expect))

    def test_direct_write_next_to_dirty_range(self):
        """b rewrites, with O_DIRECT, the page a holds stale."""
        self._stale_page_then("""
            dfd = os.open(path("f"), os.O_RDWR | os.O_DIRECT)
            buf = mmap.mmap(-1, 4096)
            buf.write(b"D" * 4096)
            os.pwrite(dfd, buf, 0)
            os.close(dfd)
            """)
        # a's 32 bytes may land before or after b's direct write, but no
        # stale P or Q may come back around them
        data = self._read(self.mount_a, "f")
        self.assertEqual(len(data), PAGE)
        self.assertEqual(data[:3000] + data[3032:], b"D" * (PAGE - 32))
        self.assertIn(data[3000:3032], (b"A" * 32, b"D" * 32))

    def test_mmap_writes_disjoint_pages(self):
        """a writes its pages through a shared mapping, b with pwrite()."""
        pages = 8
        self._write("f", b"\0" * pages * PAGE)
        self._run_pair(f"""
            fd = lazy_open("f")
            signal("a_open")
            wait_for("b_open")
            m = mmap.mmap(fd, {pages} * 4096)
            for i in range(0, {pages}, 2):
                m[i * 4096:(i + 1) * 4096] = b"M" * 4096
            m.flush()
            m.close()
            os.fsync(fd)
            signal("a_synced")
            wait_for("b_synced")
            os.close(fd)
            """, f"""
            wait_for("a_open")
            fd = lazy_open("f")
            signal("b_open")
            for i in range(1, {pages}, 2):
                os.pwrite(fd, b"W" * 4096, i * 4096)
            os.fsync(fd)
            signal("b_synced")
            wait_for("a_synced")
            os.close(fd)
            """)
        self._remount_all()
        self._check("f", b"".join((b"M", b"W")[i % 2] * PAGE
                                  for i in range(pages)))

    # -- visibility --

    def test_visible_to_a_new_reader_after_fsync(self):
        """What a writer has synced, a client that has not cached it reads."""
        self._run_pair("""
            fd = lazy_open("f")
            os.pwrite(fd, b"A" * 10000, 0)
            os.fsync(fd)
            signal("a_synced")
            wait_for("b_read")
            os.close(fd)
            """, """
            wait_for("a_synced")
            fd = lazy_open("f", os.O_RDONLY)
            data = os.pread(fd, 20000, 0)
            os.close(fd)
            assert data == b"A" * 10000, (len(data), data[:16])
            signal("b_read")
            """)

    def test_reader_drops_cache_to_see_new_data(self):
        """
        A lazy reader may keep reading its cache while another client
        writes, and sees the new data once the writer has synced and the
        reader has dropped the file from its cache (a remount).
        """
        self._write("f", b"P" * 10000)
        self._run_pair("""
            fd = lazy_open("f", os.O_RDONLY)
            assert os.pread(fd, 10000, 0) == b"P" * 10000
            signal("a_cached")
            wait_for("b_synced")
            os.close(fd)
            """, """
            wait_for("a_cached")
            fd = lazy_open("f")
            os.pwrite(fd, b"Q" * 10000, 0)
            os.fsync(fd)
            os.close(fd)
            signal("b_synced")
            """)
        self._remount(self.mount_a)
        self._check("f", b"Q" * 10000)

    # -- unmount, snapshots, recovery --

    def test_umount_flushes_dirty_data(self):
        """Unmounting writes back what lazy writers have not synced."""
        self._run_pair("""
            fd = lazy_open("f")
            signal("a_open")
            wait_for("b_open")
            os.pwrite(fd, b"A" * 3000, 0)
            os.close(fd)
            """, """
            wait_for("a_open")
            fd = lazy_open("f")
            signal("b_open")
            os.pwrite(fd, b"B" * 3000, 3000)
            os.close(fd)
            """)
        for m in (self.mount_a, self.mount_b):
            m.umount_wait(timeout=60)
            self._mount(m)
        self._check("f", b"A" * 3000 + b"B" * 3000)

    def test_umount_after_truncate_drops_dirty_range(self):
        """
        A truncate that drops a lazy dirty range leaves nothing to write
        back: unmounting must not wait for that page.
        """
        self._write("f", b"P" * 4 * PAGE)
        self._run_pair("""
            fd = lazy_open("f")
            signal("a_open")
            wait_for("b_open")
            os.pread(fd, 4 * 4096, 0)
            os.pwrite(fd, b"A" * 32, 4096 + 3000)
            os.ftruncate(fd, 4096 + 1000)
            os.close(fd)
            signal("a_done")
            """, """
            wait_for("a_open")
            fd = lazy_open("f")
            signal("b_open")
            wait_for("a_done")
            os.close(fd)
            """)
        self.mount_a.umount_wait(timeout=30)
        self._mount(self.mount_a)
        self._check("f", b"P" * (PAGE + 1000))

    def test_snapshot_of_dirty_lazy_data(self):
        """
        Data that lazy writers have not synced when a snapshot is taken
        belongs to the snapshot; what they write after it does not.  (Each
        writes its own page, so that this does not depend on writing back
        only part of a page.)
        """
        self._run_pair("""
            fd = lazy_open("f")
            signal("a_open")
            wait_for("b_open")
            os.pwrite(fd, b"1" * 2000, 0)
            signal("a_written")
            wait_for("snapped")
            os.pwrite(fd, b"2" * 2000, 0)
            os.fsync(fd)
            signal("a_synced")
            os.close(fd)
            """, """
            wait_for("a_open")
            fd = lazy_open("f")
            signal("b_open")
            os.pwrite(fd, b"1" * 2000, 4096)
            wait_for("a_written")
            os.mkdir(os.path.join(D, ".snap", "s1"))
            signal("snapped")
            os.pwrite(fd, b"2" * 2000, 4096)
            os.fsync(fd)
            wait_for("a_synced")
            os.close(fd)
            """)
        self._remount_all()
        gap = b"\0" * (PAGE - 2000)
        self._check("f", b"2" * 2000 + gap + b"2" * 2000)
        self._check(".snap/s1/f", b"1" * 2000 + gap + b"1" * 2000)
        self.mount_a.run_shell(["rmdir", "lazyio/.snap/s1"])

    def test_mds_failover_with_dirty_data(self):
        """Lazy writers write back what they buffered across an MDS failover."""
        self._write("f", b"P" * PAGE)
        a = self._start(self.mount_a, """
            fd = lazy_open("f")
            signal("a_open")
            wait_for("b_open")
            os.pwrite(fd, b"A" * 1000, 0)
            signal("a_written")
            wait_for("failed_over", 300)
            os.fsync(fd)
            os.close(fd)
            """)
        b = self._start(self.mount_b, """
            wait_for("a_open")
            fd = lazy_open("f")
            signal("b_open")
            os.pwrite(fd, b"B" * 1000, 2000)
            signal("b_written")
            wait_for("failed_over", 300)
            os.fsync(fd)
            os.close(fd)
            """)
        try:
            self._wait_for("a_written", "b_written")
            self.fs.rank_fail()
            self.fs.wait_for_daemons()
        finally:
            self._signal("failed_over")
            a.wait()
            b.wait()
        self._remount_all()
        expect = bytearray(b"P" * PAGE)
        expect[0:1000] = b"A" * 1000
        expect[2000:3000] = b"B" * 1000
        self._check("f", bytes(expect))

    def test_evicted_writer_does_not_clobber(self):
        """
        An evicted client loses what it buffered, but must not write stale
        bytes over what another client wrote.
        """
        self._write("f", b"P" * PAGE)
        a = self._start(self.mount_a, """
            fd = lazy_open("f")
            signal("a_open")
            wait_for("b_open")
            os.pread(fd, 4096, 0)
            signal("a_cached")
            wait_for("b_rewritten")
            os.pwrite(fd, b"A" * 32, 3000)
            signal("a_dirtied")
            # evicted meanwhile; exiting drops what is left
            time.sleep(30)
            """)
        try:
            self._run(self.mount_b, """
                wait_for("a_open")
                fd = lazy_open("f")
                signal("b_open")
                wait_for("a_cached")
                os.pwrite(fd, b"Q" * 4096, 0)
                os.fsync(fd)
                os.close(fd)
                signal("b_rewritten")
                wait_for("a_dirtied")
                """)
            ida = self.mount_a.get_global_id()
            self.fs.rank_asok(['session', 'evict', str(ida)])
        finally:
            try:
                a.wait()
            except CommandFailedError:
                pass
            self.mount_a.kill()
            self.mount_a.kill_cleanup()
        self._mount(self.mount_a)
        self._remount(self.mount_b)
        data = self._read(self.mount_b, "f")
        self.assertEqual(data[:3000] + data[3032:], b"Q" * (PAGE - 32))
        self.assertIn(data[3000:3032], (b"A" * 32, b"Q" * 32))
