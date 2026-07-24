import logging
from textwrap import dedent
from tasks.cephfs.cephfs_test_case import CephFSTestCase

log = logging.getLogger(__name__)

OBJECT_SIZE = 4 * 1024 * 1024  # 4MB


class TestCopyFileRange(CephFSTestCase):
    """
    Test copy_file_range via the kernel VFS, which exercises the underlying
    COPY2 offload path in both FUSE (ceph-fuse) and kclient mounts.

    The kernel copy_file_range(2) syscall routes through:
      - FUSE:  FUSE_COPY_FILE_RANGE -> ceph-fuse -> ll_copy_file_range -> COPY2
      - kclient: ceph_copy_file_range -> COPY2

    Small / unaligned ranges fall back to read+write in userspace (FUSE) or
    the kernel (kclient), while object-aligned ranges use server-side COPY2.
    """

    CLIENTS_REQUIRED = 1

    def _host_path(self, mount, basename):
        """Return the absolute host-filesystem path for a CephFS file."""
        import os
        return os.path.join(mount.hostfs_mntpt, basename)

    def _copy_and_verify(self, mount, src, dst, src_off, dst_off, length):
        """
        Use Python's os.copy_file_range() to copy a range between two
        files on the given mount, then verify the destination content
        matches the source.
        """
        src_path = self._host_path(mount, src)
        dst_path = self._host_path(mount, dst)

        pyscript = dedent(f"""
            import os

            src_fd = os.open("{src_path}", os.O_RDONLY)
            dst_fd = os.open("{dst_path}", os.O_WRONLY | os.O_CREAT, 0o666)

            copied = os.copy_file_range(src_fd, dst_fd, {length},
                                        {src_off}, {dst_off})
            print(f"COPIED={{copied}}")
            print(f"SRC_OFF={{os.lseek(src_fd, 0, os.SEEK_CUR)}}")
            print(f"DST_OFF={{os.lseek(dst_fd, 0, os.SEEK_CUR)}}")

            os.close(src_fd)
            os.close(dst_fd)

            # Verify: read back and compare
            src_fd = os.open("{src_path}", os.O_RDONLY)
            dst_fd = os.open("{dst_path}", os.O_RDONLY)

            src_data = os.pread(src_fd, {length}, {src_off})
            dst_data = os.pread(dst_fd, {length}, {dst_off})

            if src_data == dst_data:
                print("VERIFY=OK")
            else:
                print(f"VERIFY=FAIL src_len={{len(src_data)}} dst_len={{len(dst_data)}}")

            os.close(src_fd)
            os.close(dst_fd)
        """)
        output = mount.run_python(pyscript)
        log.info(f"copy_file_range output:\n{output}")

        lines = output.strip().split('\n')
        result = {}
        for line in lines:
            if '=' in line:
                k, v = line.split('=', 1)
                result[k] = v

        self.assertEqual(result.get('COPIED'), str(length),
                         f"Expected to copy {length} bytes")
        self.assertEqual(result.get('VERIFY'), 'OK',
                         "Destination content does not match source")

    def _create_pattern_file(self, mount, path, size):
        """
        Create a file with known repeating content for verification.
        """
        host_path = self._host_path(mount, path)

        pyscript = dedent(f"""
            import os

            fd = os.open("{host_path}", os.O_WRONLY | os.O_CREAT, 0o666)
            buf = bytes(range(256)) * 4096  # 1MB, repeats 0..255
            written = 0
            while written < {size}:
                chunk = min(len(buf), {size} - written)
                n = os.write(fd, buf[:chunk])
                written += n
            os.close(fd)
            print(f"WROKEN={{written}}")
        """)
        output = mount.run_python(pyscript)
        log.info(f"create_pattern_file output: {output}")

    def test_copy_file_range_small_file(self):
        """
        Copy a small file (below object_size) — exercises the manual
        read/write fallback path.
        """
        src = "test_cfr_small_src"
        dst = "test_cfr_small_dst"
        file_size = 1024

        self._create_pattern_file(self.mount_a, src, file_size)
        self._copy_and_verify(self.mount_a, src, dst, 0, 0, file_size)

        self.mount_a.run_shell(["rm", "-f", src, dst])

    def test_copy_file_range_large_aligned(self):
        """
        Copy a large object-aligned file — exercises the server-side
        COPY2 offload path.
        """
        src = "test_cfr_large_src"
        dst = "test_cfr_large_dst"
        file_size = 4 * OBJECT_SIZE  # 16MB, 4 objects

        self._create_pattern_file(self.mount_a, src, file_size)
        self._copy_and_verify(self.mount_a, src, dst, 0, 0, file_size)

        # Verify destination size via host path
        size = self.mount_a.run_shell(
            ["stat", "--format=%s",
             self._host_path(self.mount_a, dst)]).stdout.getvalue().strip()
        self.assertEqual(int(size), file_size)

        self.mount_a.run_shell(["rm", "-f", src, dst])

    def test_copy_file_range_sub_range(self):
        """
        Copy a sub-range of a file — object-aligned offset and length.
        """
        src = "test_cfr_sub_src"
        dst = "test_cfr_sub_dst"
        file_size = 3 * OBJECT_SIZE  # 12MB

        self._create_pattern_file(self.mount_a, src, file_size)

        # Object-aligned sub-range: skip first object, copy the second
        src_off = OBJECT_SIZE
        dst_off = 0
        copy_len = OBJECT_SIZE

        self._copy_and_verify(self.mount_a, src, dst,
                              src_off, dst_off, copy_len)

        self.mount_a.run_shell(["rm", "-f", src, dst])

    def test_copy_file_range_unaligned_fallback(self):
        """
        Copy with unaligned offsets — exercises the fallback path where
        the FUSE/kernel VFS falls back to read+write.
        """
        src = "test_cfr_unaligned_src"
        dst = "test_cfr_unaligned_dst"
        file_size = 1024 * 1024  # 1MB

        self._create_pattern_file(self.mount_a, src, file_size)

        # Unaligned offsets force the fallback path
        self._copy_and_verify(self.mount_a, src, dst,
                              512, 100, 5000)

        self.mount_a.run_shell(["rm", "-f", src, dst])

    def test_copy_file_range_zero_length(self):
        """
        Copy zero bytes — should succeed as a no-op.
        """
        src = "test_cfr_zero_src"
        dst = "test_cfr_zero_dst"
        file_size = 1024

        self._create_pattern_file(self.mount_a, src, file_size)

        src_path = self._host_path(self.mount_a, src)
        dst_path = self._host_path(self.mount_a, dst)

        pyscript = dedent(f"""
            import os

            src_fd = os.open("{src_path}", os.O_RDONLY)
            dst_fd = os.open("{dst_path}", os.O_WRONLY | os.O_CREAT, 0o666)

            copied = os.copy_file_range(src_fd, dst_fd, 0, 0, 0)
            print(f"COPIED={{copied}}")

            os.close(src_fd)
            os.close(dst_fd)
        """)
        output = self.mount_a.run_python(pyscript)
        self.assertIn("COPIED=0", output)

        self.mount_a.run_shell(["rm", "-f", src, dst])

    def test_copy_file_range_overwrite(self):
        """
        Overwrite a middle section of an existing destination file.
        """
        src = "test_cfr_ow_src"
        dst = "test_cfr_ow_dst"
        file_size = 1024 * 1024  # 1MB

        self._create_pattern_file(self.mount_a, src, file_size)

        # Create dst with different content (all zeros)
        dst_path = self._host_path(self.mount_a, dst)
        pyscript = dedent(f"""
            import os
            fd = os.open("{dst_path}", os.O_WRONLY | os.O_CREAT, 0o666)
            buf = b'\\x00' * 4096
            written = 0
            while written < {file_size}:
                n = os.write(fd, buf)
                written += n
            os.close(fd)
        """)
        self.mount_a.run_python(pyscript)

        # Overwrite a middle section
        src_off = 4096
        dst_off = 8192
        copy_len = 16384

        self._copy_and_verify(self.mount_a, src, dst,
                              src_off, dst_off, copy_len)

        self.mount_a.run_shell(["rm", "-f", src, dst])

    def test_copy_file_range_perf_compare(self):
        """
        Performance comparison: copy_file_range (COPY2) vs manual read+write
        for a large object-aligned file.

        Reports the speedup factor of server-side copy offload over the
        traditional client-side read/write loop.
        """
        file_size = 8 * OBJECT_SIZE  # 32MB, 8 objects
        buf_size = 1024 * 1024
        size_mb = file_size // (1024 * 1024)
        src_c2 = "test_cfr_perf_src_c2"
        src_rw = "test_cfr_perf_src_rw"
        dst_c2 = "test_cfr_perf_dst_c2"
        dst_rw = "test_cfr_perf_dst_rw"

        # Separate source files for fair comparison (same approach as
        # the C++ PerfCompare test in copyfilerange.cc).
        self._create_pattern_file(self.mount_a, src_c2, file_size)
        self._create_pattern_file(self.mount_a, src_rw, file_size)

        src_c2_p = self._host_path(self.mount_a, src_c2)
        src_rw_p = self._host_path(self.mount_a, src_rw)
        dst_c2_p = self._host_path(self.mount_a, dst_c2)
        dst_rw_p = self._host_path(self.mount_a, dst_rw)

        # Helper: drop kernel page cache for fair comparison.
        # Needs root; silently skip if not available.
        def _drop_caches():
            self.mount_a.run_shell(["sync"])
            self.mount_a.run_shell(
                ["sudo", "sh", "-c",
                 "echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true"],
                check_status=False)

        # --- COPY2 via os.copy_file_range (own source file) ---
        _drop_caches()
        pyscript = dedent(f"""
            import os, time

            sf = os.open("{src_c2_p}", os.O_RDONLY)
            df = os.open("{dst_c2_p}", os.O_WRONLY | os.O_CREAT, 0o666)
            t0 = time.time()
            c2 = os.copy_file_range(sf, df, {file_size}, 0, 0)
            t_copy2 = time.time() - t0
            os.close(sf)
            os.close(df)
            print("COPY2=" + str(round(t_copy2, 3)) + "s")
        """)
        out = self.mount_a.run_python(pyscript)

        # --- Manual read+write (own source file, 1MB buffer) ---
        _drop_caches()
        pyscript = dedent(f"""
            import os, time

            sf = os.open("{src_rw_p}", os.O_RDONLY)
            df = os.open("{dst_rw_p}", os.O_WRONLY | os.O_CREAT, 0o666)
            t0 = time.time()
            total = 0
            while total < {file_size}:
                data = os.read(sf, {buf_size})
                if not data: break
                os.write(df, data)
                total += len(data)
            t_rw = time.time() - t0
            os.close(sf)
            os.close(df)
            print("FALLBACK=" + str(round(t_rw, 3)) + "s")
        """)
        out_rw = self.mount_a.run_python(pyscript)

        # Print comparison (goes to test stdout via captured Python output)
        t_copy2 = float(out.strip().split('=')[1].rstrip('s'))
        t_rw = float(out_rw.strip().split('=')[1].rstrip('s'))
        ratio = t_rw / t_copy2 if t_copy2 > 0 else float('inf')
        log.info(f"=== PerfCompare ({size_mb}MB) ===")
        log.info(f"COPY2:     {t_copy2:.3f}s")
        log.info(f"fallback:  {t_rw:.3f}s")
        log.info(f"speedup:   {ratio:.1f}x")

        self.mount_a.run_shell(["rm", "-f", src_c2, src_rw, dst_c2, dst_rw])
