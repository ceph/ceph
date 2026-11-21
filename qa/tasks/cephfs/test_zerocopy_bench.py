"""
Functional coverage for cephfs-tool bench across sync, async-io, and zero-copy.

Collects --json / --perf-dump / --lockstat-dump artifacts under the teuthology
archive and asserts client in-flight peak counters by I/O mode. Bandwidth is
not gated.
"""

from io import StringIO
import json
import logging
import os

from teuthology import misc
from teuthology.exceptions import CommandFailedError

from tasks.cephfs.cephfs_test_case import CephFSTestCase, for_teuthology

log = logging.getLogger(__name__)


def _lockstat_env():
    env = os.environ.copy()
    env['ENABLE_LOCKSTAT'] = 'true'
    return env


class TestZeroCopyBench(CephFSTestCase):
    """
    Drive cephfs-tool via libcephfs (-i <client>). A FUSE mount is not required
    for I/O; CephFSTestCase still provisions the filesystem and client auth.
    """
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    FILES = 8
    SIZE = '4M'
    BLOCK_SIZE = '64K'
    ITERATIONS = 1

    def setUp(self):
        # Skip FUSE mount_wait: this test only needs FS + client auth for
        # cephfs-tool. Avoids vstart asok/stdin-killer mount flakiness.
        saved = []
        for m in self.mounts:
            saved.append(m.mount_wait)
            m.mount_wait = lambda **kwargs: None
        try:
            super().setUp()
        finally:
            for m, orig in zip(self.mounts, saved):
                m.mount_wait = orig

    def _archive_dir(self):
        path = os.path.join(
            misc.get_testdir(self.mount_a.ctx),
            'archive',
            'cephfs-tool-zerocopy',
        )
        self.mount_a.client_remote.run(args=['mkdir', '-p', path])
        return path

    def _supports_lockstat(self):
        p = self.mount_a.client_remote.run(
            args=['cephfs-tool', '--help'],
            stdout=StringIO(),
            stderr=StringIO(),
            check_status=False,
        )
        help_text = p.stdout.getvalue() + p.stderr.getvalue()
        return 'lockstat-dump' in help_text

    def _load_json(self, remote_path):
        raw = self.mount_a.client_remote.read_file(remote_path)
        if isinstance(raw, bytes):
            raw = raw.decode(errors='replace')
        # Prefer the first JSON value so trailing junk (e.g. historical
        # non-NUL-terminated perf dumps) does not fail the test.
        return json.JSONDecoder().raw_decode(raw.lstrip())[0]

    def _client_counter(self, perf, name, default=0):
        client = perf.get('client', {})
        if name not in client:
            return default
        return int(client[name])

    def _artifact_prefix(self, archive, mode, threads, queue_depth):
        qd = 0 if queue_depth is None else queue_depth
        return os.path.join(archive, f'{mode}_t{threads}_qd{qd}')

    def _run_bench(self, mode, threads, queue_depth, archive, with_lockstat):
        prefix = self._artifact_prefix(archive, mode, threads, queue_depth)
        json_path = f'{prefix}.json'
        perf_path = f'{prefix}.perf.json'
        lockstat_path = f'{prefix}_lockstat.json'
        dir_prefix = f'zc_{mode}_t{threads}_qd{queue_depth if queue_depth else 0}_'

        args = [
            'cephfs-tool',
            '-i', str(self.mount_a.client_id),
            '--filesystem', self.fs.name,
            '--client-oc', '0',
            'bench',
            '--dir-prefix', dir_prefix,
            '--files', str(self.FILES),
            '--size', self.SIZE,
            '--block-size', self.BLOCK_SIZE,
            '--threads', str(threads),
            '--iterations', str(self.ITERATIONS),
            '--json', json_path,
            '--perf-dump', perf_path,
        ]

        if mode == 'async':
            args.append('--async-io')
        elif mode == 'zerocopy':
            args.append('--zero-copy')
        elif mode != 'sync':
            self.fail(f'unknown mode {mode}')

        if mode != 'sync':
            assert queue_depth is not None
            args.extend(['--queue-depth', str(queue_depth)])

        if with_lockstat:
            args.extend(['--lockstat-dump', lockstat_path])

        log.info('running: %s', ' '.join(args))
        self.mount_a.client_remote.run(
            args=args,
            stdout=StringIO(),
            stderr=StringIO(),
            timeout=900,
            env=_lockstat_env(),
        )
        return json_path, perf_path, lockstat_path

    def _assert_bench_json(self, bench, mode, threads, queue_depth):
        # cephfs-tool --json may wrap under "benchmark" or emit sections at root.
        root = bench.get('benchmark', bench)
        self.assertIn('configuration', root)
        conf = root['configuration']
        summary = root.get('summary')
        self.assertTrue(summary)

        if mode == 'sync':
            self.assertFalse(conf.get('async_io', False))
            self.assertFalse(conf.get('zero_copy', False))
        elif mode == 'async':
            self.assertTrue(conf.get('async_io', False))
            self.assertFalse(conf.get('zero_copy', False))
            self.assertEqual(int(conf.get('queue_depth')), queue_depth)
        else:
            self.assertFalse(conf.get('async_io', False))
            self.assertTrue(conf.get('zero_copy', False))
            self.assertEqual(int(conf.get('queue_depth')), queue_depth)

        self.assertEqual(int(conf.get('threads')), threads)

    def _assert_perf_counters(self, perf, mode, queue_depth):
        aio_peak = self._client_counter(perf, 'aio_in_flight_peak')
        sync_peak = self._client_counter(perf, 'sync_in_flight_peak')
        log.info('mode=%s aio_in_flight_peak=%s sync_in_flight_peak=%s',
                 mode, aio_peak, sync_peak)

        if mode == 'sync':
            self.assertGreaterEqual(
                sync_peak, 1,
                'sync bench should increment sync_in_flight_peak')
            self.assertEqual(
                aio_peak, 0,
                'sync bench with client_oc=0 should not bump aio_in_flight_peak')
            return

        self.assertGreaterEqual(
            aio_peak, 1,
            f'{mode} bench should increment aio_in_flight_peak')
        if queue_depth is not None and queue_depth >= 8:
            self.assertGreaterEqual(
                aio_peak, min(queue_depth, 2),
                f'{mode} with queue_depth={queue_depth} should show concurrency')

    def _assert_lockstat(self, lockstat_base):
        root, ext = os.path.splitext(lockstat_base)
        for phase in ('write', 'read'):
            path = f'{root}_iter1_{phase}{ext}'
            try:
                data = self._load_json(path)
            except Exception as e:
                self.fail(f'missing/invalid lockstat dump {path}: {e}')
            # dump_formatted may wrap under "lockstat" or emit fields at root.
            payload = data.get('lockstat', data)
            self.assertTrue(
                'bin_ranges' in payload or 'entries' in payload,
                f'{path} missing lockstat fields: {list(payload)[:8]}')

    def _run_and_verify(self, mode, threads, queue_depth=None):
        archive = self._archive_dir()
        with_lockstat = self._supports_lockstat()
        if not with_lockstat:
            log.warning('cephfs-tool lacks --lockstat-dump; skipping lockstat checks')

        try:
            json_path, perf_path, lockstat_path = self._run_bench(
                mode, threads, queue_depth, archive, with_lockstat)
        except CommandFailedError as e:
            self.fail(f'cephfs-tool bench failed for mode={mode} '
                      f'threads={threads} queue_depth={queue_depth}: {e}')

        bench = self._load_json(json_path)
        self._assert_bench_json(bench, mode, threads, queue_depth)

        perf = self._load_json(perf_path)
        self._assert_perf_counters(perf, mode, queue_depth)

        if with_lockstat:
            self._assert_lockstat(lockstat_path)

    def test_zerocopy_smoke(self):
        """Single zero-copy run for quick local / short teuthology checks."""
        self._run_and_verify('zerocopy', threads=1, queue_depth=1)

    @for_teuthology
    def test_zerocopy_bench_sweep(self):
        """
        Mode x threads x queue-depth sweep with JSON, perf-dump, and lockstat.
        """
        for mode in ('sync', 'async', 'zerocopy'):
            for threads in (1, 4):
                if mode == 'sync':
                    self._run_and_verify(mode, threads=threads, queue_depth=None)
                else:
                    for queue_depth in (1, 8):
                        self._run_and_verify(
                            mode, threads=threads, queue_depth=queue_depth)
