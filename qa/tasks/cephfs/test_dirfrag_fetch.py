"""Test multi-batch dirfrag fetches in pipelined and buffered modes."""

import errno
import json
import signal
from contextlib import contextmanager
from io import BytesIO, StringIO

from tasks.cephfs.cephfs_test_case import CephFSTestCase


class DirfragFetchLogMixin:
    def _config_set(self, key, value):
        """Set mon config with cleanup; MDS restarts do not clear the mon store."""
        self.addCleanup(self.config_rm, 'mds', key)
        self.config_set('mds', key, value)

    def _configure(self, *, rank=0, **kwargs):
        for k, v in kwargs.items():
            if k in ('mds_inject_dir_fetch_delay',
                     'mds_dir_prefetch_backend', 'mds_dir_fetch_pipelined'):
                # Apply before fetch submission; setUp restarts MDS to reset overrides.
                self.fs.rank_asok(['config', 'set', str(k), str(v)], rank=rank)
            else:
                self._config_set(str(k), str(v))

    def _wait_for_config(self, key, value, rank=0):
        """Wait for the MDS to apply the mon config setting."""
        def applied():
            got = self.fs.rank_asok(['config', 'get', key], rank=rank)[key]
            return str(got).lower() == str(value).lower()

        self.wait_until_true(applied, timeout=60, period=1)

    def _dump_dir(self, dirname, rank=0):
        dirs = self.fs.rank_asok(['dump', 'dir', '/' + dirname, 'true'], rank=rank)
        self.assertEqual(len(dirs), 1, "test directory must have one dirfrag")
        return dirs[0]

    def _drop_caches(self):
        """Flush metadata and drop client and MDS caches to force a disk fetch."""
        self.mount_a.umount_wait()
        # Unmount can return before the MDS drops the client's session and
        # caps. A cache drop at that point may leave the entire target warm.
        self.wait_until_true(lambda: not self.fs.rank_asok(['session', 'ls']),
                             timeout=90, period=1)
        self.fs.flush()
        self.fs.rank_tell(['cache', 'drop'])
        self.mount_a.mount_wait()

    def _create_files(self, dirname, count, prefix="file"):
        self.mount_a.run_shell(["mkdir", "-p", dirname])
        self.mount_a.run_python(f"""
import os
d = os.path.join("{self.mount_a.hostfs_mntpt}", "{dirname}")
for i in range({count}):
    open(os.path.join(d, "{prefix}_%06d" % i), "w").close()
""")

    def _listdir(self, dirname):
        out = self.mount_a.run_shell(["ls", "-1", dirname]).stdout.getvalue()
        return sorted(x for x in out.split("\n") if x)

    def _stat_missing(self, path, wait=True):
        """Check the actual lookup errno, including for asynchronous reads."""
        proc = self.mount_a._run_python(f"""
import errno
import os
path = os.path.join({self.mount_a.hostfs_mntpt!r}, {path!r})
try:
    os.stat(path)
except OSError as exc:
    assert exc.errno == errno.ENOENT, (path, exc.errno, str(exc))
else:
    raise AssertionError('unexpectedly found ' + path)
""", timeout=180)
        if wait:
            proc.wait()
        return proc

    @contextmanager
    def _capture_mds_log(self, rank=0, level=10):
        """Capture rank logs; enter after restart and finish before daemon replacement."""
        mds_id = self.fs.get_rank(rank=rank)['name']
        remote = self.fs.mon_manager.find_remote('mds', mds_id)

        def asok(args):
            return self.fs.mds_asok(args, mds_id=mds_id)

        def gather_level(value):
            # "debug_mds" reads back as "<log>/<gather>".
            try:
                return int(str(value).split('/')[0])
            except ValueError:
                return -1

        saved = {}
        try:
            wanted = [('log_to_file', 'true')]
            # Preserve any higher debug level configured by the suite.
            if gather_level(asok(['config', 'get', 'debug_mds'])['debug_mds']) < level:
                wanted.append(('debug_mds', str(level)))
            for key, value in wanted:
                saved[key] = asok(['config', 'get', key])[key]
                asok(['config', 'set', key, value])
            log_path = asok(['config', 'get', 'log_file'])['log_file']
            self.assertTrue(log_path, "MDS must have a file log for this test")
            asok(['log', 'flush'])
            offset = int(remote.run(
                args=['sudo', 'stat', '-c', '%s', '--', log_path],
                stdout=StringIO()).stdout.getvalue())
            def read_log():
                asok(['log', 'flush'])
                output = remote.run(
                    args=['sudo', 'tail', '-c', '+{}'.format(offset + 1),
                          '--', log_path], stdout=StringIO()).stdout.getvalue()
                return output.splitlines()

            yield read_log
        finally:
            for key, value in reversed(list(saved.items())):
                asok(['config', 'set', key, value])

    @contextmanager
    def _capture_fetch_log(self, ino, rank=0):
        """Read new messages for one unsplit dirfrag, including restarts."""
        prefix = '.cache.dir(0x{:x}) '.format(ino)
        with self._capture_mds_log(rank=rank) as read_log:
            yield lambda: [line for line in read_log() if prefix in line]


class TestDirfragFetch(DirfragFetchLogMixin, CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    KEYS_PER_OP = 16
    NFILES = 200

    def setUp(self):
        super(TestDirfragFetch, self).setUp()
        # Keep all batches in one dirfrag.
        self._configure(mds_bal_fragment_dirs=False,
                        mds_dir_keys_per_op=self.KEYS_PER_OP)
        self._wait_for_config('mds_dir_keys_per_op', self.KEYS_PER_OP)

    def _dir_fetch_count(self):
        return self.fs.rank_asok(['perf', 'dump', 'mds'])['mds']['dir_fetch_complete']

    def _check_readdir_after_fetch(self, dirname, expected, pipelined=None):
        """Check a cold listing; when pipelined is given, also verify batching."""
        before = self._dir_fetch_count()
        ino = self.mount_a.path_to_ino(dirname)
        self._drop_caches()
        if pipelined is None:
            listing = self._listdir(dirname)
        else:
            with self._capture_fetch_log(ino) as read_log:
                listing = self._listdir(dirname)
                decoded = [line for line in read_log()
                           if '_fetched ' in line and ' keys for ' in line]
            if pipelined:
                self.assertGreaterEqual(
                    len(decoded), 2,
                    "fetch decoded in a single batch: {0}".format(decoded))
            else:
                # Find the combined decode; concurrent keyed reads may log others.
                sizes = [int(line.split('_fetched ')[1].split(' keys for ')[0])
                         for line in decoded]
                self.assertTrue(
                    any(n > self.KEYS_PER_OP for n in sizes),
                    "buffered fetch never decoded more than one batch at "
                    "once: {0}".format(decoded))
        after = self._dir_fetch_count()

        self.assertEqual(len(listing), len(expected),
                         "listed {0} entries, expected {1}".format(
                             len(listing), len(expected)))
        self.assertEqual(listing, expected)

        self.assertGreater(after, before,
                           "dirfrag was served from cache, fetch path untested")

    def test_multi_batch_fetch_pipelined(self):
        """Read a multi-batch dirfrag intact with pipelining enabled."""
        self._configure(mds_dir_fetch_pipelined=True)

        dirname = "pipelined"
        self._create_files(dirname, self.NFILES)
        expected = self._listdir(dirname)
        self.assertEqual(len(expected), self.NFILES)

        self._check_readdir_after_fetch(dirname, expected, pipelined=True)

    def test_multi_batch_fetch_not_pipelined(self):
        """Read a multi-batch dirfrag intact with buffering enabled."""
        self._configure(mds_dir_fetch_pipelined=False)

        dirname = "not_pipelined"
        self._create_files(dirname, self.NFILES)
        expected = self._listdir(dirname)
        self.assertEqual(len(expected), self.NFILES)

        self._check_readdir_after_fetch(dirname, expected, pipelined=False)

    def _check_cold_fetch_version(self, pipelined):
        self._configure(mds_dir_fetch_pipelined=pipelined)
        dirname = 'cold_fetch_version'
        self._create_files(dirname, self.NFILES)
        ino = self.mount_a.path_to_ino(dirname)
        self._drop_caches()
        # Resolve ancestors without fetching any entries in the target.
        self.mount_a.run_shell(['stat', dirname])
        cold = self._dump_dir(dirname)
        if cold.get('status') != 'dirfrag not in cache':
            self.assertEqual(int(cold['version']), 0,
                             'must exercise adoption of an on-disk fnode')
            self.assertEqual(int(cold['committed_version']), 0)
            self.assertNotIn('complete', cold['states'])
            self.assertEqual(cold['dentries'], [])

        with self._capture_fetch_log(ino) as read_log:
            if pipelined:
                listing = self._listdir(dirname)
            else:
                # Observe the first buffered reply before EOF: adopting its
                # header here would mutate the cold dirfrag prematurely.
                self._configure(mds_inject_dir_fetch_delay=10000)
                proc = None
                try:
                    proc = self.mount_a.run_shell(['ls', '-1', dirname], wait=False)
                    self.wait_until_true(
                        lambda: any('_omap_fetch_start buffering header ' in line
                                    for line in read_log()),
                        timeout=60, period=2)
                    during = self._dump_dir(dirname)
                    self.assertFalse(proc.finished)
                    self.assertNotIn('complete', during['states'])
                    for field in ('version', 'committing_version', 'committed_version'):
                        self.assertEqual(int(during[field]), 0)
                    self.assertEqual(during['dentries'], [])
                finally:
                    self._configure(mds_inject_dir_fetch_delay=0)
                    if proc is not None:
                        proc.wait()
                listing = sorted(proc.stdout.getvalue().splitlines())
            lines = read_log()

        self.assertEqual(listing, ['file_%06d' % i for i in range(self.NFILES)])
        loaded = self._dump_dir(dirname)
        self.assertIn('complete', loaded['states'])
        self.assertGreater(int(loaded['committed_version']), 0)
        headers = [line for line in lines if '_fetched header ' in line]
        self.assertEqual(len(headers), 1,
                         'cold fetch reread its header: {}'.format(headers))
        races = [line for line in lines if 'while fetching at v' in line]
        self.assertEqual(races, [],
                         'adopting the disk version was mistaken for a commit')

    def test_cold_fetch_version_pipelined(self):
        """Adopting the fnode must not report a race in any fetch batch."""
        self._check_cold_fetch_version(pipelined=True)

    def test_cold_fetch_version_not_pipelined(self):
        """Adopting the fnode must not cause a redundant buffered fetch."""
        self._check_cold_fetch_version(pipelined=False)

    def _check_cold_background_fetch_version(self, pipelined):
        self._configure(mds_dir_fetch_pipelined=pipelined,
                        mds_dir_prefetch=False,
                        mds_dir_prefetch_backend=False,
                        mds_dir_prefetch_backend_hit_threshold=1,
                        mds_dir_prefetch_backend_max=1)
        dirname = 'background_cold'
        self._create_files(dirname, self.NFILES)
        ino = self.mount_a.path_to_ino(dirname)
        self._drop_caches()
        self.mount_a.run_shell(['stat', dirname])
        self.assertEqual(self._dump_dir(dirname).get('dentries', []), [])
        self._configure(mds_dir_prefetch_backend=True)
        with self._capture_fetch_log(ino) as read_log:
            self.mount_a.run_shell(['stat', f'{dirname}/file_000042'])
            self.wait_until_true(
                lambda: 'complete' in self._dump_dir(dirname).get('states', []),
                timeout=60)
            lines = read_log()
        if pipelined:
            self.assertFalse(any('while fetching at v' in line for line in lines),
                             'adopting the disk fnode was mistaken for a commit')
        # Buffered mode preserves the original version check: a keyed fetch
        # adopting the fnode can cause the full fetch to restart from v0.
        self.assertEqual(sum('_fetched header ' in line for line in lines), 2,
                         'expect one keyed read and one full fetch')
        self.assertEqual(self._listdir(dirname),
                         ['file_%06d' % i for i in range(self.NFILES)])

    def test_cold_background_fetch_version_pipelined(self):
        self._check_cold_background_fetch_version(pipelined=True)

    def test_cold_background_fetch_version_not_pipelined(self):
        self._check_cold_background_fetch_version(pipelined=False)

    def test_both_modes_agree(self):
        """
        That both modes produce the same listing for the same directory.
        """
        dirname = "agree"
        self._create_files(dirname, self.NFILES)
        expected = self._listdir(dirname)

        self._configure(mds_dir_fetch_pipelined=True)
        self._drop_caches()
        pipelined = self._listdir(dirname)

        self._configure(mds_dir_fetch_pipelined=False)
        self._drop_caches()
        buffered = self._listdir(dirname)

        self.assertEqual(pipelined, expected)
        self.assertEqual(buffered, expected)

    def test_single_batch_fetch(self):
        """
        That a dirfrag small enough to arrive in one batch still works -- the
        pipeline must not require a second batch to finish the fetch.
        """
        self._configure(mds_dir_keys_per_op=1024)

        dirname = "single_batch"
        count = 10
        self._create_files(dirname, count)
        expected = self._listdir(dirname)
        self.assertEqual(len(expected), count)

        for mode in (True, False):
            with self.subTest(pipelined=mode):
                self._configure(mds_dir_fetch_pipelined=mode)
                self._check_readdir_after_fetch(dirname, expected)

    def test_empty_dir_fetch(self):
        """An empty on-disk dirfrag finishes a full fetch in both modes."""
        dirname = 'empty'
        self._configure(mds_dir_prefetch=True,
                        mds_dir_prefetch_backend=False)
        self._wait_for_config('mds_dir_prefetch', True)
        # Materialize the object/header before making its OMAP empty.
        self._create_files(dirname, 1)
        self.mount_a.run_shell(['rm', f'{dirname}/file_000000'])
        ino = self.mount_a.path_to_ino(dirname)
        obj = f'{ino:x}.00000000'
        self.fs.flush()
        self.assertEqual(self.fs.radosmo(
            ['listomapkeys', obj], stdout=StringIO()).splitlines(), [])
        raw = self.fs.radosmo(['getomapheader', obj, '-'], stdout=BytesIO())
        header = json.loads(self.fs.dencoder('fnode_t', raw))
        self.assertGreater(int(header['version']), 0)

        for mode in (True, False):
            with self.subTest(pipelined=mode):
                self._configure(mds_dir_fetch_pipelined=mode)
                self._drop_caches()
                self.mount_a.run_shell(['stat', dirname])
                cold = self._dump_dir(dirname)
                if cold.get('status') != 'dirfrag not in cache':
                    self.assertNotIn('complete', cold['states'])
                with self._capture_fetch_log(ino) as read_log:
                    # libcephfs can infer completeness from empty directory
                    # stats. Traverse on the MDS to bypass that client shortcut.
                    # --await supplies the finisher needed for an ENOENT reply.
                    result = self.fs.rank_tell(
                        ['lock', 'path', f'/{dirname}/missing', 'policy:r', '--await'],
                        check_status=False)
                    self.assertEqual(result['result'], -errno.ENOENT, result)
                    lines = read_log()
                decoded = [line for line in lines
                           if '_fetched ' in line and ' keys for ' in line]
                self.assertEqual(len(decoded), 1, lines)
                self.assertIn('_fetched 0 keys for ', decoded[0])
                self.assertIn('|fetching', decoded[0])
                self.assertFalse(any('fetch_keys ' in line for line in lines), lines)
                self.assertIn('complete', self._dump_dir(dirname)['states'])
                self.assertEqual(self._listdir(dirname), [])

    def test_fetch_keys_path(self):
        """
        That the fetch_keys() path -- a single omap_get_vals_by_keys(), never
        batched -- still resolves both present and absent names.  It shares
        the decode with the batched path, so a refactor there can break it.
        """
        self._configure(mds_dir_prefetch=False,
                        mds_dir_prefetch_backend=False)
        self._wait_for_config('mds_dir_prefetch', False)

        dirname = "fetch_keys"
        self._create_files(dirname, self.NFILES)
        ino = self.mount_a.path_to_ino(dirname)

        for mode in (True, False):
            with self.subTest(pipelined=mode):
                self._configure(mds_dir_fetch_pipelined=mode)
                self._drop_caches()
                self.mount_a.run_shell(['stat', dirname])
                cold = self._dump_dir(dirname)
                if cold.get('status') != 'dirfrag not in cache':
                    self.assertNotIn('complete', cold['states'])
                self.assertEqual(cold.get('dentries', []), [])
                with self._capture_fetch_log(ino) as read_log:
                    self.mount_a.run_shell(['stat', f'{dirname}/file_000042'])
                    self._stat_missing(f'{dirname}/does_not_exist')
                    lines = read_log()
                self.assertEqual(sum('fetch_keys 1 keys on ' in line for line in lines),
                                 2, lines)
                sizes = [int(line.split('_fetched ')[1].split(' keys for ')[0])
                         for line in lines if '_fetched ' in line and ' keys for ' in line]
                self.assertEqual(sizes, [1, 0], lines)
                self.assertNotIn('complete', self._dump_dir(dirname)['states'])
                self.assertEqual(self._listdir(dirname),
                                 ['file_%06d' % i for i in range(self.NFILES)])

    def test_fetch_with_concurrent_creates(self):
        """Smoke-test the final namespace after concurrent ls and creates.

        This does not establish overlap with any full-fetch batch. The ls
        may finish first, or its locks may delay the creates until fetch EOF.
        Background-fetch mutation coverage checks actual batch interleaving.
        """
        self._configure(mds_dir_fetch_pipelined=True)

        dirname = "concurrent"
        self._create_files(dirname, self.NFILES)

        self._drop_caches()

        p = self.mount_a.run_shell(["ls", "-1", dirname], wait=False)
        self._create_files(dirname, 20, prefix="zzz")
        p.wait()

        expected = (['file_%06d' % i for i in range(self.NFILES)] +
                    ['zzz_%06d' % i for i in range(20)])
        self.assertEqual(self._listdir(dirname), expected)

    def test_fetch_after_unlink(self):
        """
        That names removed before the fetch do not come back.  A dirfrag
        committed while it is being read used to restart the whole fetch;
        the pipelined path carries on instead, so unlinked names must stay
        unlinked.
        """
        self._configure(mds_dir_fetch_pipelined=True)

        dirname = "unlinked"
        self._create_files(dirname, self.NFILES)

        removed = ["file_%06d" % i for i in range(0, self.NFILES, 10)]
        for name in removed:
            self.mount_a.run_shell(["rm", "-f", f"{dirname}/{name}"])

        expected = self._listdir(dirname)
        self.assertEqual(len(expected), self.NFILES - len(removed))

        self._check_readdir_after_fetch(dirname, expected, pipelined=True)
        for name in removed:
            self.assertNotIn(name, self._listdir(dirname))

    def test_fetch_with_snapshot(self):
        """
        That a fragment carrying snapshotted dentries reads back over several
        batches.  Snap dentries take a different branch in the decode loop
        and are the reason the batch is walked in reverse.
        """
        self._configure(mds_dir_fetch_pipelined=True)

        dirname = "snapped"
        self._create_files(dirname, self.NFILES)

        self.mount_a.run_shell(["mkdir", f"{dirname}/.snap/snap1"])
        try:
            for i in range(0, self.NFILES, 10):
                self.mount_a.run_shell(["rm", "-f", f"{dirname}/file_%06d" % i])

            expected = self._listdir(dirname)
            for mode in (True, False):
                with self.subTest(pipelined=mode):
                    self._configure(mds_dir_fetch_pipelined=mode)
                    self._check_readdir_after_fetch(dirname, expected,
                                                    pipelined=mode)
                    self.assertEqual(self._listdir(f"{dirname}/.snap/snap1"),
                                     ['file_%06d' % i for i in range(self.NFILES)])
        finally:
            self.mount_a.run_shell(["rmdir", f"{dirname}/.snap/snap1"])


    def test_snapshot_versions_across_batches(self):
        """Different incarnations of one name must survive batch boundaries."""
        self.KEYS_PER_OP = 2
        self._configure(mds_dir_keys_per_op=self.KEYS_PER_OP,
                        mds_dir_prefetch_backend=False)
        self._wait_for_config('mds_dir_keys_per_op', 2)
        self.fs.set_allow_new_snaps(True)
        dirname = 'version_boundaries'
        name = 'same_name'
        snapshots = ['s%d' % i for i in range(6)]
        self._create_files(dirname, 3, prefix='aaa')
        self._create_files(dirname, 3, prefix='zzz')
        versions = []
        made = []
        try:
            for i in range(len(snapshots) + 1):
                if i:
                    self.mount_a.run_shell(['rm', f'{dirname}/{name}'])
                content = 'incarnation %d' % i
                self.mount_a.write_file(f'{dirname}/{name}', content)
                versions.append((self.mount_a.path_to_ino(f'{dirname}/{name}'),
                                 content))
                if i < len(snapshots):
                    self.mount_a.run_shell(
                        ['mkdir', f'{dirname}/.snap/{snapshots[i]}'])
                    made.append(snapshots[i])
            self.assertEqual(len({ino for ino, _ in versions}), len(versions))

            ino = self.mount_a.path_to_ino(dirname)
            self.fs.flush()
            snap_ids = {snap['name']: snap['snapid'] for snap in
                        self.fs.rank_asok(['dump', 'snaps', '--server'])['snaps']}
            keys = self.fs.radosmo(
                ['listomapkeys', f'{ino:x}.00000000'], stdout=StringIO()).splitlines()
            keys = sorted(keys)
            positions = [i for i, key in enumerate(keys)
                         if key.rsplit('_', 1)[0] == name]
            self.assertEqual(len(positions), len(versions), keys)
            self.assertIn(name + '_head', keys)
            self.assertGreaterEqual(len({i // 2 for i in positions}), 3,
                                    'same-name history did not cross batches')
            expected = sorted(['aaa_%06d' % i for i in range(3)] +
                              ['zzz_%06d' % i for i in range(3)] + [name])

            for mode in (True, False):
                with self.subTest(pipelined=mode):
                    self._configure(mds_dir_fetch_pipelined=mode)
                    self._check_readdir_after_fetch(dirname, expected,
                                                    pipelined=mode)
                    # FUSE can embed a snapshot tag in stat's inode number.
                    # Compare the recovered server-side identities instead.
                    loaded = {dn['snap_last']: dn['inode']
                              for dn in self._dump_dir(dirname)['dentries']
                              if dn['path'].rsplit('/', 1)[-1] == name}
                    self.assertEqual(len(loaded), len(versions), loaded)
                    for snap, (expected_ino, _) in zip(snapshots, versions):
                        self.assertEqual(loaded[snap_ids[snap]], expected_ino, loaded)
                    self.assertEqual(loaded[max(loaded)], versions[-1][0], loaded)
                    paths = [f'{dirname}/.snap/{snap}' for snap in snapshots]
                    paths.append(dirname)
                    for path, (_, content) in zip(paths, versions):
                        self.assertEqual(self._listdir(path), expected)
                        self.assertEqual(self.mount_a.read_file(f'{path}/{name}'),
                                         content, path)
        finally:
            for snap in reversed(made):
                self.mount_a.run_shell(['rmdir', f'{dirname}/.snap/{snap}'])

    def test_background_fetch_with_named_reads(self):
        """Named reads finish between batches, even when full fetch wins."""
        self._configure(mds_dir_fetch_pipelined=True,
                        mds_dir_prefetch=False,
                        mds_dir_prefetch_backend=False,
                        mds_dir_prefetch_backend_hit_threshold=1,
                        mds_dir_prefetch_backend_max=1)
        dirname = 'background_fetch'
        self._create_files(dirname, self.NFILES)
        self._drop_caches()
        self.mount_a.run_shell(['stat', dirname])
        self._configure(mds_dir_prefetch_backend=True)
        before = self.fs.rank_asok(['perf', 'dump', 'mds'])['mds']
        self._configure(mds_inject_dir_fetch_delay=10000)
        first = second = missing = None
        try:
            first = self.mount_a.run_shell(
                ['stat', f'{dirname}/file_000000'], wait=False)

            def partially_loaded():
                dump = self._dump_dir(dirname)
                return ('complete' not in dump.get('states', []) and
                        self.KEYS_PER_OP <= len(dump.get('dentries', [])) <
                        self.NFILES)

            self.wait_until_true(partially_loaded, timeout=90, period=1)
            self.assertEqual(len(self._dump_dir(dirname)['dentries']),
                             self.KEYS_PER_OP,
                             'must issue the named read before the second batch')
            # Request the next delayed batch's first name so it takes the keyed
            # waiter; the lookup must finish before the remaining scan.
            second = self.mount_a.run_shell(
                ['stat', f'{dirname}/file_{self.KEYS_PER_OP:06d}'], wait=False)
            missing = self._stat_missing(f'{dirname}/missing', wait=False)
            self.wait_until_true(lambda: second.finished, timeout=45, period=1)
            second.wait()
            missing.wait()
            self.assertNotIn('complete', self._dump_dir(dirname)['states'])
        finally:
            self._configure(mds_inject_dir_fetch_delay=0)
            for proc in (first, second, missing):
                if proc is not None:
                    proc.wait()
        self.assertEqual(self._listdir(dirname),
                         ['file_%06d' % i for i in range(self.NFILES)])
        after = self.fs.rank_asok(['perf', 'dump', 'mds'])['mds']
        self.assertEqual(after['dir_fetch_background'] -
                         before['dir_fetch_background'], 1)
        self.assertEqual(after['dir_fetch_complete'] -
                         before['dir_fetch_complete'], 1)

        # A second fetch proves the first released num_backend_fetching.
        self._configure(mds_dir_prefetch_backend=False)
        self._drop_caches()
        self.mount_a.run_shell(['stat', dirname])
        self._configure(mds_dir_prefetch_backend=True)
        before = self.fs.rank_asok(['perf', 'dump', 'mds'])['mds']
        self.mount_a.run_shell(['stat', f'{dirname}/file_000042'])
        self.wait_until_true(
            lambda: 'complete' in self._dump_dir(dirname).get('states', []),
            timeout=60)
        after = self.fs.rank_asok(['perf', 'dump', 'mds'])['mds']
        self.assertEqual(after['dir_fetch_background'] -
                         before['dir_fetch_background'], 1)
        self.assertEqual(after['dir_fetch_complete'] -
                         before['dir_fetch_complete'], 1)


class TestDirfragFetchRejoin(DirfragFetchLogMixin, CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 2

    KEYS_PER_OP = 4
    NENTRIES = 32

    def _inode(self, ino, rank=0):
        dump = self.fs.rank_asok(['dump', 'inode', str(ino)], rank=rank)
        return dump if isinstance(dump, dict) else {}

    def test_rejoin_undef_inodes_with_interleaved_keyed_fetch(self):
        """Recover placeholders per batch while another dirfrag fetches keys."""
        self._configure(mds_bal_fragment_dirs=False,
                        mds_dir_keys_per_op=self.KEYS_PER_OP,
                        mds_dir_prefetch=True,
                        mds_dir_prefetch_backend=False,
                        mds_dir_fetch_pipelined=True)
        self.fs.set_max_mds(2)
        self.fs.wait_for_daemons()
        for rank in (0, 1):
            for key, value in (('mds_bal_fragment_dirs', False),
                               ('mds_dir_keys_per_op', self.KEYS_PER_OP),
                               ('mds_dir_prefetch', True)):
                self._wait_for_config(key, value, rank=rank)

        dirname = 'rejoin_batches'
        probe = 'rejoin_keyed'
        self.mount_a.run_shell(['mkdir', dirname, probe, 'survivor'])
        self.mount_a.setfattr(dirname, 'ceph.dir.pin', '0')
        self.mount_a.setfattr(probe, 'ceph.dir.pin', '0')
        self.mount_a.setfattr('survivor', 'ceph.dir.pin', '1')
        self.mount_a.write_file('survivor/keep', 'survivor')
        for name in ('present_a', 'present_b'):
            self.mount_a.write_file(f'{probe}/{name}', 'keyed payload')
        expected = json.loads(self.mount_a.run_python(f"""
import json
import os
d = os.path.join({self.mount_a.hostfs_mntpt!r}, {dirname!r})
entries = {{}}
for i in range({self.NENTRIES}):
    name = 'entry_%06d' % i
    path = os.path.join(d, name)
    if i % 4 == 0:
        os.mkdir(path)
    else:
        with open(path, 'w') as f:
            f.write(name)
    st = os.stat(path)
    entries[name] = {{'ino': st.st_ino, 'mode': st.st_mode,
                     'content': None if i % 4 == 0 else name}}
print(json.dumps(entries))
"""))
        self._wait_subtrees([('/' + dirname, 0), ('/' + probe, 0),
                             ('/survivor', 1)], rank=0)
        full_ino = self.mount_a.path_to_ino(dirname)
        probe_ino = self.mount_a.path_to_ino(probe)
        requests = []
        delay_armed = False
        try:
            # These are clean replicas held by internal requests, not client
            # caps that process_imported_caps() would recover before rejoin.
            for name in sorted(expected):
                op = self.fs.rank_asok(
                    ['lock', 'path', f'/{dirname}/{name}', 'policy:r', '--await'],
                    rank=1)
                requests.append((1, self._reqid_tostr(op['reqid'])))
                self.assertEqual(op['result'], 0, op)
            replicas = self._dump_dir(dirname, rank=1)
            self.assertEqual({dn['inode'] for dn in replicas['dentries']},
                             {item['ino'] for item in expected.values()})
            self.mount_a.umount_wait()
            for rank in (0, 1):
                self.wait_until_true(
                    lambda rank=rank: not self.fs.rank_asok(['session', 'ls'], rank=rank),
                    timeout=90, period=1)
                self.fs.flush(rank=rank)

            old_auth = self.fs.get_rank(rank=0)
            survivor = self.fs.get_rank(rank=1)
            # Use persistent settings so the first rejoin fetch is delayed.
            self._config_set('mds_dir_fetch_pipelined', True)
            self._config_set('mds_dir_prefetch_backend', False)
            self._config_set('mds_inject_dir_fetch_delay', 10000)
            delay_armed = True
            self.fs.rank_signal(signal.SIGKILL, rank=0)
            self.fs.rank_fail(rank=0)
            self.fs.mds_restart(old_auth['name'])
            self.fs.wait_for_state('up:rejoin', rank=0, timeout=180)
            self.assertNotEqual(self.fs.get_rank(rank=0)['gid'], old_auth['gid'])
            self.assertEqual(self.fs.get_rank(rank=1)['gid'], survivor['gid'])

            full_prefix = f'.cache.dir(0x{full_ino:x}) '
            probe_prefix = f'.cache.dir(0x{probe_ino:x}) '
            first = expected['entry_000000']['ino']
            last = expected['entry_%06d' % (self.NENTRIES - 1)]['ino']
            with self._capture_mds_log(level=12) as read_log:
                # Dentry counts cannot measure progress: all the placeholders
                # already exist before any of the full fetch's replies land.
                self.wait_until_true(
                    lambda: 'rejoinundef' in self._inode(first).get('states', [])
                    and 'rejoinundef' in self._inode(last).get('states', []),
                    timeout=90, period=0.5)

                def partial_recovery():
                    early = self._inode(first)
                    late = self._inode(last)
                    return (bool(early) and 'rejoinundef' not in early.get('states', []) and
                            'rejoinundef' in late.get('states', []))

                self.wait_until_true(partial_recovery, timeout=90, period=0.5)
                self.assertNotIn('complete', self._dump_dir(dirname)['states'])
                self.assertEqual(self._inode(first)['pins'].get('dirfetchundef', 0), 0)
                self.assertEqual(self.fs.get_rank(rank=0)['state'], 'up:rejoin')

                # Use another dirfrag because full fetches serialize same-dirfrag reads.
                # Internal requests can traverse before the client dispatcher is active.
                self.fs.rank_asok(['config', 'set', 'mds_dir_prefetch', 'false'])
                probe_dump = self._dump_dir(probe)
                self.assertEqual(probe_dump.get('dentries', []), [], probe_dump)
                for name in ('present_a', 'present_b'):
                    op = self.fs.rank_asok(
                        ['lock', 'path', f'/{probe}/{name}', 'policy:r'])
                    requests.append((0, self._reqid_tostr(op['reqid'])))

                def keyed_replies():
                    lines = [line for line in read_log() if probe_prefix in line]
                    return sum('_fetched 1 keys for ' in line for line in lines) == 2

                self.wait_until_true(keyed_replies, timeout=90, period=0.5)
                self.assertNotIn('complete', self._dump_dir(dirname)['states'])
                self.assertEqual(self.fs.get_rank(rank=0)['state'], 'up:rejoin')
                self._configure(mds_inject_dir_fetch_delay=0)
                self.fs.wait_for_state('up:active', rank=0, timeout=120)
                lines = read_log()
                batches = [i for i, line in enumerate(lines)
                           if full_prefix in line and
                           f'_fetched {self.KEYS_PER_OP} keys for ' in line]
                keyed = [i for i, line in enumerate(lines)
                         if probe_prefix in line and '_fetched ' in line
                         and ' keys for ' in line]
                self.assertGreaterEqual(len(batches), 2, lines)
                self.assertEqual(len(keyed), 2, lines)
                self.assertLess(batches[0], min(keyed), lines)
                self.assertLess(max(keyed), batches[-1], lines)

            self.assertEqual(self.fs.get_rank(rank=1)['gid'], survivor['gid'])
            self.assertIn('complete', self._dump_dir(dirname)['states'])
            for item in expected.values():
                inode = self._inode(item['ino'])
                self.assertNotIn('rejoinundef', inode['states'], inode)
                self.assertEqual(inode['pins'].get('dirfetchundef', 0), 0, inode)
            self.mount_a.mount_wait()
            self.assertEqual(self._listdir(dirname), sorted(expected))
            actual = json.loads(self.mount_a.run_python(f"""
import json
import os
d = os.path.join({self.mount_a.hostfs_mntpt!r}, {dirname!r})
entries = {{}}
for name in os.listdir(d):
    path = os.path.join(d, name)
    st = os.stat(path)
    content = None
    if not os.path.isdir(path):
        with open(path) as f:
            content = f.read()
    entries[name] = {{'ino': st.st_ino, 'mode': st.st_mode, 'content': content}}
print(json.dumps(entries))
"""))
            self.assertEqual(actual, expected)
            self.assertEqual(self._listdir(probe), ['present_a', 'present_b'])
            for name in ('present_a', 'present_b'):
                self.assertEqual(self.mount_a.read_file(f'{probe}/{name}'), 'keyed payload')
        finally:
            if delay_armed:
                self.config_set('mds', 'mds_inject_dir_fetch_delay', 0)
                self._configure(mds_inject_dir_fetch_delay=0)
            for rank, reqid in reversed(requests):
                self.fs.rank_tell(['op', 'kill', reqid], rank=rank, check_status=False)


class TestDirfragCommitFetchRace(DirfragFetchLogMixin, CephFSTestCase):
    """Verify buffered restarts and pipelined snapshot purging across a racing commit."""

    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 1

    KEYS_PER_OP = 16
    NFILES = 200
    # Unlink every Nth file while a snapshot retains its on-disk dentry.
    VICTIM_STRIDE = 10

    FETCH_DELAY_MS = 10000

    def setUp(self):
        super(TestDirfragCommitFetchRace, self).setUp()
        self._configure(mds_bal_fragment_dirs=False,
                        mds_dir_keys_per_op=self.KEYS_PER_OP,
                        mds_dir_prefetch=False,
                        mds_dir_fetch_pipelined=True)
        self.fs.set_allow_new_snaps(True)


    def _read_header(self, obj):
        raw = self.fs.radosmo(['getomapheader', obj, '-'], stdout=BytesIO())
        return json.loads(self.fs.dencoder('fnode_t', raw))

    def _snap_server_dump(self):
        return self.fs.rank_asok(["dump", "snaps", "--server"])

    def _dirfrag_obj(self, dirpath):
        ino = self.mount_a.path_to_ino(dirpath)
        return "{0:x}.00000000".format(ino)

    def _omap_keys(self, obj):
        out = self.fs.radosmo(["listomapkeys", obj], stdout=StringIO())
        return [k for k in out.split("\n") if k]

    def _victim_snap_keys(self, obj, victims):
        """Find victim snapshot keys (name_<hex-snapid>), excluding head keys."""
        out = []
        for k in self._omap_keys(obj):
            if k.endswith("_head"):
                continue
            name = k.rsplit("_", 1)[0]
            if name in victims:
                out.append(k)
        return out

    def _drop_a_and_mds_cache(self):
        """Drop client A and MDS caches; client B pins keep the dirfrag incomplete."""
        self.mount_a.umount_wait()
        self.fs.flush()
        self.fs.rank_tell(['cache', 'drop'])
        self.mount_a.mount_wait()


    def test_buffered_fetch_restarts_after_commit(self):
        """A real commit between batches must still restart a buffered fetch."""
        self._configure(mds_dir_fetch_pipelined=False)
        dirname = 'buffered_commit_race'
        kept = 'keep'
        expected = ['file_%06d' % i for i in range(self.NFILES)] + [kept]
        self.mount_a.run_shell(['mkdir', dirname])
        self.mount_a.run_python(f"""
import os
d = os.path.join({self.mount_a.hostfs_mntpt!r}, {dirname!r})
for name in {expected!r}:
    open(os.path.join(d, name), 'w').close()
""")
        ino = self.mount_a.path_to_ino(dirname)
        bg = self.mount_b.open_background(basename=f'{dirname}/{kept}')
        try:
            self._drop_a_and_mds_cache()
            cold = self._dump_dir(dirname)
            self.assertNotIn('complete', cold['states'])
            committed_before = int(cold['committed_version'])
            self.assertGreater(committed_before, 0)
            self.assertEqual(int(cold['committing_version']), committed_before)

            with self._capture_fetch_log(ino) as read_log:
                self._configure(mds_inject_dir_fetch_delay=self.FETCH_DELAY_MS)
                listing = None
                try:
                    listing = self.mount_a.run_shell(
                        ['ls', '-1', dirname], wait=False)
                    # The buffering log and locked dump prove the first callback ran;
                    # the delay holds later callbacks outside mds_lock.
                    self.wait_until_true(
                        lambda: any('_omap_fetch_start buffering header ' in line
                                    for line in read_log()),
                        timeout=60, period=2)
                    before = self._dump_dir(dirname)
                    self.assertNotIn('complete', before['states'])
                    self.assertFalse(listing.finished)
                    self.assertEqual(int(before['committed_version']),
                                     committed_before)
                    self.assertFalse(any('while fetching at v' in line
                                         for line in read_log()))

                    # A runtime toggle must not change this fetch's buffered
                    # mode, including the restart caused by the commit below.
                    self._configure(mds_dir_fetch_pipelined=True)

                    # The pinned inode is already cached, so dirtying it does
                    # not block on the full fetch's WAIT_COMPLETE.
                    self.mount_b.run_python(f"""
import os
fd = os.open(os.path.join({self.mount_b.hostfs_mntpt!r},
                          {dirname!r}, {kept!r}), os.O_RDONLY)
try:
    mode = os.fstat(fd).st_mode & 0o777
    os.fchmod(fd, mode ^ 0o100)
    os.fsync(fd)
finally:
    os.close(fd)
""", timeout=300)
                    self.fs.flush()
                    during = self._dump_dir(dirname)
                    self.assertNotIn('complete', during['states'])
                    self.assertFalse(listing.finished)
                    self.assertGreater(int(during['committed_version']),
                                       committed_before)
                finally:
                    self._configure(mds_inject_dir_fetch_delay=0)
                    if listing is not None:
                        listing.wait()
                lines = read_log()

            self.assertEqual(sorted(listing.stdout.getvalue().splitlines()),
                             sorted(expected))
            self.assertIn('complete', self._dump_dir(dirname)['states'])
            restart = 'while fetching at v{}, restarting'.format(committed_before)
            self.assertTrue(any(restart in line for line in lines),
                            'buffered fetch ignored the racing commit')
            self.assertGreaterEqual(
                sum('_omap_fetch_start buffering header ' in line for line in lines), 2,
                'restart must actually reread the header')
        finally:
            self.mount_b._kill_background(bg)

    def test_stale_items_not_leaked_on_commit_fetch_race(self):
        dirname = "victimdir"
        kept = "keep_000000"          # non-victim, pinned open by client B

        self.mount_a.run_shell(["mkdir", "-p", dirname])
        self.mount_a.run_python(f"""
import os
d = os.path.join("{self.mount_a.hostfs_mntpt}", "{dirname}")
open(os.path.join(d, "{kept}"), "w").close()
for i in range({self.NFILES}):
    open(os.path.join(d, "file_%06d" % i), "w").close()
""")

        victims = set("file_%06d" % i
                      for i in range(0, self.NFILES, self.VICTIM_STRIDE))

        self.mount_a.run_shell(["mkdir", f"{dirname}/.snap/s1"])
        for name in sorted(victims):
            self.mount_a.run_shell(["rm", "-f", f"{dirname}/{name}"])

        # Persist snapshot dentries before they become stale.
        obj = self._dirfrag_obj(dirname)
        self.fs.flush()
        self.assertGreater(
            len(self._victim_snap_keys(obj, victims)), 0,
            "expected victim snap-dentries on disk after snapshotting+unlink")

        # Pin a dentry to allow dirtying during fetch without WAIT_COMPLETE.
        bg = self.mount_b.open_background(basename=f"{dirname}/{kept}")
        try:
            # Make the dirfrag incomplete before destroying the snapshot,
            # deferring stale-snap purging to the fetch path.
            self._drop_a_and_mds_cache()

            last_destroyed0 = int(self._snap_server_dump()["last_destroyed"])
            self.mount_b.run_shell(["rmdir", f"{dirname}/.snap/s1"])
            self.wait_until_true(
                lambda: len(self._snap_server_dump()["pending_destroy"]) == 0,
                timeout=60)
            self.wait_until_true(
                lambda: int(self._snap_server_dump()["last_destroyed"]) > last_destroyed0,
                timeout=60)

            # Require stale keys on disk so the leak check cannot pass vacuously.
            self.assertGreater(
                len(self._victim_snap_keys(obj, victims)), 0,
                "victim snap-dentries were purged before the race could run")

            purge_before = int(self._read_header(obj)['snap_purged_thru'])
            ino = self.mount_a.path_to_ino(dirname)
            with self._capture_fetch_log(ino) as read_log:
                self._drive_race(dirname, kept, obj, purge_before)
                lines = read_log()
            self.assertTrue(any('while fetching at v' in line and
                                ', continuing' in line for line in lines))
            self.assertFalse(any(', restarting' in line for line in lines))

            # Flush must persist stale-key removals discovered after the racing commit.
            self._configure(mds_inject_dir_fetch_delay=0)
            self.fs.flush()

            leaked = self._victim_snap_keys(obj, victims)
            self.assertEqual(
                leaked, [],
                "stale snap-dentries leaked on disk after commit/fetch race: "
                "{0}".format(leaked))
        finally:
            self._configure(mds_inject_dir_fetch_delay=0)
            self.mount_b._kill_background(bg)

    def _drive_race(self, dirname, kept, obj, purge_before):
        self._configure(mds_inject_dir_fetch_delay=0)
        self._drop_a_and_mds_cache()
        cold = self._dump_dir(dirname)
        self.assertNotIn('complete', cold['states'])
        cached_paths = {dn['path'] for dn in cold.get('dentries', [])}
        self._configure(mds_inject_dir_fetch_delay=self.FETCH_DELAY_MS)

        listing = self.mount_a.run_shell(['ls', '-1', dirname], wait=False)
        try:
            first_batch = {}

            def decoded_partial_batch():
                dump = self._dump_dir(dirname)
                paths = {dn['path'] for dn in dump.get('dentries', [])}
                if 'complete' in dump['states'] or not paths - cached_paths:
                    return False
                first_batch.update(dump)
                return True

            self.wait_until_true(decoded_partial_batch, timeout=60, period=2)
            self.assertFalse(listing.finished)
            committed_before = int(first_batch['committed_version'])

            # Use a cached, pinned inode so this does not wait for readdir.
            # fsync orders the new mode's cap flush before the journal flush.
            self.mount_b.run_python(f"""
import os
fd = os.open(os.path.join({self.mount_b.hostfs_mntpt!r},
                          {dirname!r}, {kept!r}), os.O_RDONLY)
try:
    os.fchmod(fd, 0o600)
    os.fsync(fd)
finally:
    os.close(fd)
""", timeout=300)
            self.fs.flush()

            # Global perf counters cannot prove this ordering: inspect the
            # target itself and reject a commit that only finished after EOF.
            during = self._dump_dir(dirname)
            self.assertNotIn('complete', during['states'])
            self.assertFalse(listing.finished)
            self.assertGreater(int(during['committed_version']), committed_before)
            header = self._read_header(obj)
            self.assertEqual(int(header['snap_purged_thru']), purge_before,
                             'commit published purge progress before fetch EOF')
        finally:
            self._configure(mds_inject_dir_fetch_delay=0)
            listing.wait()

        expected = {"file_%06d" % i for i in range(self.NFILES)
                    if i % self.VICTIM_STRIDE != 0}
        expected.add(kept)
        self.assertEqual(set(listing.stdout.getvalue().splitlines()), expected)

    def test_namespace_ops_wait_for_readdir_fetch(self):
        """Verify mutations wait for readdir's filelock, then survive a cold fetch."""
        for operation in ('unlink', 'rename'):
            with self.subTest(operation=operation):
                self._check_namespace_op_waits_for_readdir_fetch(operation)

    def _check_namespace_op_waits_for_readdir_fetch(self, operation):
        self._configure(mds_dir_prefetch_backend=False,
                        mds_enable_op_tracker=True)
        self._wait_for_config('mds_enable_op_tracker', True)
        dirname = 'namespace_ops_' + operation
        unlinked = 'file_000190'        # beyond the cursor, pinned by B
        rename_src = 'file_000195'      # beyond the cursor, pinned by B
        rename_dst = 'aaa_dest'         # before the cursor, pinned by B
        created = 'zzz_new'             # does not exist on disk at all

        names = ['file_%06d' % i for i in range(self.NFILES)] + [rename_dst]
        self.mount_a.run_shell(['mkdir', dirname])
        self.mount_a.run_python(f"""
import os
d = os.path.join({self.mount_a.hostfs_mntpt!r}, {dirname!r})
for name in {names!r}:
    open(os.path.join(d, name), 'w').close()
""")

        pinned = [self.mount_b.open_background(basename=f'{dirname}/{name}',
                                               write=False)
                  for name in (unlinked, rename_src, rename_dst)]
        try:
            self._drop_a_and_mds_cache()
            cold = self._dump_dir(dirname)
            self.assertNotIn('complete', cold['states'])
            cached = {dn['path'].rsplit('/', 1)[-1]
                      for dn in cold.get('dentries', [])}
            self.assertTrue({unlinked, rename_src, rename_dst} <= cached,
                            "client B must keep the victims cached, have "
                            "{0}".format(sorted(cached)))

            ino = self.mount_a.path_to_ino(dirname)
            with self._capture_mds_log() as read_log:
                self._configure(mds_inject_dir_fetch_delay=self.FETCH_DELAY_MS)
                listing = None
                ops = []
                try:
                    listing = self.mount_a.run_shell(['ls', '-1', dirname],
                                                     wait=False)

                    def decoded_partial_batch():
                        dump = self._dump_dir(dirname)
                        have = {dn['path'].rsplit('/', 1)[-1]
                                for dn in dump.get('dentries', [])}
                        fresh = have - cached
                        if 'complete' in dump['states'] or not fresh:
                            return False
                        # the victims have to be ahead of the cursor still
                        return max(fresh) < unlinked

                    self.wait_until_true(decoded_partial_batch, timeout=60,
                                         period=2)
                    self.assertFalse(listing.finished)

                    # Run separately: the client can serialize operations on
                    # one directory before the second request reaches the MDS.
                    if operation == 'unlink':
                        args = ['rm', '-f', f'{dirname}/{unlinked}']
                        target = unlinked
                    else:
                        args = ['mv', f'{dirname}/{rename_src}',
                                f'{dirname}/{rename_dst}']
                        target = rename_src
                    ops.append(self.mount_b.run_shell(args, wait=False))
                    observed = {}

                    def mutations_wait_for_locks():
                        requests = self.fs.get_ops(locks=True)['ops']
                        observed['requests'] = requests
                        waiting = False
                        readdir_holds_filelock = False
                        for request in requests:
                            desc = request['description']
                            data = request['type_data']
                            if ' readdir ' in desc:
                                readdir_holds_filelock |= any(
                                    lock['lock'].get('type') == 'ifile' and
                                    lock['flags'] & 1 and
                                    lock['object_string'].startswith(f'[inode 0x{ino:x} ')
                                    for lock in data.get('locks', []))
                            if (f' {operation} ' in desc and target in desc and
                                    data['flag_point'] == 'failed to wrlock, waiting'):
                                waiting = True
                        return readdir_holds_filelock and waiting

                    self.wait_until_true(mutations_wait_for_locks, timeout=60, period=1)
                    # Request states establish arrival at the MDS. Also tie the
                    # write-lock wait to this directory, not an unrelated lock.
                    lines = read_log()
                    self.assertTrue(any(
                        'wrlock_start waiting on (ifile ' in line and
                        f'on [inode 0x{ino:x} ' in line for line in lines),
                        observed)
                    self.assertNotIn('complete', self._dump_dir(dirname)['states'])
                    self.assertFalse(listing.finished)
                    self.assertFalse(ops[0].finished, observed)
                    ops.append(self.mount_b.run_shell(
                        ['touch', f'{dirname}/{created}'], wait=False))
                finally:
                    self._configure(mds_inject_dir_fetch_delay=0)
                    pending = ops + ([listing] if listing is not None else [])
                    self.wait_until_true(lambda: all(proc.finished for proc in pending),
                                         timeout=120, period=1)
                    for proc in pending:
                        proc.wait()

            expected = {'file_%06d' % i for i in range(self.NFILES)}
            expected.add(rename_dst)
            expected.remove(unlinked if operation == 'unlink' else rename_src)
            expected.add(created)

            # Concurrent ls may span multiple readdir requests; only a fresh
            # listing after all mutations has a defined view.
            self.assertIn('complete', self._dump_dir(dirname)['states'])
            self.assertEqual(set(self._listdir(dirname)), expected)

            self.fs.flush()
            self._drop_a_and_mds_cache()
            self.assertEqual(set(self._listdir(dirname)), expected)
        finally:
            for proc in pinned:
                self.mount_b._kill_background(proc)
