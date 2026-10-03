"""Test multi-batch dirfrag fetches in pipelined and buffered modes.

Fetch progress is observed through the dirfrag dump's "fetch" section,
present only while a full fetch is between replies, and through the MDS
dir_fetch_* perf counters. The counters are daemon-wide, so each test keeps
other dirfrags quiet while it measures one.
"""

import errno
import json
import signal
from io import BytesIO, StringIO

from tasks.cephfs.cephfs_test_case import CephFSTestCase


class DirfragFetchMixin:
    # Long enough to inspect the dirfrag between two replies.
    FETCH_DELAY_MS = 10000
    FETCH_COUNTERS = ('dir_fetch_complete', 'dir_fetch_keys',
                      'dir_fetch_background', 'dir_fetch_restart',
                      'dir_fetch_continue')

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

    def _fetch_counters(self, rank=0):
        mds = self.fs.rank_asok(['perf', 'dump', 'mds'], rank=rank)['mds']
        return {k: mds[k] for k in self.FETCH_COUNTERS}

    def _fetch_delta(self, before, rank=0):
        after = self._fetch_counters(rank=rank)
        return {k: after[k] - before[k] for k in before}

    def _wait_for_fetch(self, dirname, ready=lambda fetch: True, rank=0,
                        timeout=60):
        """Wait until a full fetch of dirname is between replies; return its dump."""
        seen = {}

        def between_replies():
            # A cold lookup may not have loaded the directory inode yet.
            dirs = self.fs.rank_asok(['dump', 'dir', '/' + dirname, 'true'],
                                     rank=rank)
            if not dirs:
                return False
            self.assertEqual(len(dirs), 1, "test directory must have one dirfrag")
            dump = dirs[0]
            if 'fetch' not in dump or not ready(dump['fetch']):
                return False
            seen.update(dump)
            return True

        self.wait_until_true(between_replies, timeout=timeout, period=1)
        return seen

    def _list_during_fetch(self, dirname, ready=lambda fetch: True):
        """List a cold dirfrag with delayed replies; return (listing, dump seen)."""
        self._configure(mds_inject_dir_fetch_delay=self.FETCH_DELAY_MS)
        proc = None
        try:
            proc = self.mount_a.run_shell(['ls', '-1', dirname], wait=False)
            seen = self._wait_for_fetch(dirname, ready)
            self.assertFalse(proc.finished)
            self.assertNotIn('complete', seen['states'])
        finally:
            self._configure(mds_inject_dir_fetch_delay=0)
            if proc is not None:
                proc.wait()
        return sorted(proc.stdout.getvalue().splitlines()), seen

    def _inode(self, ino, rank=0):
        dump = self.fs.rank_asok(['dump', 'inode', str(ino)], rank=rank)
        return dump if isinstance(dump, dict) else {}

    @staticmethod
    def _names(dump, null=False):
        return {dn['path'].rsplit('/', 1)[-1] for dn in dump.get('dentries', [])
                if dn['is_null'] == null}


class TestDirfragFetch(DirfragFetchMixin, CephFSTestCase):
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
        self._drop_caches()
        if pipelined is None:
            listing = self._listdir(dirname)
        elif pipelined:
            # Decoding before EOF is what distinguishes the pipeline.
            listing, seen = self._list_during_fetch(
                dirname, lambda fetch: fetch['decoded_keys'] > 0)
            self.assertTrue(seen['fetch']['pipelined'], seen)
            self.assertEqual(seen['fetch']['buffered_keys'], 0, seen)
            self.assertTrue(self._names(seen), seen)
        else:
            # Two replies in, every key must still be buffered, not decoded.
            listing, seen = self._list_during_fetch(
                dirname, lambda fetch: fetch['replies'] >= 2)
            self.assertFalse(seen['fetch']['pipelined'], seen)
            self.assertEqual(seen['fetch']['decoded_keys'], 0, seen)
            self.assertGreater(seen['fetch']['buffered_keys'],
                               self.KEYS_PER_OP, seen)
            self.assertEqual(self._names(seen), set(), seen)
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

        before = self._fetch_counters()
        if pipelined:
            listing = self._listdir(dirname)
        else:
            # Observe the first buffered reply before EOF: adopting its
            # header here would mutate the cold dirfrag prematurely.
            listing, during = self._list_during_fetch(dirname)
            self.assertFalse(during['fetch']['pipelined'], during)
            for field in ('version', 'committing_version', 'committed_version'):
                self.assertEqual(int(during[field]), 0)
            self.assertEqual(during['dentries'], [])
        delta = self._fetch_delta(before)

        self.assertEqual(listing, ['file_%06d' % i for i in range(self.NFILES)])
        loaded = self._dump_dir(dirname)
        self.assertIn('complete', loaded['states'])
        self.assertGreater(int(loaded['committed_version']), 0)
        # One full fetch, with no keyed read or restart rereading the header.
        self.assertEqual(delta['dir_fetch_complete'], 1, delta)
        self.assertEqual(delta['dir_fetch_keys'], 0, delta)
        self.assertEqual(delta['dir_fetch_restart'], 0,
                         'adopting the disk version was mistaken for a commit')
        self.assertEqual(delta['dir_fetch_continue'], 0,
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
        self._drop_caches()
        self.mount_a.run_shell(['stat', dirname])
        self.assertEqual(self._dump_dir(dirname).get('dentries', []), [])
        self._configure(mds_dir_prefetch_backend=True)
        before = self._fetch_counters()
        self.mount_a.run_shell(['stat', f'{dirname}/file_000042'])
        self.wait_until_true(
            lambda: 'complete' in self._dump_dir(dirname).get('states', []),
            timeout=60)
        delta = self._fetch_delta(before)
        # Buffered mode preserves the original version check: a keyed fetch
        # adopting the fnode can cause the full fetch to restart from v0.
        if pipelined:
            self.assertEqual(delta['dir_fetch_continue'], 0,
                             'adopting the disk fnode was mistaken for a commit')
        self.assertEqual(delta['dir_fetch_keys'], 1, delta)
        self.assertEqual(delta['dir_fetch_background'], 1, delta)
        self.assertEqual(delta['dir_fetch_complete'], 1, delta)
        self.assertEqual(self._listdir(dirname),
                         ['file_%06d' % i for i in range(self.NFILES)])

    def test_cold_background_fetch_version_pipelined(self):
        self._check_cold_background_fetch_version(pipelined=True)

    def test_cold_background_fetch_version_not_pipelined(self):
        self._check_cold_background_fetch_version(pipelined=False)

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
                before = self._fetch_counters()
                # libcephfs can infer completeness from empty directory
                # stats. Traverse on the MDS to bypass that client shortcut.
                # --await supplies the finisher needed for an ENOENT reply.
                result = self.fs.rank_tell(
                    ['lock', 'path', f'/{dirname}/missing', 'policy:r', '--await'],
                    check_status=False)
                self.assertEqual(result['result'], -errno.ENOENT, result)
                delta = self._fetch_delta(before)
                # Prefetch turns the lookup into a full fetch of an empty OMAP.
                self.assertEqual(delta['dir_fetch_complete'], 1, delta)
                self.assertEqual(delta['dir_fetch_keys'], 0, delta)
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

        for mode in (True, False):
            with self.subTest(pipelined=mode):
                self._configure(mds_dir_fetch_pipelined=mode)
                self._drop_caches()
                self.mount_a.run_shell(['stat', dirname])
                cold = self._dump_dir(dirname)
                if cold.get('status') != 'dirfrag not in cache':
                    self.assertNotIn('complete', cold['states'])
                self.assertEqual(cold.get('dentries', []), [])
                before = self._fetch_counters()
                self.mount_a.run_shell(['stat', f'{dirname}/file_000042'])
                self._stat_missing(f'{dirname}/does_not_exist')
                delta = self._fetch_delta(before)
                self.assertEqual(delta['dir_fetch_keys'], 2, delta)
                self.assertEqual(delta['dir_fetch_complete'], 0, delta)
                keyed = self._dump_dir(dirname)
                self.assertNotIn('complete', keyed['states'])
                # The absent name is cached as a null dentry, not left unread.
                self.assertEqual(self._names(keyed), {'file_000042'}, keyed)
                self.assertEqual(self._names(keyed, null=True),
                                 {'does_not_exist'}, keyed)
                self.assertEqual(self._listdir(dirname),
                                 ['file_%06d' % i for i in range(self.NFILES)])

    def test_snapshot_versions_across_batches(self):
        """Snapshot dentries must survive batch boundaries.

        Snap dentries take a different branch in the decode loop and are
        the reason a batch is walked in reverse. Cover several incarnations
        of one name, and names that only a snapshot still holds.
        """
        self.KEYS_PER_OP = 2
        self._configure(mds_dir_keys_per_op=self.KEYS_PER_OP,
                        mds_dir_prefetch_backend=False)
        self._wait_for_config('mds_dir_keys_per_op', 2)
        self.fs.set_allow_new_snaps(True)
        dirname = 'version_boundaries'
        name = 'same_name'
        snapshots = ['s%d' % i for i in range(6)]
        # Unlinked after s0, so only s0 still holds them.
        unlinked = ['ggg_%06d' % i for i in range(3)]
        self._create_files(dirname, 3, prefix='aaa')
        self._create_files(dirname, 3, prefix='ggg')
        self._create_files(dirname, 3, prefix='zzz')
        versions = []
        made = []
        try:
            for i in range(len(snapshots) + 1):
                if i:
                    self.mount_a.run_shell(['rm', f'{dirname}/{name}'])
                if i == 1:
                    self.mount_a.run_shell(
                        ['rm'] + [f'{dirname}/{n}' for n in unlinked])
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
            listings = ([sorted(expected + unlinked)] +
                        [expected] * len(snapshots))

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
                    for path, listing, (_, content) in zip(paths, listings,
                                                           versions):
                        self.assertEqual(self._listdir(path), listing, path)
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
        self._configure(mds_inject_dir_fetch_delay=self.FETCH_DELAY_MS)
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


class TestDirfragFetchRejoin(DirfragFetchMixin, CephFSTestCase):
    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 2

    KEYS_PER_OP = 4
    NENTRIES = 32

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
            self._config_set('mds_inject_dir_fetch_delay', self.FETCH_DELAY_MS)
            delay_armed = True
            self.fs.rank_signal(signal.SIGKILL, rank=0)
            self.fs.rank_fail(rank=0)
            self.fs.mds_restart(old_auth['name'])
            self.fs.wait_for_state('up:rejoin', rank=0, timeout=180)
            self.assertNotEqual(self.fs.get_rank(rank=0)['gid'], old_auth['gid'])
            self.assertEqual(self.fs.get_rank(rank=1)['gid'], survivor['gid'])

            first = expected['entry_000000']['ino']
            last = expected['entry_%06d' % (self.NENTRIES - 1)]['ino']
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
            before = self._fetch_counters()
            for name in ('present_a', 'present_b'):
                op = self.fs.rank_asok(
                    ['lock', 'path', f'/{probe}/{name}', 'policy:r'])
                requests.append((0, self._reqid_tostr(op['reqid'])))

            keyed = {'present_a', 'present_b'}
            self.wait_until_true(
                lambda: keyed <= self._names(self._dump_dir(probe)),
                timeout=90, period=0.5)
            # The keyed reads finished between two replies of the full fetch.
            during = self._dump_dir(dirname)
            self.assertIn('fetch', during, 'full fetch finished before keyed reads')
            self.assertGreater(during['fetch']['decoded_keys'], 0, during)
            self.assertLess(during['fetch']['decoded_keys'], self.NENTRIES, during)
            self.assertNotIn('complete', during['states'])
            self.assertEqual(self._fetch_delta(before)['dir_fetch_keys'], 2)
            self.assertEqual(self.fs.get_rank(rank=0)['state'], 'up:rejoin')
            self._configure(mds_inject_dir_fetch_delay=0)
            self.fs.wait_for_state('up:active', rank=0, timeout=120)

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


class TestDirfragCommitFetchRace(DirfragFetchMixin, CephFSTestCase):
    """Verify buffered restarts and pipelined snapshot purging across a racing commit."""

    CLIENTS_REQUIRED = 2
    MDSS_REQUIRED = 1

    KEYS_PER_OP = 16
    NFILES = 200
    # Unlink every Nth file while a snapshot retains its on-disk dentry.
    VICTIM_STRIDE = 10

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
        bg = self.mount_b.open_background(basename=f'{dirname}/{kept}')
        try:
            self._drop_a_and_mds_cache()
            cold = self._dump_dir(dirname)
            self.assertNotIn('complete', cold['states'])
            committed_before = int(cold['committed_version'])
            self.assertGreater(committed_before, 0)
            self.assertEqual(int(cold['committing_version']), committed_before)

            counters = self._fetch_counters()
            self._configure(mds_inject_dir_fetch_delay=self.FETCH_DELAY_MS)
            listing = None
            try:
                listing = self.mount_a.run_shell(
                    ['ls', '-1', dirname], wait=False)
                # The dump proves the first callback ran under mds_lock; the
                # delay holds later callbacks outside it.
                before = self._wait_for_fetch(dirname)
                self.assertFalse(before['fetch']['pipelined'], before)
                self.assertEqual(int(before['fetch']['omap_version']),
                                 committed_before)
                self.assertFalse(listing.finished)
                self.assertEqual(int(before['committed_version']),
                                 committed_before)
                self.assertEqual(
                    self._fetch_delta(counters)['dir_fetch_restart'], 0)

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

                # The next reply sees the commit and resubmits the fetch at
                # the new version; its first reply is delayed as well.
                restarted = self._wait_for_fetch(
                    dirname,
                    lambda fetch: int(fetch['omap_version']) > committed_before)
                self.assertFalse(restarted['fetch']['pipelined'],
                                 'restart picked up the runtime toggle')
                self.assertFalse(listing.finished)
            finally:
                self._configure(mds_inject_dir_fetch_delay=0)
                if listing is not None:
                    listing.wait()

            self.assertEqual(sorted(listing.stdout.getvalue().splitlines()),
                             sorted(expected))
            self.assertIn('complete', self._dump_dir(dirname)['states'])
            delta = self._fetch_delta(counters)
            self.assertEqual(delta['dir_fetch_restart'], 1,
                             'buffered fetch ignored the racing commit')
            self.assertEqual(delta['dir_fetch_continue'], 0, delta)
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
            counters = self._fetch_counters()
            self._drive_race(dirname, kept, obj, purge_before)
            delta = self._fetch_delta(counters)
            self.assertEqual(delta['dir_fetch_continue'], 1, delta)
            self.assertEqual(delta['dir_fetch_restart'], 0, delta)

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
                # Request states establish arrival at the MDS. The only
                # conflicting lock is readdir's rdlock on this filelock.
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


class TestDirfragFetchTrim(DirfragFetchMixin, CephFSTestCase):
    """Exercise cache trimming between batches of a full dirfrag fetch."""

    CLIENTS_REQUIRED = 1
    MDSS_REQUIRED = 1

    NFILES = 64
    KEYS_PER_OP = 8

    def test_trim_between_fetch_batches(self):
        # Let the cache drop visit the whole LRU without throttling.
        self._configure(mds_bal_fragment_dirs=False,
                        mds_dir_fetch_pipelined=True,
                        mds_dir_keys_per_op=self.KEYS_PER_OP,
                        mds_cache_trim_threshold=1000000)
        self._wait_for_config('mds_dir_keys_per_op', self.KEYS_PER_OP)

        dirname = 'fetch_trim'
        expected = ['file_%06d' % i for i in range(self.NFILES)]
        self._create_files(dirname, self.NFILES)
        self._drop_caches()
        # Load ancestors without reading the target entries.
        self.mount_a.run_shell(['stat', dirname])
        cold = self._dump_dir(dirname)
        self.assertEqual(cold.get('dentries', []), [])
        self.assertNotIn('complete', cold.get('states', []))

        # Delay replies outside mds_lock so cache drop can finish during the scan.
        self._configure(mds_inject_dir_fetch_delay=self.FETCH_DELAY_MS)
        listing = None
        try:
            listing = self.mount_a.run_shell(['ls', '-1', dirname], wait=False)

            def has_trim_candidates(fetch):
                # These are actual trim candidates, not entries protected by
                # client caps or dirty metadata. The fetch pins only the dir.
                dump = self._dump_dir(dirname)
                return any(dn['nref'] == 0 and 'dirty' not in dn['states'] and
                           not dn['is_null'] for dn in dump.get('dentries', []))

            first_batch = self._wait_for_fetch(dirname, has_trim_candidates)
            self.assertFalse(listing.finished)
            self.assertGreater(first_batch['auth_pins'], 0)
            paths = {dn['path'] for dn in first_batch['dentries']}

            # Apply the toggle synchronously; the scan must retain its initial mode.
            self._configure(mds_dir_fetch_pipelined=False)

            result = self.fs.rank_tell(['cache', 'drop', '1'])
            self.assertEqual(result['flush_journal']['return_code'], 0)
            self.assertIn('trim_cache', result)
            after_trim = self._dump_dir(dirname)
            # Require trimming before this fragment finishes fetching.
            self.assertFalse(listing.finished)
            self.assertIn('fetch', after_trim)
            self.assertTrue(after_trim['fetch']['pipelined'], after_trim)
            self.assertGreater(after_trim['auth_pins'], 0)
            self.assertTrue(paths <= {dn['path']
                                      for dn in after_trim['dentries']},
                            "cache trim evicted a decoded fetch batch")
        finally:
            self._configure(mds_inject_dir_fetch_delay=0)
            if listing is not None:
                listing.wait()

        self.assertEqual(sorted(listing.stdout.getvalue().splitlines()), expected)

    def test_background_fetch_with_mutation_commit_and_trim(self):
        """Cold named mutations commit and survive trimming before scan EOF."""
        count = 256
        dirname = 'background_mutation_trim'
        control = 'trim_control'
        removed = 'file_000240'
        source = 'file_000248'
        destination = 'aaa_renamed'  # behind the full fetch's cursor
        created = 'zzz_created'    # beyond the full fetch's cursor
        self._configure(mds_bal_fragment_dirs=False,
                        mds_dir_keys_per_op=self.KEYS_PER_OP,
                        mds_dir_prefetch=False,
                        mds_dir_prefetch_backend=False,
                        mds_dir_prefetch_backend_hit_threshold=1,
                        mds_dir_prefetch_backend_max=1,
                        mds_dir_fetch_pipelined=True,
                        mds_cache_trim_threshold=1000000)
        for key, value in (('mds_dir_keys_per_op', self.KEYS_PER_OP),
                           ('mds_dir_prefetch', False),
                           ('mds_bal_fragment_dirs', False),
                           ('mds_dir_prefetch_backend_hit_threshold', 1),
                           ('mds_dir_prefetch_backend_max', 1),
                           ('mds_cache_trim_threshold', 1000000)):
            self._wait_for_config(key, value)
        self._create_files(dirname, count)
        self._create_files(control, 1)
        control_ino = self.mount_a.path_to_ino(f'{control}/file_000000')
        self.mount_a.write_file(f'{dirname}/{source}', 'rename payload')
        source_ino = self.mount_a.path_to_ino(f'{dirname}/{source}')
        self._drop_caches()
        self.mount_a.run_shell(['stat', dirname])
        cold = self._dump_dir(dirname)
        self.assertEqual(cold.get('dentries', []), [])
        self.assertNotIn('complete', cold.get('states', []))
        # A clean, unpinned dentry outside the scan shows that the cache
        # drop below really trims: no client holds caps on it.
        op = self.fs.rank_asok(['lock', 'path', f'/{control}/file_000000',
                                'policy:r', '--await'])
        self.assertEqual(op['result'], 0, op)
        self.fs.rank_tell(['op', 'kill', self._reqid_tostr(op['reqid'])],
                          check_status=False)
        self.assertTrue(self._inode(control_ino))
        before = self._fetch_counters()
        procs = []
        self._configure(mds_dir_prefetch_backend=True,
                        mds_inject_dir_fetch_delay=self.FETCH_DELAY_MS)
        try:
            procs.append(self.mount_a.run_shell(
                ['stat', f'{dirname}/file_000000'], wait=False, timeout=180))

            initial = self._wait_for_fetch(
                dirname, lambda fetch: fetch['decoded_keys'] >= self.KEYS_PER_OP,
                timeout=90)
            self.assertLess(len(initial['dentries']), count)
            self.assertFalse({removed, source, destination, created} &
                             self._names(initial),
                             'mutations must resolve cold names')
            committed_before = int(initial['committed_version'])
            keyed_before = self._fetch_counters()
            for args in (['rm', f'{dirname}/{removed}'],
                         ['mv', f'{dirname}/{source}', f'{dirname}/{destination}'],
                         ['touch', f'{dirname}/{created}']):
                procs.append(self.mount_a.run_shell(args, wait=False, timeout=180))
            self.wait_until_true(lambda: all(p.finished for p in procs),
                                 timeout=180, period=1)
            for proc in procs:
                proc.wait()
            self.assertNotIn('complete', self._dump_dir(dirname)['states'])
            # rm, both rename names and the new name each need a keyed read.
            self.assertGreaterEqual(
                self._fetch_delta(keyed_before)['dir_fetch_keys'], 4)

            self.fs.flush()
            committed = self._dump_dir(dirname)
            self.assertNotIn('complete', committed['states'])
            self.assertGreater(int(committed['committed_version']), committed_before)
            candidates = {dn['path'] for dn in committed['dentries']
                          if dn['nref'] == 0 and 'dirty' not in dn['states']
                          and not dn['is_null']}
            self.assertTrue(candidates, committed)
            self.assertTrue(self._inode(control_ino),
                            'control dentry was trimmed before the cache drop')
            result = self.fs.rank_tell(['cache', 'drop', '1'])
            self.assertEqual(result['flush_journal']['return_code'], 0)
            self.assertIn('trim_cache', result)
            trimmed = self._dump_dir(dirname)
            self.assertNotIn('complete', trimmed['states'])
            self.assertIn('fetch', trimmed)
            self.assertGreater(trimmed['auth_pins'], 0)
            self.assertEqual(self._inode(control_ino), {},
                             'cache drop did not trim the control dentry')
            self.assertTrue(candidates <= {dn['path'] for dn in trimmed['dentries']},
                            'trim evicted a partially decoded batch')
        finally:
            self._configure(mds_inject_dir_fetch_delay=0)
            self.wait_until_true(lambda: all(p.finished for p in procs),
                                 timeout=120, period=1)
            for proc in procs:
                proc.wait()

        self.wait_until_true(
            lambda: 'complete' in self._dump_dir(dirname)['states'],
            timeout=90, period=1)
        delta = self._fetch_delta(before)
        self.assertEqual(delta['dir_fetch_continue'], 1, delta)
        self.assertEqual(delta['dir_fetch_restart'], 0, delta)
        self.assertEqual(delta['dir_fetch_background'], 1, delta)
        self.assertEqual(delta['dir_fetch_complete'], 1, delta)
        self.assertEqual(self._dump_dir(dirname)['auth_pins'], 0)
        expected = {'file_%06d' % i for i in range(count)}
        expected.difference_update((removed, source))
        expected.update((destination, created))
        for cold_read in (False, True):
            with self.subTest(cold=cold_read):
                if cold_read:
                    self._configure(mds_dir_prefetch_backend=False)
                    self._drop_caches()
                self.assertEqual(self._listdir(dirname), sorted(expected))
                self.assertEqual(self.mount_a.path_to_ino(f'{dirname}/{destination}'),
                                 source_ino)
                self.assertEqual(self.mount_a.read_file(f'{dirname}/{destination}'),
                                 'rename payload')
