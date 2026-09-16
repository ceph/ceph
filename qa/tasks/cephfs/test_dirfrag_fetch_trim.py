"""Exercise cache trimming between batches of a full dirfrag fetch."""

from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.test_dirfrag_fetch import DirfragFetchLogMixin


class TestDirfragFetchTrim(DirfragFetchLogMixin, CephFSTestCase):
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
        self.fs.rank_asok(['config', 'set', 'mds_inject_dir_fetch_delay', '10000'])
        listing = None
        try:
            listing = self.mount_a.run_shell(['ls', '-1', dirname], wait=False)
            first_batch = {}

            def has_decoded_batch():
                dump = self._dump_dir(dirname)
                entries = dump.get('dentries', [])
                if ('complete' in dump.get('states', []) or
                        not 0 < len(entries) < self.NFILES):
                    return False
                # These are actual trim candidates, not entries protected by
                # client caps or dirty metadata. The fetch pins only the dir.
                candidates = [dn for dn in entries
                              if dn['nref'] == 0 and
                              'dirty' not in dn['states'] and
                              not dn['is_null']]
                if not candidates:
                    return False
                first_batch.update(dump)
                return True

            self.wait_until_true(has_decoded_batch, timeout=60, period=0.5)
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
            self.assertNotIn('complete', after_trim['states'])
            self.assertGreater(after_trim['auth_pins'], 0)
            self.assertTrue(paths <= {dn['path']
                                      for dn in after_trim['dentries']},
                            "cache trim evicted a decoded fetch batch")
        finally:
            self.fs.rank_asok(['config', 'set', 'mds_inject_dir_fetch_delay', '0'])
            if listing is not None:
                listing.wait()

        self.assertEqual(sorted(listing.stdout.getvalue().splitlines()), expected)

    def test_background_fetch_with_mutation_commit_and_trim(self):
        """Cold named mutations commit and survive trimming before scan EOF."""
        count = 256
        dirname = 'background_mutation_trim'
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
        self.mount_a.write_file(f'{dirname}/{source}', 'rename payload')
        source_ino = self.mount_a.path_to_ino(f'{dirname}/{source}')
        ino = self.mount_a.path_to_ino(dirname)
        self._drop_caches()
        self.mount_a.run_shell(['stat', dirname])
        cold = self._dump_dir(dirname)
        self.assertEqual(cold.get('dentries', []), [])
        self.assertNotIn('complete', cold.get('states', []))
        before = self.fs.rank_asok(['perf', 'dump', 'mds'])['mds']
        procs = []
        prefix = f'.cache.dir(0x{ino:x}) '
        with self._capture_mds_log(level=12) as read_log:
            self._configure(mds_dir_prefetch_backend=True,
                            mds_inject_dir_fetch_delay=10000)
            try:
                procs.append(self.mount_a.run_shell(
                    ['stat', f'{dirname}/file_000000'], wait=False, timeout=180))

                def partial():
                    dump = self._dump_dir(dirname)
                    return ('complete' not in dump.get('states', []) and
                            self.KEYS_PER_OP <= len(dump.get('dentries', [])) < count)

                self.wait_until_true(partial, timeout=90, period=0.5)
                initial = self._dump_dir(dirname)
                names = {dn['path'].rsplit('/', 1)[-1]
                         for dn in initial['dentries']}
                self.assertFalse({removed, source, destination, created} & names,
                                 'mutations must resolve cold names')
                committed_before = int(initial['committed_version'])
                for args in (['rm', f'{dirname}/{removed}'],
                             ['mv', f'{dirname}/{source}', f'{dirname}/{destination}'],
                             ['touch', f'{dirname}/{created}']):
                    procs.append(self.mount_a.run_shell(args, wait=False, timeout=180))
                self.wait_until_true(lambda: all(p.finished for p in procs),
                                     timeout=180, period=1)
                for proc in procs:
                    proc.wait()
                self.assertNotIn('complete', self._dump_dir(dirname)['states'])
                lines = [line for line in read_log() if prefix in line]
                for name in (removed, source, destination, created):
                    self.assertTrue(any(f'fetch key({name}, ' in line for line in lines),
                                    f'no cold named fetch for {name}: {lines}')
                self.assertTrue(any('_fetched 0 keys for ' in line for line in lines),
                                'must resolve an absent name through keyed fetch')

                self.fs.flush()
                committed = self._dump_dir(dirname)
                self.assertNotIn('complete', committed['states'])
                self.assertGreater(int(committed['committed_version']), committed_before)
                candidates = {dn['path'] for dn in committed['dentries']
                              if dn['nref'] == 0 and 'dirty' not in dn['states']
                              and not dn['is_null']}
                self.assertTrue(candidates, committed)
                # Exclude earlier incidental trimming from the log check.
                trim_offset = len(read_log())
                result = self.fs.rank_tell(['cache', 'drop', '1'])
                self.assertEqual(result['flush_journal']['return_code'], 0)
                self.assertIn('trim_cache', result)
                trimmed = self._dump_dir(dirname)
                self.assertNotIn('complete', trimmed['states'])
                self.assertGreater(trimmed['auth_pins'], 0)
                self.assertTrue(candidates <= {dn['path'] for dn in trimmed['dentries']},
                                'trim evicted a partially decoded batch')
                trim_lines = read_log()[trim_offset:]
                self.assertTrue(any('trim_dentry keeping dentry in fetching dirfrag ' in line
                                    and dirname + '/' in line for line in trim_lines),
                                f'trim did not visit the target dirfrag: {trim_lines}')
            finally:
                self._configure(mds_inject_dir_fetch_delay=0)
                self.wait_until_true(lambda: all(p.finished for p in procs),
                                     timeout=120, period=1)
                for proc in procs:
                    proc.wait()

            self.wait_until_true(
                lambda: 'complete' in self._dump_dir(dirname)['states'],
                timeout=90, period=1)
            lines = [line for line in read_log() if prefix in line]
            self.assertTrue(any('while fetching at v' in line and ', continuing' in line
                                for line in lines), lines)
            self.assertFalse(any(', restarting' in line for line in lines), lines)
        after = self.fs.rank_asok(['perf', 'dump', 'mds'])['mds']
        self.assertEqual(after['dir_fetch_background'] - before['dir_fetch_background'], 1)
        self.assertEqual(after['dir_fetch_complete'] - before['dir_fetch_complete'], 1)
        self.assertGreater(after['dir_fetch_keys'] - before['dir_fetch_keys'], 1)
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
