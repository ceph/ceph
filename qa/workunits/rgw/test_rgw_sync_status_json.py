#!/usr/bin/env python3
"""
Functional checks for ``radosgw-admin sync status [--format=json]``.

Covers:
  * single-site / meta-master shape (no metadata sync, empty data_sync)
  * optional secondary-zone expectations when data_sync sources exist
  * plaintext default compatibility
  * structured/parseable JSON fields (counts, behind shards, timestamps)
  * light performance vs plaintext

JSONFormatter omits the root section name, so fields are at the top level
(not wrapped in a ``sync_status`` object). Per-shard markers are not dumped.
"""

import json
import os
import re
import shutil
import statistics
import subprocess
import sys
import time
import unittest


def repo_root():
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.abspath(os.path.join(here, '..', '..', '..'))


def find_radosgw_admin():
    env = os.environ.get('RADOSGW_ADMIN') or os.environ.get('CEPH_BIN')
    if env:
        candidate = env
        if os.path.isdir(candidate):
            candidate = os.path.join(candidate, 'radosgw-admin')
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    found = shutil.which('radosgw-admin')
    if found:
        return found
    root = repo_root()
    for sub in ('build-local/bin/radosgw-admin', 'build/bin/radosgw-admin'):
        candidate = os.path.join(root, sub)
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    return None


def find_ceph_conf():
    if os.environ.get('CEPH_CONF'):
        path = os.environ['CEPH_CONF']
        if os.path.isfile(path):
            return path
    root = repo_root()
    candidates = [
        os.path.join(root, 'build-local', 'ceph.conf'),
        os.path.join(root, 'build', 'ceph.conf'),
        os.path.join(root, 'build-local', 'out', 'ceph.conf'),
        os.path.join(root, 'build', 'out', 'ceph.conf'),
        '/etc/ceph/ceph.conf',
    ]
    conf_path = os.environ.get('CEPH_CONF_PATH')
    if conf_path:
        candidates.insert(0, os.path.join(conf_path, 'ceph.conf'))
    for path in candidates:
        if os.path.isfile(path):
            return path
    return None


def parse_sync_status_json(stdout: str) -> dict:
    """Return the top-level sync-status object (flat JSONFormatter root)."""
    data = json.loads(stdout)
    # Be tolerant if a future change wraps under sync_status.
    if isinstance(data, dict) and 'metadata_sync' in data:
        return data
    if isinstance(data, dict) and isinstance(data.get('sync_status'), dict):
        return data['sync_status']
    raise AssertionError(f'unexpected sync status JSON shape: {data!r}')


def _assert_sync_progress(obj, label):
    """full_sync / incremental_sync must be typed counts, not prose."""
    for key in ('full_sync', 'incremental_sync'):
        assert key in obj, f'{label}: missing {key}'
        section = obj[key]
        assert isinstance(section, dict), f'{label}.{key} must be object'
        assert isinstance(section.get('shards'), int), f'{label}.{key}.shards'
        assert isinstance(section.get('total_shards'), int), f'{label}.{key}.total_shards'
        assert 'summary' not in section, f'{label}.{key} must not use string summary'


def _assert_behind_fields(obj, label):
    assert 'caught_up' in obj, f'{label}: missing caught_up'
    assert isinstance(obj['caught_up'], bool), f'{label}: caught_up must be bool'
    assert isinstance(obj.get('shards_behind'), int), f'{label}: shards_behind'
    assert isinstance(obj.get('behind_shards'), list), f'{label}: behind_shards'
    for shard in obj['behind_shards']:
        assert isinstance(shard, int), f'{label}: behind_shards entries must be ints'
    if obj['caught_up']:
        assert obj['shards_behind'] == 0, f'{label}: caught_up but shards_behind!=0'
    oldest = obj.get('oldest_incremental_change')
    if oldest is not None:
        assert isinstance(oldest, dict), f'{label}: oldest_incremental_change'
        assert isinstance(oldest.get('shard_id'), int)
        assert isinstance(oldest.get('timestamp'), str)
        assert oldest['timestamp'], f'{label}: empty oldest timestamp'


class TestSyncStatusFormat(unittest.TestCase):
    PERF_ITERS = 5
    CMD_TIMEOUT_SEC = 30

    REQUIRED_KEYS = (
        'realm_id',
        'realm_name',
        'zonegroup_id',
        'zonegroup_name',
        'zone_id',
        'zone_name',
        'current_time',
        'metadata_sync',
        'data_sync',
    )

    @classmethod
    def setUpClass(cls):
        cls.radosgw_admin = find_radosgw_admin()
        if not cls.radosgw_admin:
            raise unittest.SkipTest(
                'radosgw-admin not found; set PATH/CEPH_BIN/RADOSGW_ADMIN'
            )
        cls.ceph_conf = find_ceph_conf()
        try:
            probe = cls._run_static(['sync', 'status'], check=False)
        except subprocess.TimeoutExpired as e:
            raise unittest.SkipTest(
                f'radosgw-admin timed out after {cls.CMD_TIMEOUT_SEC}s — '
                'cluster likely incomplete. '
                f'cmd={e.cmd}'
            ) from e
        if probe.returncode != 0:
            raise unittest.SkipTest(
                'no usable Ceph cluster for radosgw-admin '
                f'(ret={probe.returncode}). stderr=\n{probe.stderr}'
            )

    @classmethod
    def _run_static(cls, args, check=True):
        cmd = [cls.radosgw_admin]
        if cls.ceph_conf:
            cmd += ['-c', cls.ceph_conf]
        cmd += args
        completed = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
            timeout=cls.CMD_TIMEOUT_SEC,
        )
        if check and completed.returncode != 0:
            raise AssertionError(
                f'command failed: {cmd}\n'
                f'ret={completed.returncode}\n'
                f'stdout={completed.stdout}\n'
                f'stderr={completed.stderr}'
            )
        return completed

    def _run(self, args, check=True):
        return self._run_static(args, check=check)

    def _json_status(self):
        out = self._run(['sync', 'status', '--format=json']).stdout
        return parse_sync_status_json(out)

    def test_default_is_plaintext(self):
        out = self._run(['sync', 'status']).stdout
        self.assertIn('realm', out)
        self.assertIn('zonegroup', out)
        self.assertIn('zone', out)
        self.assertIn('metadata sync', out)
        self.assertFalse(
            out.lstrip().startswith('{'),
            'default sync status output must remain human-readable plaintext',
        )

    def test_format_json_is_structured(self):
        status = self._json_status()
        for key in self.REQUIRED_KEYS:
            self.assertIn(key, status, f'missing key {key}')
        self.assertIsInstance(status['metadata_sync'], dict)
        self.assertIn('status', status['metadata_sync'])
        self.assertIsInstance(status['data_sync'], list)
        self.assertTrue(status['zonegroup_id'])
        self.assertTrue(status['zone_id'])
        self.assertTrue(status['current_time'])
        # Concise structured summary — no per-shard marker dump / prose blobs.
        md = status['metadata_sync']
        self.assertNotIn('sync_status', md)
        self.assertNotIn('messages', md)
        for src in status['data_sync']:
            self.assertNotIn('sync_status', src)
            self.assertNotIn('messages', src)

    def test_pretty_format_json_still_parses(self):
        out = self._run(
            ['sync', 'status', '--format=json', '--pretty-format']
        ).stdout
        status = parse_sync_status_json(out)
        self.assertIn('metadata_sync', status)
        self.assertIn('data_sync', status)

    def test_single_site_or_master_shape(self):
        """
        On a single-site / meta-master zone:
          metadata_sync.status == 'no sync (zone is master)'
          data_sync is typically empty (no peer sources).
        """
        status = self._json_status()
        md = status['metadata_sync']
        if md.get('status') != 'no sync (zone is master)':
            # Likely a secondary zone — covered by secondary-shape test.
            raise unittest.SkipTest(
                f'not a meta-master view (metadata_sync={md!r}); '
                'run against master or use multisite nose tests'
            )
        self.assertEqual(md['status'], 'no sync (zone is master)')
        self.assertNotIn('error', md)
        self.assertNotIn('full_sync', md)
        # Single-site default: no connected sources.
        self.assertIsInstance(status['data_sync'], list)
        for src in status['data_sync']:
            self.assertIn('source_zone', src)
            self.assertTrue('status' in src or 'error' in src)

    def test_secondary_shape_when_present(self):
        """
        If this gateway syncs metadata (secondary), JSON must expose typed
        progress / behind fields for parsers.
        """
        status = self._json_status()
        md = status['metadata_sync']
        if md.get('status') == 'no sync (zone is master)':
            raise unittest.SkipTest('single-site / master — no secondary shape')

        self.assertIn(
            md.get('status'),
            ('init', 'preparing for full sync', 'syncing', 'unknown'),
            f'unexpected metadata status: {md!r}',
        )
        self.assertNotIn('messages', md)
        self.assertNotIn('sync_status', md)
        if 'error' not in md:
            _assert_sync_progress(md, 'metadata_sync')
            _assert_behind_fields(md, 'metadata_sync')
            self.assertIn('period_mismatch', md)
        if status['data_sync']:
            for src in status['data_sync']:
                self.assertIn('source_zone', src)
                self.assertNotIn('messages', src)
                self.assertNotIn('sync_status', src)
                if 'error' not in src and src.get('status') != 'not syncing from zone':
                    _assert_sync_progress(src, 'data_sync')
                    _assert_behind_fields(src, 'data_sync')
                    self.assertIsInstance(src.get('shards_recovering'), int)
                    self.assertIsInstance(src.get('recovering_shards'), list)

    def test_json_matches_plaintext_identity(self):
        """Typed JSON counts must agree with plaintext summary lines."""
        plain = self._run(['sync', 'status']).stdout
        status = self._json_status()
        self.assertIn(status['zonegroup_id'], plain)
        self.assertIn(status['zone_id'], plain)
        if status['zonegroup_name']:
            self.assertIn(status['zonegroup_name'], plain)
        if status['zone_name']:
            self.assertIn(status['zone_name'], plain)

        md = status['metadata_sync']
        if md.get('status') != 'no sync (zone is master)' and 'error' not in md:
            m = re.search(r'full sync:\s*(\d+)/(\d+)\s*shards', plain)
            self.assertIsNotNone(m, 'plaintext missing metadata full sync line')
            self.assertEqual(md['full_sync']['shards'], int(m.group(1)))
            self.assertEqual(md['full_sync']['total_shards'], int(m.group(2)))
            m = re.search(r'incremental sync:\s*(\d+)/(\d+)\s*shards', plain)
            self.assertIsNotNone(m, 'plaintext missing metadata incremental line')
            self.assertEqual(md['incremental_sync']['shards'], int(m.group(1)))
            self.assertEqual(md['incremental_sync']['total_shards'], int(m.group(2)))
            if md.get('caught_up'):
                self.assertIn('metadata is caught up with master', plain)
            else:
                self.assertRegex(
                    plain,
                    r'metadata is behind on\s+%d\s+shards' % md['shards_behind'],
                )

        for src in status['data_sync']:
            if 'error' in src or src.get('status') == 'not syncing from zone':
                continue
            source = src['source_zone']
            self.assertIn(source, plain)
            # Match the data-sync block for this source (best-effort).
            block_m = re.search(
                r'source:\s*' + re.escape(source) + r'.*?(?=source:|\Z)',
                plain,
                re.S,
            )
            block = block_m.group(0) if block_m else plain
            m = re.search(r'full sync:\s*(\d+)/(\d+)\s*shards', block)
            self.assertIsNotNone(m, f'plaintext missing data full sync for {source}')
            self.assertEqual(src['full_sync']['shards'], int(m.group(1)))
            self.assertEqual(src['full_sync']['total_shards'], int(m.group(2)))
            m = re.search(r'incremental sync:\s*(\d+)/(\d+)\s*shards', block)
            self.assertIsNotNone(m, f'plaintext missing data incremental for {source}')
            self.assertEqual(src['incremental_sync']['shards'], int(m.group(1)))
            self.assertEqual(src['incremental_sync']['total_shards'], int(m.group(2)))
            if src.get('caught_up'):
                self.assertIn('data is caught up with source', block)

    def test_json_path_performance_comparable(self):
        def timed(args):
            times = []
            for _ in range(self.PERF_ITERS):
                start = time.perf_counter()
                completed = self._run(args)
                elapsed = time.perf_counter() - start
                times.append(elapsed)
                self.assertTrue(len(completed.stdout) > 0)
            return times

        plain = timed(['sync', 'status'])
        js = timed(['sync', 'status', '--format=json'])
        plain_mean = statistics.mean(plain)
        json_mean = statistics.mean(js)
        print(
            f'sync status perf: plaintext mean={plain_mean:.4f}s '
            f'json mean={json_mean:.4f}s '
            f'(n={self.PERF_ITERS})',
            file=sys.stderr,
        )
        self.assertLessEqual(
            json_mean,
            plain_mean * 5.0 + 1.0,
            f'JSON sync status too slow vs plaintext '
            f'(json={json_mean:.4f}s plain={plain_mean:.4f}s)',
        )


if __name__ == '__main__':
    unittest.main()
