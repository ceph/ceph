# -*- coding: utf-8 -*-
# pylint: disable=protected-access

import unittest
from datetime import datetime, timezone
from unittest import mock

from ..controllers.summary import Summary
from ..services.ceph_service import CephService


class SummaryPoolCreateTimeTest(unittest.TestCase):
    def test_converts_ceph_iso_time_to_utc(self):
        result = Summary._pool_create_time_to_iso('2024-05-01T14:00:00.123456+0200')
        self.assertEqual(result, '2024-05-01T12:00:00.123456+00:00')

    def test_keeps_utc_time(self):
        result = Summary._pool_create_time_to_iso('2024-05-01T12:00:00.123456+0000')
        self.assertEqual(result, '2024-05-01T12:00:00.123456+00:00')

    def test_converts_legacy_local_time_to_utc(self):
        legacy = '2019-08-02 02:23:01.618519'
        expected = datetime.strptime(legacy, '%Y-%m-%d %H:%M:%S.%f') \
            .astimezone(timezone.utc).isoformat()
        self.assertEqual(Summary._pool_create_time_to_iso(legacy), expected)

    def test_returns_none_for_missing_or_invalid_values(self):
        for value in (None, '', 'not a date', 42, '2024-05-01'):
            self.assertIsNone(Summary._pool_create_time_to_iso(value))


class SummaryPoolMigrationTasksTest(unittest.TestCase):
    @staticmethod
    def _migration(**overrides):
        migration = {
            'pool_id': 0,
            'pool_name': 'target_pool',
            'src_id': 1,
            'src_name': '.migrate-0',
            'progress': 40,
            'create_time': '2024-05-01T12:00:00.123456+0000',
        }
        migration.update(overrides)
        return migration

    @mock.patch.object(CephService, 'get_pool_list', return_value=[])
    @mock.patch.object(CephService, 'get_pool_migration_info')
    def test_begin_time_is_the_target_pool_creation_time(self, mock_info, _pool_list):
        mock_info.return_value = ([self._migration()], {})

        first = Summary._get_pool_migration_tasks()
        second = Summary._get_pool_migration_tasks()

        self.assertEqual(first[0]['begin_time'], '2024-05-01T12:00:00.123456+00:00')
        # stable across polls, unlike the previous request-time value
        self.assertEqual(first[0]['begin_time'], second[0]['begin_time'])
        self.assertEqual(first[0]['progress'], 40)
        self.assertEqual(first[0]['metadata'],
                         {'pool_name': 'target_pool', 'pool_id': 0, 'src_name': '.migrate-0'})

    @mock.patch.object(CephService, 'get_pool_list', return_value=[])
    @mock.patch.object(CephService, 'get_pool_migration_info')
    def test_migration_without_valid_create_time_is_skipped(self, mock_info, _pool_list):
        mock_info.return_value = ([
            self._migration(pool_id=1, create_time=None),
            self._migration(pool_id=2, create_time='garbage'),
            self._migration(pool_id=3),
        ], {})

        tasks = Summary._get_pool_migration_tasks()

        self.assertEqual([t['metadata']['pool_id'] for t in tasks], [3])
