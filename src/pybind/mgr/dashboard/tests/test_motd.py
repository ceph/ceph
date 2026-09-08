# -*- coding: utf-8 -*-
# pylint: disable=protected-access

import hashlib
import json
import unittest

from ..plugins.motd import Motd, MotdSeverity
from . import KVStoreMockMixin  # pylint: disable=no-name-in-module


class MotdTest(unittest.TestCase, KVStoreMockMixin):
    """Unit tests for Motd._set_motd and the helpers it depends on."""

    def setUp(self):
        self.mock_kv_store()
        self.plugin = Motd()

    # ------------------------------------------------------------------
    # _normalize_severity
    # ------------------------------------------------------------------

    def test_normalize_severity_from_enum(self):
        result = self.plugin._normalize_severity(MotdSeverity.INFO)
        self.assertEqual(result, MotdSeverity.INFO)

    def test_normalize_severity_from_valid_string(self):
        for value, expected in (
            ('info', MotdSeverity.INFO),
            ('warning', MotdSeverity.WARNING),
            ('danger', MotdSeverity.DANGER),
        ):
            with self.subTest(value=value):
                self.assertEqual(self.plugin._normalize_severity(value), expected)

    def test_normalize_severity_invalid_string_returns_none(self):
        self.assertIsNone(self.plugin._normalize_severity('critical'))

    # ------------------------------------------------------------------
    # _set_motd — error paths
    # ------------------------------------------------------------------

    def test_set_motd_invalid_severity_returns_error(self):
        rc, out, err = self.plugin._set_motd('critical', '0', 'hello')
        self.assertEqual(rc, 1)
        self.assertEqual(out, '')
        self.assertIn('severity', err)

    def test_set_motd_invalid_expires_returns_error(self):
        rc, out, err = self.plugin._set_motd('info', 'bad-expires', 'hello')
        self.assertEqual(rc, 1)
        self.assertEqual(out, '')
        self.assertIn('expires', err)

    # ------------------------------------------------------------------
    # _set_motd — happy path: no expiry
    # ------------------------------------------------------------------

    def test_set_motd_no_expiry_returns_success(self):
        rc, out, err = self.plugin._set_motd('info', '0', 'hello world')
        self.assertEqual(rc, 0)
        self.assertNotEqual(out, '')
        self.assertEqual(err, '')

    def test_set_motd_no_expiry_stores_expected_fields(self):
        self.plugin._set_motd('warning', '0', 'test message')
        raw = self.plugin.get_option(self.plugin.NAME)
        self.assertIsNotNone(raw)
        data = json.loads(raw)
        self.assertEqual(data['message'], 'test message')
        self.assertEqual(data['severity'], 'warning')
        self.assertEqual(data['expires'], '')   # '0' → no expiry

    def test_set_motd_stores_correct_md5(self):
        message = 'checksum test'
        self.plugin._set_motd('danger', '0', message)
        data = json.loads(self.plugin.get_option(self.plugin.NAME))
        expected_md5 = hashlib.md5(  # pylint: disable=unexpected-keyword-arg
            message.encode(), usedforsecurity=False).hexdigest()
        self.assertEqual(data['md5'], expected_md5)

    # ------------------------------------------------------------------
    # _set_motd — happy path: with expiry
    # ------------------------------------------------------------------

    def test_set_motd_with_expiry_stores_iso_timestamp(self):
        self.plugin._set_motd('info', '2h', 'expiring message')
        data = json.loads(self.plugin.get_option(self.plugin.NAME))
        # expires must be a non-empty ISO 8601 string when a duration is given
        self.assertTrue(data['expires'], 'expected a non-empty expires value')

    def test_set_motd_accepts_enum_severity(self):
        rc, _, err = self.plugin._set_motd(MotdSeverity.DANGER, '0', 'enum severity')
        self.assertEqual(rc, 0)
        self.assertEqual(err, '')
        data = json.loads(self.plugin.get_option(self.plugin.NAME))
        self.assertEqual(data['severity'], 'danger')

    # ------------------------------------------------------------------
    # Overwrite: second call replaces the stored value
    # ------------------------------------------------------------------

    def test_set_motd_overwrite(self):
        self.plugin._set_motd('info', '0', 'first')
        self.plugin._set_motd('warning', '0', 'second')
        data = json.loads(self.plugin.get_option(self.plugin.NAME))
        self.assertEqual(data['message'], 'second')
        self.assertEqual(data['severity'], 'warning')
