# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .helper import DashboardTestCase, JList, JObj


class LogsTest(DashboardTestCase):
    CEPHFS = True

    def test_logs(self):
        data = self._get("/api/logs/all")
        self.assertStatus(200)
        log_entry_schema = JList(JObj({
            'addrs': JObj({
                'addrvec': JList(JObj({
                    'addr': str,
                    'nonce': int,
                    'type': str
                }))
            }),
            'channel': str,
            'message': str,
            'name': str,
            'priority': str,
            'rank': str,
            'seq': int,
            'stamp': str
        }))
        schema = JObj({
            'audit_log': log_entry_schema,
            'clog': log_entry_schema
        })
        self.assertSchema(data, schema)

    @DashboardTestCase.RunAs('test', 'test', ['pool-manager'])
    def test_log_perms(self):
        self._get("/api/logs/all")
        self.assertStatus(403)
