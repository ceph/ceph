from __future__ import absolute_import

from .helper import DashboardTestCase


class SummaryTest(DashboardTestCase):
    CEPHFS = True

    def test_summary(self):
        data = self._get("/api/summary")
        self.assertStatus(200)

        self.assertIn('health_status', data)
        self.assertIn('mgr_id', data)
        self.assertIn('have_mon_connection', data)
        self.assertIn('rbd_mirroring', data)
        self.assertIn('executing_tasks', data)
        self.assertIn('finished_tasks', data)
        self.assertIn('version', data)
        self.assertIsNotNone(data['health_status'])
        self.assertIsNotNone(data['mgr_id'])
        self.assertIsNotNone(data['have_mon_connection'])
        self.assertEqual(data['rbd_mirroring'], {'errors': 0, 'warnings': 0})

    @DashboardTestCase.RunAs('test', 'test', ['pool-manager'])
    def test_summary_permissions(self):
        data = self._get("/api/summary")
        self.assertStatus(200)

        self.assertIn('health_status', data)
        self.assertIn('mgr_id', data)
        self.assertIn('have_mon_connection', data)
        self.assertNotIn('rbd_mirroring', data)
        self.assertIn('executing_tasks', data)
        self.assertIn('finished_tasks', data)
        self.assertIn('version', data)
        self.assertIsNotNone(data['health_status'])
        self.assertIsNotNone(data['mgr_id'])
        self.assertIsNotNone(data['have_mon_connection'])
