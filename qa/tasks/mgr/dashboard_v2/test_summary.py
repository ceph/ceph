from __future__ import absolute_import

from .helper import DashboardTestCase, authenticate


class SummaryTest(DashboardTestCase):
    CEPHFS = True

    @authenticate
    def test_summary(self):
        data = self._get("/api/summary")
        self.assertStatus(200)

        self.assertIn('filesystems', data)
        self.assertIn('health_status', data)
        self.assertIn('rbd_pools', data)
        self.assertIn('mgr_id', data)
        self.assertIn('have_mon_connection', data)
        self.assertIn('rbd_mirroring', data)
        self.assertIsNotNone(data['filesystems'])
        self.assertIsNotNone(data['health_status'])
        self.assertIsNotNone(data['rbd_pools'])
        self.assertIsNotNone(data['mgr_id'])
        self.assertIsNotNone(data['have_mon_connection'])
        self.assertEqual(data['rbd_mirroring'], {'errors': 0, 'warnings': 0})
