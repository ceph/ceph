from __future__ import absolute_import

from .helper import DashboardTestCase, authenticate


class ClusterConfigurationTest(DashboardTestCase):
    @authenticate
    def test_list(self):
        data = self._get('/api/cluster_conf')
        self.assertStatus(200)
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 1000)
        for conf in data:
            self._validate_single(conf)

    @authenticate
    def test_get(self):
        data = self._get('/api/cluster_conf/admin_socket')
        self.assertStatus(200)
        self._validate_single(data)
        self.assertIn('enum_values', data)

        data = self._get('/api/cluster_conf/fantasy_name')
        self.assertStatus(404)

    def _validate_single(self, data):
        self.assertIn('name', data)
        self.assertIn('daemon_default', data)
        self.assertIn('long_desc', data)
        self.assertIn('level', data)
        self.assertIn('default', data)
        self.assertIn('see_also', data)
        self.assertIn('tags', data)
        self.assertIn('min', data)
        self.assertIn('max', data)
        self.assertIn('services', data)
        self.assertIn('type', data)
        self.assertIn('desc', data)

