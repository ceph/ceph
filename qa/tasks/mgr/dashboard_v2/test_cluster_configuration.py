from __future__ import absolute_import

from .helper import DashboardTestCase, authenticate


class ClusterConfigurationTest(DashboardTestCase):
    @authenticate
    def test_list(self):
        data = self._get('/api/cluster_conf')
        self.assertStatus(200)
        self.assertIsInstance(data, list)
        self.assertGreater(len(data), 1000)

        # service filter
        data = self._get('/api/cluster_conf?service=mds')
        self.assertTrue(all('mds' in e['services'] for e in data))
        self.assertFalse(all('mon' in e['services'] for e in data))

        # basic filter
        data = self._get('/api/cluster_conf?level=basic')
        self.assertTrue(all('basic' in e['level'] for e in data))

        # advanced filter
        data = self._get('/api/cluster_conf?level=advanced')
        actual_levels = set([e['level'] for e in data])
        self.assertTrue({'advanced', 'basic'}.issubset(actual_levels))

        # developer filter
        data = self._get('/api/cluster_conf?level=developer')
        actual_levels = set([e['level'] for e in data])
        self.assertTrue({'advanced', 'basic',
                         'developer'}.issubset(actual_levels))

        # two filters
        data = self._get('/api/cluster_conf?level=advanced&service=mds')
        actual_levels = set([e['level'] for e in data])
        self.assertTrue({'advanced', 'basic'}.issubset(actual_levels))
        self.assertTrue(all('mds' in e['services'] for e in data))

    @authenticate
    def test_get(self):
        data = self._get('/api/cluster_conf/admin_socket')
        self.assertStatus(200)
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
        self.assertIn('enum_values', data)
        self.assertIn('type', data)
        self.assertIn('desc', data)

        data = self._get('/api/cluster_conf/fantasy_name')
        self.assertStatus(404)
