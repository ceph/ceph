from .helper import DashboardTestCase, JLeaf, JObj


class ClusterTest(DashboardTestCase):

    def setUp(self):
        super().setUp()
        self.reset_session()

    def test_get_status(self):
        data = self._get('/api/cluster', version='0.1')
        self.assertStatus(200)
        self.assertSchema(data, JObj(sub_elems={
            "status": JLeaf(str)
        }, allow_unknown=False))

    def test_update_status(self):
        req = {'status': 'POST_INSTALLED'}
        self._put('/api/cluster', req, version='0.1')
        self.assertStatus(200)
        data = self._get('/api/cluster', version='0.1')
        self.assertStatus(200)
        self.assertEqual(data, req)
