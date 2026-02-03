import time

from .helper import DashboardTestCase, MgrModuleTestCase


class FeedbackTest(DashboardTestCase, MgrModuleTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._ceph_cmd(['mgr', 'module', 'enable', 'feedback'], wait=3)
        cls._get(
            '/api/mgr/module',
            retries=2,
            wait_func=lambda:  # pylint: disable=unnecessary-lambda
            cls.wait_until_rest_api_accessible()
        )

    def test_create_api_key(self):
        self._post('/api/feedback/api_key', {'api_key': 'testapikey'}, version='0.1')
        self.assertStatus(201)

    def test_get_api_key(self):
        response = self._get('/api/feedback/api_key', version='0.1')
        self.assertStatus(200)
        self.assertEqual(response, 'testapikey')

    def test_remove_api_key(self):
        self._delete('/api/feedback/api_key', version='0.1')
        self.assertStatus(204)

    def test_issue_tracker_create_with_invalid_key(self):
        self._post('/api/feedback', {'api_key': 'invalidapikey', 'description': 'test',
                                     'project': 'dashboard', 'subject': 'sub', 'tracker': 'bug'},
                   version='0.1')
        self.assertStatus(400)

    def test_issue_tracker_create_with_invalid_params(self):
        self._post('/api/feedback', {'api_key': '', 'description': 'test', 'project': 'xyz',
                                     'subject': 'testsub', 'tracker': 'invalid'}, version='0.1')
        self.assertStatus(400)
