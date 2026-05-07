from .test_mgr_module import MgrModuleTestCase


class FeedbackTest(MgrModuleTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls._ceph_cmd(['mgr', 'module', 'enable', 'feedback'], wait=3)
        # Point the feedback module at an unreachable host so the test
        # does not depend on tracker.ceph.com being available. Any
        # create_issue call will fail fast with a ConnectionError
        # (a RequestException subclass) which the dashboard controller
        # is expected to surface as HTTP 400.
        cls._ceph_cmd(['config', 'set', 'mgr',
                       'mgr/feedback/tracker_url',
                       'invalid.example.invalid'])
        cls._get(
            '/api/mgr/module',
            retries=5,
            wait_func=lambda:  # pylint: disable=unnecessary-lambda
            MgrModuleTestCase.wait_until_rest_api_accessible(cls)
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
