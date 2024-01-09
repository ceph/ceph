# -*- coding: utf-8 -*-

from __future__ import absolute_import

from .helper import DashboardTestCase, JList, JObj


class ECPTest(DashboardTestCase):

    AUTH_ROLES = ['pool-manager']

    @DashboardTestCase.RunAs('test', 'test', ['rgw-manager'])
    def test_read_access_permissions(self):
        self._get('/api/erasure_code_profile')
        self.assertStatus(403)

    @DashboardTestCase.RunAs('test', 'test', ['read-only'])
    def test_write_access_permissions(self):
        self._get('/api/erasure_code_profile')
        self.assertStatus(200)
        data = {'name': 'ecp32', 'k': 3, 'm': 2}
        self._post('/api/erasure_code_profile', data)
        self.assertStatus(403)
        self._delete('/api/erasure_code_profile/default')
        self.assertStatus(403)

    @classmethod
    def tearDownClass(cls):
        super(ECPTest, cls).tearDownClass()
        cls._ceph_cmd(['osd', 'erasure-code-profile', 'rm', 'ecp32'])
        cls._ceph_cmd(['osd', 'erasure-code-profile', 'rm', 'lrc'])

    def test_list(self):
        data = self._get('/api/erasure_code_profile')
        self.assertStatus(200)

        default = [p for p in data if p['name'] == 'default']
        if default:
            default_ecp = {
                'k': 2,
                'technique': 'reed_sol_van',
                'm': 1,
                'name': 'default',
                'plugin': 'jerasure'
            }
            if 'crush-failure-domain' in default[0]:
                default_ecp['crush-failure-domain'] = default[0]['crush-failure-domain']
            self.assertSubset(default_ecp, default[0])
            get_data = self._get('/api/erasure_code_profile/default')
            self.assertEqual(get_data, default[0])

    def test_create(self):
        data = {'name': 'ecp32', 'k': 3, 'm': 2}
        self._post('/api/erasure_code_profile', data)
        self.assertStatus(201)

        self._get('/api/erasure_code_profile/ecp32')
        self.assertJsonSubset({
            'crush-device-class': '',
            'crush-failure-domain': 'osd',
            'crush-root': 'default',
            'jerasure-per-chunk-alignment': 'false',
            'k': 3,
            'm': 2,
            'name': 'ecp32',
            'plugin': 'jerasure',
            'technique': 'reed_sol_van',
        })

        self.assertStatus(200)

        self._delete('/api/erasure_code_profile/ecp32')
        self.assertStatus(204)

    def test_create_plugin(self):
        data = {'name': 'lrc', 'k': '2', 'm': '2', 'l': '2', 'plugin': 'lrc'}
        self._post('/api/erasure_code_profile', data)
        self.assertJsonBody(None)
        self.assertStatus(201)

        self._get('/api/erasure_code_profile/lrc')
        self.assertJsonSubset({
            'crush-device-class': '',
            'crush-failure-domain': 'host',
            'crush-root': 'default',
            'k': 2,
            'l': '2',
            'm': 2,
            'name': 'lrc',
            'plugin': 'lrc'
        })

        self.assertStatus(200)

        self._delete('/api/erasure_code_profile/lrc')
        self.assertStatus(204)

    def test_ecp_info(self):
        self._get('/ui-api/erasure_code_profile/info')
        self.assertSchemaBody(JObj({
            'names': JList(str),
            'plugins': JList(str),
            'directory': str,
            'nodes': JList(JObj({}, allow_unknown=True))
        }))
