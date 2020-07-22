# -*- coding: utf-8 -*-

from __future__ import absolute_import

from .helper import DashboardTestCase, JObj, JList


class CrushRuleTest(DashboardTestCase):

    AUTH_ROLES = ['pool-manager']

    rule_schema = JObj(sub_elems={
        'max_size': int,
        'min_size': int,
        'rule_id': int,
        'rule_name': str,
        'ruleset': int,
        'steps': JList(JObj({}, allow_unknown=True))
    }, allow_unknown=True)

    def create_and_delete_rule(self, data):
        name = data['name']
        # Creates rule
        self._post('/api/crush_rule', data)
        self.assertStatus(201)
        # Makes sure rule exists
        rule = self._get('/api/crush_rule/{}'.format(name))
        self.assertStatus(200)
        self.assertSchemaBody(self.rule_schema)
        self.assertEqual(rule['rule_name'], name)
        # Deletes rule
        self._delete('/api/crush_rule/{}'.format(name))
        self.assertStatus(204)

    @DashboardTestCase.RunAs('test', 'test', ['rgw-manager'])
    def test_read_access_permissions(self):
        self._get('/api/crush_rule')
        self.assertStatus(403)

    @DashboardTestCase.RunAs('test', 'test', ['read-only'])
    def test_write_access_permissions(self):
        self._get('/api/crush_rule')
        self.assertStatus(200)
        data = {'name': 'some_rule', 'root': 'default', 'failure_domain': 'osd'}
        self._post('/api/crush_rule', data)
        self.assertStatus(403)
        self._delete('/api/crush_rule/default')
        self.assertStatus(403)

    @classmethod
    def tearDownClass(cls):
        super(CrushRuleTest, cls).tearDownClass()
        cls._ceph_cmd(['osd', 'crush', 'rule', 'rm', 'some_rule'])
        cls._ceph_cmd(['osd', 'crush', 'rule', 'rm', 'another_rule'])

    def test_list(self):
        self._get('/api/crush_rule')
        self.assertStatus(200)
        self.assertSchemaBody(JList(self.rule_schema))

    def test_create(self):
        self.create_and_delete_rule({
            'name': 'some_rule',
            'root': 'default',
            'failure_domain': 'osd'
        })

    @DashboardTestCase.RunAs('test', 'test', ['pool-manager', 'cluster-manager'])
    def test_create_with_ssd(self):
        data = self._get('/api/osd/0')
        self.assertStatus(200)
        device_class = data['osd_metadata']['default_device_class']
        self.create_and_delete_rule({
            'name': 'another_rule',
            'root': 'default',
            'failure_domain': 'osd',
            'device_class': device_class
        })

    def test_crush_rule_info(self):
        self._get('/ui-api/crush_rule/info')
        self.assertStatus(200)
        self.assertSchemaBody(JObj({
            'names': JList(str),
            'nodes': JList(JObj({}, allow_unknown=True))
        }))

