# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .helper import DashboardTestCase


class OrchestratorControllerTest(DashboardTestCase):

    AUTH_ROLES = ['cluster-manager']

    URL_STATUS = '/ui-api/orchestrator/status'

    ORCHESTRATOR = True

    @classmethod
    def setUpClass(cls):
        super(OrchestratorControllerTest, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        cmd = ['test_orchestrator', 'load_data', '-i', '-']
        cls.mgr_cluster.mon_manager.raw_cluster_cmd_result(*cmd, stdin='{}')

    def test_status_get(self):
        data = self._get(self.URL_STATUS)
        self.assertStatus(200)
        self.assertTrue(data['available'])
