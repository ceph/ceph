import inspect
import unittest
from unittest import mock

from orchestrator import Orchestrator as OrchestratorBase

from ..controllers.orchestrator import Orchestrator
from ..services.orchestrator import OrchFeature
from ..tests import ControllerTestCase


class OrchestratorControllerTest(ControllerTestCase):
    URL_STATUS = '/ui-api/orchestrator/status'
    URL_INVENTORY = '/api/orchestrator/inventory'

    @classmethod
    def setup_server(cls):
        cls.setup_controllers([Orchestrator])

    @mock.patch('dashboard.controllers.orchestrator.OrchClient.instance')
    def test_status_get(self, instance):
        status = {'available': False, 'description': ''}

        fake_client = mock.Mock()
        fake_client.status.return_value = status
        instance.return_value = fake_client

        self._get(self.URL_STATUS)
        self.assertStatus(200)
        self.assertJsonBody(status)


class TestOrchestrator(unittest.TestCase):
    def test_features_has_corresponding_methods(self):
        defined_methods = [v for k, v in inspect.getmembers(
            OrchFeature, lambda m: not inspect.isroutine(m)) if not k.startswith('_')]
        orchestrator_methods = [k for k, v in inspect.getmembers(
            OrchestratorBase, inspect.isroutine)]
        for method in defined_methods:
            self.assertIn(method, orchestrator_methods)
