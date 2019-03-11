# -*- coding: utf-8 -*-
from . import ControllerTestCase
from .. import mgr
from ..controllers import BaseController, Controller
from ..controllers.prometheus import Prometheus, PrometheusReceiver


@Controller('alertmanager/mocked/api/v1/alerts', secure=False)
class AlertManagerMockInstance(BaseController):
    def __call__(self, path, **params):
        return 'Some Api {}'.format(path)


class PrometheusControllerTest(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        settings = {
            'ALERTMANAGER_API_HOST': 'http://localhost:{}/alertmanager/mocked/'.format(54583)
        }
        mgr.get_module_option.side_effect = settings.get
        Prometheus._cp_config['tools.authenticate.on'] = False  # pylint: disable=protected-access
        cls.setup_controllers([AlertManagerMockInstance, Prometheus, PrometheusReceiver])

    def test_list(self):
        self._get('/api/prometheus')
        self.assertStatus(200)

    def test_post_on_receiver(self):
        PrometheusReceiver.notifications = []
        self._post('/api/prometheus_receiver', {'name': 'foo'})
        self.assertEqual(len(PrometheusReceiver.notifications), 1)
        notification = PrometheusReceiver.notifications[0]
        self.assertEqual(notification['name'], 'foo')
        self.assertTrue(len(notification['notified']) > 20)

    def test_get_last_notification_with_empty_notifications(self):
        PrometheusReceiver.notifications = []
        self._post('/api/prometheus_receiver', {'name': 'foo'})
        self._post('/api/prometheus_receiver', {'name': 'bar'})
        self._post('/api/prometheus/get_notifications_since', {})
        self.assertStatus(200)
        last = PrometheusReceiver.notifications[1]
        self.assertEqual(self.jsonBody(), [last])

    def test_get_no_notification_since_with_last_notification(self):
        PrometheusReceiver.notifications = []
        self._post('/api/prometheus_receiver', {'name': 'foo'})
        notification = PrometheusReceiver.notifications[0]
        self._post('/api/prometheus/get_notifications_since', notification)
        self.assertBody('[]')

    def test_get_empty_list_with_no_notifications(self):
        PrometheusReceiver.notifications = []
        self._post('/api/prometheus/get_notifications_since', {})
        self.assertEqual(self.jsonBody(), [])

    def test_get_notifications_since_last_notification(self):
        PrometheusReceiver.notifications = []
        self._post('/api/prometheus_receiver', {'name': 'foobar'})
        next_to_last = PrometheusReceiver.notifications[0]
        self._post('/api/prometheus_receiver', {'name': 'foo'})
        self._post('/api/prometheus_receiver', {'name': 'bar'})
        self._post('/api/prometheus/get_notifications_since', next_to_last)
        foreLast = PrometheusReceiver.notifications[1]
        last = PrometheusReceiver.notifications[2]
        self.assertEqual(self.jsonBody(), [foreLast, last])
