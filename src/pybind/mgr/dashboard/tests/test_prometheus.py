# -*- coding: utf-8 -*-
# pylint: disable=protected-access
from . import ControllerTestCase
from .. import mgr
from ..controllers import BaseController, Controller
from ..controllers.prometheus import Prometheus, PrometheusReceiver, PrometheusNotifications


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
        Prometheus._cp_config['tools.authenticate.on'] = False
        PrometheusNotifications._cp_config['tools.authenticate.on'] = False
        cls.setup_controllers([AlertManagerMockInstance, Prometheus,
                               PrometheusNotifications, PrometheusReceiver])

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

    def test_get_empty_list_with_no_notifications(self):
        PrometheusReceiver.notifications = []
        self._get('/api/prometheus/notifications')
        self.assertStatus(200)
        self.assertJsonBody([])
        self._get('/api/prometheus/notifications?from=last')
        self.assertStatus(200)
        self.assertJsonBody([])

    def test_get_all_notification(self):
        PrometheusReceiver.notifications = []
        self._post('/api/prometheus_receiver', {'name': 'foo'})
        self._post('/api/prometheus_receiver', {'name': 'bar'})
        self._get('/api/prometheus/notifications')
        self.assertStatus(200)
        self.assertJsonBody(PrometheusReceiver.notifications)

    def test_get_last_notification_with_use_of_last_keyword(self):
        PrometheusReceiver.notifications = []
        self._post('/api/prometheus_receiver', {'name': 'foo'})
        self._post('/api/prometheus_receiver', {'name': 'bar'})
        self._get('/api/prometheus/notifications?from=last')
        self.assertStatus(200)
        last = PrometheusReceiver.notifications[1]
        self.assertJsonBody([last])

    def test_get_no_notification_with_unknown_id(self):
        PrometheusReceiver.notifications = []
        self._post('/api/prometheus_receiver', {'name': 'foo'})
        self._post('/api/prometheus_receiver', {'name': 'bar'})
        self._get('/api/prometheus/notifications?from=42')
        self.assertStatus(200)
        self.assertJsonBody([])

    def test_get_no_notification_since_with_last_notification(self):
        PrometheusReceiver.notifications = []
        self._post('/api/prometheus_receiver', {'name': 'foo'})
        notification = PrometheusReceiver.notifications[0]
        self._get('/api/prometheus/notifications?from=' + notification['id'])
        self.assertStatus(200)
        self.assertJsonBody([])

    def test_get_notifications_since_last_notification(self):
        PrometheusReceiver.notifications = []
        self._post('/api/prometheus_receiver', {'name': 'foobar'})
        next_to_last = PrometheusReceiver.notifications[0]
        self._post('/api/prometheus_receiver', {'name': 'foo'})
        self._post('/api/prometheus_receiver', {'name': 'bar'})
        self._get('/api/prometheus/notifications?from=' + next_to_last['id'])
        foreLast = PrometheusReceiver.notifications[1]
        last = PrometheusReceiver.notifications[2]
        self.assertEqual(self.jsonBody(), [foreLast, last])
