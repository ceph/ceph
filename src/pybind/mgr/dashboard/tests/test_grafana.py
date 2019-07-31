from . import ControllerTestCase
from ..controllers.grafana import Grafana
from .. import mgr


class GrafanaTest(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        cls.server_settings()
        # pylint: disable=protected-access
        Grafana._cp_config['tools.authenticate.on'] = False
        cls.setup_controllers([Grafana])

    @classmethod
    def server_settings(
            cls,
            url='http://localhost:3000',
            user='admin',
            password='admin',
    ):
        settings = dict()
        if url is not None:
            settings['GRAFANA_API_URL'] = url
        if user is not None:
            settings['GRAFANA_API_USERNAME'] = user
        if password is not None:
            settings['GRAFANA_API_PASSWORD'] = password
        mgr.get_module_option.side_effect = settings.get

    def test_url(self):
        self.server_settings()
        self._get('/api/grafana/url')
        self.assertStatus(200)
        self.assertJsonBody({'instance': 'http://localhost:3000'})

    def test_validation(self):
        self.server_settings()
        self._get('/api/grafana/validation/foo')
        self.assertStatus(500)

    def test_dashboards(self):
        self.server_settings(url=None)
        self._post('/api/grafana/dashboards')
        self.assertStatus(500)
        self.server_settings(user=None)
        self._post('/api/grafana/dashboards')
        self.assertStatus(500)
        self.server_settings(password=None)
        self._post('/api/grafana/dashboards')
        self.assertStatus(500)
