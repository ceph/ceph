from . import ControllerTestCase
from ..controllers.grafana import Grafana
from .. import mgr


class GrafanaTest(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        settings = {
            'GRAFANA_API_URL': 'http://localhost:3000'
        }
        mgr.get_module_option.side_effect = settings.get
        # pylint: disable=protected-access
        Grafana._cp_config['tools.authenticate.on'] = False
        cls.setup_controllers([Grafana])

    def test_url(self):
        self._get('/api/grafana/url')
        self.assertStatus(200)
        self.assertJsonBody({'instance': 'http://localhost:3000'})

    def test_validation(self):
        self._get('/api/grafana/validation/foo')
        self.assertStatus(500)
