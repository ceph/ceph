from . import UiApiController, RESTController, Endpoint


@UiApiController('uisetting')
class UiSettings(RESTController):

    RESOURCE_ID = "name"
    """Collect Grafana settings """
    GrafanaObj = {
        'name': 'grafana',
        'refresh_interval': 6,
        'timepicker': 'from=now-1h&to=now'
    }

    def _aggregate(self):
        listSetting = []
        listSetting.append(self.GrafanaObj)

        return listSetting

    def _updateGrafana(self, data):
        self.GrafanaObj = data

    @RESTController.Resource('PUT')
    def set(self, name, config):
        if name == 'grafana':
            self._updateGrafana(config)

    @Endpoint()
    def get_setting(self):
        return self._aggregate()
