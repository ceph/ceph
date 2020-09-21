from .helper import DashboardTestCase, JObj


class TelemetryTest(DashboardTestCase):

    pre_enabled_status = True

    @classmethod
    def setUpClass(cls):
        super(TelemetryTest, cls).setUpClass()
        data = cls._get('/api/mgr/module/telemetry')
        cls.pre_enabled_status = data['enabled']

    @classmethod
    def tearDownClass(cls):
        if cls.pre_enabled_status:
            cls._enable_module()
        else:
            cls._disable_module()
        super(TelemetryTest, cls).tearDownClass()

    def test_disable_module(self):
        self._enable_module()
        self._check_telemetry_enabled(True)
        self._disable_module()
        self._check_telemetry_enabled(False)

    def test_enable_module_correct_license(self):
        self._disable_module()
        self._check_telemetry_enabled(False)

        self._put('/api/telemetry', {
            'enable': True,
            'license_name': 'sharing-1-0'
        })
        self.assertStatus(200)
        self._check_telemetry_enabled(True)

    def test_enable_module_empty_license(self):
        self._disable_module()
        self._check_telemetry_enabled(False)

        self._put('/api/telemetry', {
            'enable': True,
            'license_name': ''
        })
        self.assertStatus(400)
        self.assertError(code='telemetry_enable_license_missing')
        self._check_telemetry_enabled(False)

    def test_enable_module_invalid_license(self):
        self._disable_module()
        self._check_telemetry_enabled(False)

        self._put('/api/telemetry', {
            'enable': True,
            'license_name': 'invalid-license'
        })
        self.assertStatus(400)
        self.assertError(code='telemetry_enable_license_missing')
        self._check_telemetry_enabled(False)

    def test_get_report(self):
        self._enable_module()
        data = self._get('/api/telemetry/report')
        self.assertStatus(200)
        schema = JObj({
            'report': JObj({}, allow_unknown=True),
            'device_report': JObj({}, allow_unknown=True)
        })
        self.assertSchema(data, schema)

    @classmethod
    def _enable_module(cls):
        cls._put('/api/telemetry', {
            'enable': True,
            'license_name': 'sharing-1-0'
        })

    @classmethod
    def _disable_module(cls):
        cls._put('/api/telemetry', {
            'enable': False
        })

    def _check_telemetry_enabled(self, enabled):
        data = self._get('/api/mgr/module/telemetry')
        self.assertStatus(200)
        self.assertEqual(data['enabled'], enabled)
