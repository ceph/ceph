# -*- coding: utf-8 -*-

import unittest

try:
    from mock import Mock, patch
except ImportError:
    from unittest.mock import Mock, patch

from ..plugins.feature_toggles import Actions, Features, FeatureToggles
from ..tests import KVStoreMockMixin


class SettingsTest(unittest.TestCase, KVStoreMockMixin):
    @classmethod
    def setUpClass(cls):
        cls.mock_kv_store()
        cls.CONFIG_KEY_DICT['url_prefix'] = ''

        # Mock MODULE_OPTIONS
        from .. import mgr
        cls.mgr = mgr

        # Populate real endpoint map
        from ..controllers import BaseController
        cls.controllers = BaseController.load_controllers()

        # Initialize FeatureToggles plugin
        cls.plugin = FeatureToggles()
        cls.CONFIG_KEY_DICT.update(
            {k['name']: k['default'] for k in cls.plugin.get_options()})
        cls.plugin.setup()

    def test_filter_request_when_all_features_enabled(self):
        """
        This test iterates over all the registered endpoints to ensure that, with default
        feature toggles, none is disabled.
        """
        import cherrypy

        request = Mock()
        for controller in self.controllers:
            request.path_info = controller.get_path()
            try:
                self.plugin.filter_request_before_handler(request)
            except cherrypy.HTTPError:
                self.fail("Request filtered {} and it shouldn't".format(
                    request.path_info))

    def test_filter_request_when_some_feature_enabled(self):
        """
        This test focuses on a single feature and checks whether it's actually
        disabled
        """
        import cherrypy

        self.plugin.register_commands()['handle_command'](
            self.mgr, Actions.DISABLE, [Features.CEPHFS])

        with patch.object(self.plugin, '_get_feature_from_request',
                          return_value=Features.CEPHFS):
            with self.assertRaises(cherrypy.HTTPError):
                request = Mock()
                self.plugin.filter_request_before_handler(request)
