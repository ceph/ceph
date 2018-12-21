# -*- coding: utf-8 -*-
from __future__ import absolute_import
import cherrypy
import json

from .. import logger
from ..settings import Settings


class FeatureToggleTool(cherrypy.Tool):
    FEATURE_TOGGLES = [
        'ENABLE_RBD_IMAGES',
        'ENABLE_RBD_MIRRORING',
        'ENABLE_RBD_ISCSI',
        'ENABLE_CEPHFS',
        'ENABLE_RGW'
    ]

    def __init__(self):
        super(FeatureToggleTool, self).__init__(
            'before_handler', self.is_disabled, priority=10)

    def _setup(self):
        super(FeatureToggleTool, self)._setup()
        cherrypy.request.hooks.attach('before_finalize',
                                      self.announce_feature_toggles,
                                      priority=10)

    def is_disabled(self):
        controller = cherrypy.request.handler.callable.__self__
        feature_enable_option = getattr(controller, '_feature_enable_option', None)

        if feature_enable_option and getattr(Settings, feature_enable_option, True) is False:
            logger.info("[FeatureToggle] Disabled: class: '%s', option: '%s'",
                        type(controller).__name__, feature_enable_option)
            raise cherrypy.HTTPError(501, "Feature disabled by option '{}'".format(
                feature_enable_option))

    def announce_feature_toggles(self):
        cherrypy.response.headers["X-Dashboard-FeatureToggles"] = {
            feature_enable_option: getattr(Settings, feature_enable_option, True) for
            feature_enable_option in self.FEATURE_TOGGLES}
