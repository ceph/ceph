# -*- coding: utf-8 -*-
from __future__ import absolute_import
import cherrypy

from .. import logger
from ..settings import Settings


class FeatureToggleTool(cherrypy.Tool):
    def __init__(self):
        super(FeatureToggleTool, self).__init__(
            'before_handler', self.is_disabled, priority=10)

    def is_disabled(self):
        controller = cherrypy.request.handler.callable.__self__
        feature_enable_option = getattr(controller, '_feature_enable_option', None)

        if feature_enable_option and getattr(Settings, feature_enable_option, True) is False:
            logger.info("[FeatureToggle] Disabled: class: '%s', option: '%s'",
                        type(controller).__name__, feature_enable_option)
            raise cherrypy.HTTPError(501, "Feature disabled by option '{}'".format(
                feature_enable_option))
