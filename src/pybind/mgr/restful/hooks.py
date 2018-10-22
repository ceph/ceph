from __future__ import absolute_import

from pecan.hooks import PecanHook

import traceback

from . import context

class ErrorHook(PecanHook):
    def on_error(self, stat, exc):
        context.instance.log.error(str(traceback.format_exc()))
