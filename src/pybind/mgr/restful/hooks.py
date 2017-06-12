from pecan.hooks import PecanHook

import traceback

import module

class ErrorHook(PecanHook):
    def on_error(self, stat, exc):
        module.instance.log.error(str(traceback.format_exc()))
