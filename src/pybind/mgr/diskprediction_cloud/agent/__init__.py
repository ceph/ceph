from __future__ import absolute_import

from ..common import timeout, TimeoutError


class BaseAgent(object):

    measurement = ''

    def __init__(self, mgr_module, obj_sender, timeout=30):
        self.data = []
        self._client = None
        self._client = obj_sender
        self._logger = mgr_module.log
        self._module_inst = mgr_module
        self._timeout = timeout

    def run(self):
        try:
            self._collect_data()
            self._run()
        except TimeoutError:
            self._logger.error('{} failed to execute {} task'.format(
                __name__, self.measurement))

    def __nonzero__(self):
        if not self._module_inst:
            return False
        else:
            return True

    @timeout
    def _run(self):
        pass

    @timeout
    def _collect_data(self):
        pass
