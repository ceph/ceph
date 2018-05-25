from __future__ import absolute_import

from . import MetricsAgent


class MEM_Agent(MetricsAgent):
    measurement = 'mem'

    def _collect_data(self):
        # process data and save to 'self.data'
        pass
