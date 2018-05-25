from __future__ import absolute_import

from . import MetricsAgent


class CPU_Agent(MetricsAgent):
    measurement = 'cpu'

    def _collect_data(self):
        # process data and save to 'self.data'
        pass
