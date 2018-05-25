from __future__ import absolute_import

from . import MetricsAgent


class Net_Agent(MetricsAgent):
    measurement = 'net'

    def _collect_data(self):
        # process data and save to 'self.data'
        pass
