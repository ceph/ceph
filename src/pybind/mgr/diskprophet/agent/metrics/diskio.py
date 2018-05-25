from __future__ import absolute_import

from . import MetricsAgent


class DiskIO_Agent(MetricsAgent):
    measurement = 'diskio'

    def _collect_data(self):
        # process data and save to 'self.data'
        pass
