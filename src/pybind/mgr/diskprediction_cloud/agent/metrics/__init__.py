from __future__ import absolute_import

from .. import BaseAgent
from ...common import DP_MGR_STAT_FAILED, DP_MGR_STAT_WARNING, DP_MGR_STAT_OK

AGENT_VERSION = '1.0.0'


class MetricsField(object):
    def __init__(self):
        self.tags = {}
        self.fields = {}
        self.timestamp = None

    def __str__(self):
        return str({
            'tags': self.tags,
            'fields': self.fields,
            'timestamp': self.timestamp
        })


class MetricsAgent(BaseAgent):

    def log_summary(self, status_info):
        try:
            if status_info:
                measurement = status_info['measurement']
                success_count = status_info['success_count']
                failure_count = status_info['failure_count']
                total_count = success_count + failure_count
                display_string = \
                    '%s agent stats in total count: %s, success count: %s, failure count: %s.'
                self._logger.info(
                    display_string % (measurement, total_count, success_count, failure_count)
                )
        except Exception as e:
            self._logger.error(str(e))

    def _run(self):
        collect_data = self.data
        result = {}
        if collect_data and self._client:
            status_info = self._client.send_info(collect_data, self.measurement)
            # show summary info
            self.log_summary(status_info)
            # write sub_agent buffer
            total_count = status_info['success_count'] + status_info['failure_count']
            if total_count:
                if status_info['success_count'] == 0:
                    self._module_inst.status = \
                        {'status': DP_MGR_STAT_FAILED,
                         'reason': 'failed to send metrics data to the server'}
                elif status_info['failure_count'] == 0:
                    self._module_inst.status = \
                        {'status': DP_MGR_STAT_OK}
                else:
                    self._module_inst.status = \
                        {'status': DP_MGR_STAT_WARNING,
                         'reason': 'failed to send partial metrics data to the server'}
        return result
