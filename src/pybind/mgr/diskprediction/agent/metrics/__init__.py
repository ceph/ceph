from __future__ import absolute_import

import time

from .. import BaseAgent

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
        if collect_data:
            status_info = dict()
            status_info['measurement'] = None
            status_info['success_count'] = 0
            status_info['failure_count'] = 0
            for dp_data in collect_data:
                measurement = dp_data.__class__.measurement
                if not status_info['measurement']:
                    status_info['measurement'] = measurement
                tag_list = []
                field_list = []
                for name in dp_data.tags:
                    tag = '{}={}'.format(name, dp_data.tags[name])
                    tag_list.append(tag)
                for name in dp_data.fields:
                    if dp_data.fields[name] is None:
                        continue
                    if isinstance(dp_data.fields[name], str):
                        field = '{}=\"{}\"'.format(name, dp_data.fields[name])
                    elif isinstance(dp_data.fields[name], bool):
                        field = '{}={}'.format(name, str(dp_data.fields[name]).lower())
                    elif (isinstance(dp_data.fields[name], int) or
                          isinstance(dp_data.fields[name], long)):
                        field = '{}={}i'.format(name, dp_data.fields[name])
                    else:
                        field = '{}={}'.format(name, dp_data.fields[name])
                    field_list.append(field)
                data = '{},{} {} {}'.format(
                    measurement,
                    ','.join(tag_list),
                    ','.join(field_list),
                    int(time.time() * 1000 * 1000 * 1000))
                try:
                    resp = self._client.send_info(data=data, measurement=measurement)
                    status_code = resp.status_code
                    if 200 <= status_code < 300:
                        self._logger.debug(
                            '%s send diskprediction api success(ret: %s)'
                            % (measurement, status_code))
                        status_info['success_count'] += 1
                    else:
                        self._logger.error(
                            'return code: %s, content: %s, data: %s' % (status_code, resp.content, data))
                        status_info['failure_count'] += 1
                except Exception as e:
                    status_info['failure_count'] += 1
                    self._logger.error(str(e))
            # show summary info
            self.log_summary(status_info)
            # write sub_agent buffer
            total_count = status_info['success_count'] + status_info['failure_count']
            if total_count:
                if status_info['success_count'] == 0:
                    result['last_result'] = 'Error'
                elif status_info['failure_count'] == 0:
                    result['last_result'] = 'OK'
                else:
                    result['last_result'] = 'Warning'
            return result
        result['last_result'] = 'Not_Ready'
        return result
