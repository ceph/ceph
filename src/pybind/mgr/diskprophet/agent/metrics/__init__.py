from __future__ import absolute_import


from logging import Logger
import time

from .. import BaseAgent


class MetricsAgent(BaseAgent):

    def check_database(self):
        try:
            resp = self._command.show_database()
            data = resp.json()
            if resp.status_code >= 200 and resp.status_code < 300:
                rc = data.get('results', [])
                if rc:
                    series = rc[0].get('series', [])
                    if series:
                        values = series[0].get('values', [])
                        if self._command.dbname not in values:
                            self._command.create_database()
                        return True
            return False
        except Exception as e:
            self._logger.error(str(e))
            return False

    def show_summary(self, status_info):
        try:
            if status_info:
                measurement = status_info['measurement']
                success_count = status_info['success_count']
                failure_count = status_info['failure_count']
                total_count = success_count + failure_count
                display_string = \
                    "%s agent stats in total count: %s, success count: %s, failure count: %s."
                self._logger.info(
                    display_string % (measurement, total_count, success_count, failure_count)
                )
        except Exception as e:
            self._logger.error(str(e))

    def query_info(self, sql):
        return self._command.query_info(sql)

    def _run(self):
        collect_data = self.data
        result = {}
        if collect_data:
            status_info = {}
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
                    tag = '%s=%s' % (name, dp_data.tags[name])
                    tag_list.append(tag)
                for name in dp_data.fields:
                    if dp_data.fields[name] is None:
                        continue
                    if isinstance(dp_data.fields[name], str):
                        field = '%s=\"%s\"' % (name, dp_data.fields[name])
                    elif isinstance(dp_data.fields[name], bool):
                        field = '%s=%s' % (name, dp_data.fields[name])
                    elif (isinstance(dp_data.fields[name], int) or
                          isinstance(dp_data.fields[name], long)):
                        field = '%s=%si' % (name, dp_data.fields[name])
                    else:
                        field = '%s=%s' % (name, dp_data.fields[name])
                    field_list.append(field)
                data = "%s,%s %s %s" % (
                    measurement,
                    ','.join(tag_list),
                    ','.join(field_list),
                    int(time.time() * 1000 * 1000 * 1000))
                try:
                    resp = self._command.send_info(data)
                    status_code = resp.status_code
                    if status_code >= 200 and status_code < 300:
                        self._logger.debug(
                            "%s send diskprophet api success" % measurement)
                        status_info['success_count'] += 1
                    else:
                        self._logger.error(resp.content)
                        status_info['failure_count'] += 1
                except Exception as e:
                    status_info['failure_count'] += 1
                    self._logger.error(str(e))
            # show summary info
            self.show_summary(status_info)
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