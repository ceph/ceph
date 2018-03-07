# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .. import mgr
from ..tools import ApiController, AuthRequired, RESTController


class PerfCounter(RESTController):
    def __init__(self, service_type):
        self._service_type = service_type

    def _get_rate(self, daemon_type, daemon_name, stat):
        data = mgr.get_counter(daemon_type, daemon_name, stat)[stat]
        if data and len(data) > 1:
            return (data[-1][1] - data[-2][1]) / float(data[-1][0] - data[-2][0])
        return 0

    def _get_latest(self, daemon_type, daemon_name, stat):
        data = mgr.get_counter(daemon_type, daemon_name, stat)[stat]
        if data:
            return data[-1][1]
        return 0

    def get(self, service_id):
        schema = mgr.get_perf_schema(
            self._service_type, str(service_id)).values()[0]
        counters = []

        for key, value in sorted(schema.items()):
            counter = dict()
            counter['name'] = str(key)
            counter['description'] = value['description']
            # pylint: disable=W0212
            if mgr._stattype_to_str(value['type']) == 'counter':
                counter['value'] = self._get_rate(
                    self._service_type, service_id, key)
                counter['unit'] = mgr._unit_to_str(value['units'])
            else:
                counter['value'] = self._get_latest(
                    self._service_type, service_id, key)
                counter['unit'] = ''
            counters.append(counter)

        return {
            'service': {
                'type': self._service_type,
                'id': service_id
            },
            'counters': counters
        }


@ApiController('perf_counters')
@AuthRequired()
class PerfCounters(RESTController):
    def __init__(self):
        self.mds = PerfCounter('mds')
        self.mon = PerfCounter('mon')
        self.osd = PerfCounter('osd')
        self.rgw = PerfCounter('rgw')
        self.rbd_mirror = PerfCounter('rbd-mirror')
        self.mgr = PerfCounter('mgr')

    def list(self):
        counters = mgr.get_all_perf_counters()
        return counters
