# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import ApiController, AuthRequired, RESTController
from .. import mgr


class PerfCounter(RESTController):
    service_type = None  # type: str

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
        schema_dict = mgr.get_perf_schema(self.service_type, str(service_id))
        schema = schema_dict["{}.{}".format(self.service_type, service_id)]
        counters = []

        for key, value in sorted(schema.items()):
            counter = dict()
            counter['name'] = str(key)
            counter['description'] = value['description']
            # pylint: disable=W0212
            if mgr._stattype_to_str(value['type']) == 'counter':
                counter['value'] = self._get_rate(
                    self.service_type, service_id, key)
                counter['unit'] = mgr._unit_to_str(value['units'])
            else:
                counter['value'] = self._get_latest(
                    self.service_type, service_id, key)
                counter['unit'] = ''
            counters.append(counter)

        return {
            'service': {
                'type': self.service_type,
                'id': str(service_id)
            },
            'counters': counters
        }


@ApiController('perf_counters/mds')
@AuthRequired()
class MdsPerfCounter(PerfCounter):
    service_type = 'mds'


@ApiController('perf_counters/mon')
@AuthRequired()
class MonPerfCounter(PerfCounter):
    service_type = 'mon'


@ApiController('perf_counters/osd')
@AuthRequired()
class OsdPerfCounter(PerfCounter):
    service_type = 'osd'


@ApiController('perf_counters/rgw')
@AuthRequired()
class RgwPerfCounter(PerfCounter):
    service_type = 'rgw'


@ApiController('perf_counters/rbd-mirror')
@AuthRequired()
class RbdMirrorPerfCounter(PerfCounter):
    service_type = 'rbd-mirror'


@ApiController('perf_counters/mgr')
@AuthRequired()
class MgrPerfCounter(PerfCounter):
    service_type = 'mgr'


@ApiController('perf_counters')
@AuthRequired()
class PerfCounters(RESTController):
    def list(self):
        return mgr.get_all_perf_counters()
