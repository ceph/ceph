# -*- coding: utf-8 -*-

import cherrypy

from .. import mgr
from ..security import Scope
from ..services.ceph_service import CephService
from . import APIDoc, APIRouter, EndpointDoc, RESTController

PERF_SCHEMA = {
    "mon.a": ({
        ".cache_bytes": ({
            "description": (str, ""),
            "nick": (str, ""),
            "type": (int, ""),
            "priority": (int, ""),
            "units": (int, ""),
            "value": (int, "")
        }, ""),
    }, "Service ID"),
}


class PerfCounter(RESTController):
    service_type = None  # type: str

    def get(self, service_id):
        try:
            return CephService.get_service_perf_counters(self.service_type, str(service_id))
        except KeyError as error:
            raise cherrypy.HTTPError(404, "{0} not found".format(error))


@APIRouter('perf_counters/mds', Scope.CEPHFS)
@APIDoc("Mds Perf Counters Management API", "MdsPerfCounter")
class MdsPerfCounter(PerfCounter):
    service_type = 'mds'


@APIRouter('perf_counters/mon', Scope.MONITOR)
@APIDoc("Mon Perf Counters Management API", "MonPerfCounter")
class MonPerfCounter(PerfCounter):
    service_type = 'mon'


@APIRouter('perf_counters/osd', Scope.OSD)
@APIDoc("OSD Perf Counters Management API", "OsdPerfCounter")
class OsdPerfCounter(PerfCounter):
    service_type = 'osd'


@APIRouter('perf_counters/rgw', Scope.RGW)
@APIDoc("Rgw Perf Counters Management API", "RgwPerfCounter")
class RgwPerfCounter(PerfCounter):
    service_type = 'rgw'


@APIRouter('perf_counters/rbd-mirror', Scope.RBD_MIRRORING)
@APIDoc("Rgw Mirroring Perf Counters Management API", "RgwMirrorPerfCounter")
class RbdMirrorPerfCounter(PerfCounter):
    service_type = 'rbd-mirror'


@APIRouter('perf_counters/mgr', Scope.MANAGER)
@APIDoc("Mgr Perf Counters Management API", "MgrPerfCounter")
class MgrPerfCounter(PerfCounter):
    service_type = 'mgr'


@APIRouter('perf_counters/tcmu-runner', Scope.ISCSI)
@APIDoc("Tcmu Runner Perf Counters Management API", "TcmuRunnerPerfCounter")
class TcmuRunnerPerfCounter(PerfCounter):
    service_type = 'tcmu-runner'


@APIRouter('perf_counters')
@APIDoc("Perf Counters Management API", "PerfCounters")
class PerfCounters(RESTController):
    @EndpointDoc("Display Perf Counters",
                 responses={200: PERF_SCHEMA})
    def list(self):
        return mgr.get_unlabeled_perf_counters()
