
from typing import Dict, List, Optional, Tuple

import cherrypy
from orchestrator import HardwareNoReport

from ..model.hardware import HardwareHostResponse
from ..plugins.ttl_cache import ttl_cache
from ..services._paginate import ListPaginator
from ..services.exception import handle_custom_error, handle_orchestrator_error
from ..services.hardware import HardwareService
from ..services.orchestrator import OrchClient
from . import APIDoc, APIRouter, EndpointDoc, Param, RESTController
from ._version import APIVersion
from .orchestrator import raise_if_no_orchestrator

HARDWARE_CATEGORY_SCHEMA = {
    'health': (str, 'Worst health of the components: OK, Unknown, Warning or Critical'),
    'total': (int, 'Number of components'),
    'ok': (int, 'Components with health OK'),
    'warning': (int, 'Components with health Warning'),
    'critical': (int, 'Components with health Critical'),
    'unknown': (int, 'Components whose health is not reported'),
    'components': ([{
        'id': (str, 'Component id'),
        'chassis_id': (str, 'Chassis id'),
        'health': (str, 'OK, Unknown, Warning or Critical'),
        'state': (str, 'Redfish state, e.g. Enabled'),
    }], 'Components; each also carries the fields its category reports'),
}

HARDWARE_HOST_SCHEMA = {
    'hostname': (str, 'Host name'),
    'sn': (str, 'Serial number'),
    'health': (str, 'Worst health of the categories that have components'),
    'status': ({
        category: (HARDWARE_CATEGORY_SCHEMA, f'{category} components')
        for category in ('storage', 'processors', 'memory', 'network', 'power', 'fans',
                         'temperatures')
    }, 'Hardware per category'),
    'firmware': (dict, 'Installed firmware: name -> version'),
}


@APIRouter('/hardware')
@APIDoc("Hardware management API", "Hardware")
class Hardware(RESTController):

    @RESTController.Collection('GET', version=APIVersion.EXPERIMENTAL)
    @EndpointDoc("Retrieve a summary of the hardware health status")
    def summary(self, categories: Optional[List[str]] = None, hostname: Optional[List[str]] = None):
        """
        Get the health status of as many hardware categories, or all of them if none is given
        :param categories: The hardware type, all of them by default
        :param hostname: The host to retrieve from, all of them by default
        """
        return HardwareService.get_summary(categories, hostname)


@APIRouter('/hardware/compression')
@APIDoc("Hardware compression API", "Hardware")
class HardwareCompression(RESTController):

    @EndpointDoc("Retrieve cluster-wide FCM hardware compression statistics")
    def list(self):
        return HardwareService.get_compression()


@APIRouter('/hardware/hosts')
@APIDoc("Hardware hosts API", "Hardware")
class HardwareHosts(RESTController):

    @EndpointDoc(
        "List the hardware of all hosts with node-proxy data, paginated.",
        parameters=ListPaginator.doc(default_limit=5),
        responses={200: [HARDWARE_HOST_SCHEMA]},
    )
    @raise_if_no_orchestrator()
    @handle_orchestrator_error('hardware')
    def list(self, offset: int = 0, limit: int = 5, sort: str = '',
             search: str = '') -> List[Dict[str, object]]:
        hosts, total = _hosts_page(int(offset), int(limit), sort, search)
        cherrypy.response.headers['X-Total-Count'] = total
        return hosts

    @EndpointDoc(
        "Get the hardware of a single host, with per-component data for all categories.",
        parameters={
            'hostname': Param(str, 'The name of the host to retrieve hardware detail for', False),
        },
        responses={200: HARDWARE_HOST_SCHEMA},
    )
    @raise_if_no_orchestrator()
    @handle_orchestrator_error('hardware')
    @handle_custom_error('Hardware', http_status_code=404, exceptions=(HardwareNoReport,))
    def get(self, hostname: str) -> Dict[str, object]:
        return HardwareHostResponse.from_raw(
            OrchClient.instance().hardware.fullreport(hostname)).to_dict()


# node-proxy reports change every few minutes, while every open dashboard
# tab polls this list; cache each page briefly instead of calling cephadm each time.
@ttl_cache(30, label='hardware_hosts')
def _hosts_page(offset: int, limit: int, sort: str,
                search: str) -> Tuple[List[Dict[str, object]], int]:
    hardware = OrchClient.instance().hardware

    # Paginate the cheap hostname list first, then fetch the reports of this page
    # in one call: the cost grows with the page size, not the cluster size.
    paginator = ListPaginator(
        offset, limit, sort, search,
        input_list=[{'hostname': h} for h in hardware.list_hosts()],
        searchable_params=['hostname'],
        sortable_params=['hostname'],
        default_sort='+hostname',
    )
    hostnames = [ref['hostname'] for ref in paginator.list()]
    reports = hardware.fullreports(hostnames)
    # A host removed after list_hosts() is missing from reports and skipped.
    hosts = [HardwareHostResponse.from_raw(reports[hostname]).to_dict()
             for hostname in hostnames if hostname in reports]
    # The paginator only knows the filtered count once list() is consumed.
    return hosts, paginator.get_count()
