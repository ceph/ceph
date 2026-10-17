
from typing import List, Optional

from ..services.hardware import HardwareService
from . import APIDoc, APIRouter, EndpointDoc, Param, RESTController
from ._version import APIVersion


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


@APIRouter('/hardware/hosts')
@APIDoc("Hardware hosts API", "Hardware")
class HardwareHosts(RESTController):

    @EndpointDoc(
        "List all hosts with hardware health summary, paginated.",
        parameters={
            'page': Param(int, 'Page number (1-based)', True, 1),
            'per_page': Param(int, 'Number of hosts per page', True, 10),
        }
    )
    def list(self, page: int = 1, per_page: int = 10):
        return HardwareService.get_hosts(int(page), int(per_page))

    @EndpointDoc(
        "Get full hardware detail for a single host, with per-component data for all categories.",
        parameters={
            'hostname': Param(str, 'The name of the host to retrieve hardware detail for', False),
        }
    )
    def get(self, hostname: str):
        return HardwareService.get_host_detail(hostname)
