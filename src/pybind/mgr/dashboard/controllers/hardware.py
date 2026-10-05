
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


@APIRouter('/hardware/compression')
@APIDoc("Hardware compression API", "Hardware")
class HardwareCompression(RESTController):

    @EndpointDoc(
        "Retrieve cluster-wide FCM hardware compression statistics. "
        "compression_ratio, savings_bytes and efficiency_percent are null when "
        "total physical used is below 100 GiB to avoid misleading values on "
        "fresh or lightly-loaded clusters."
    )
    def list(self):
        return HardwareService.get_compression()
