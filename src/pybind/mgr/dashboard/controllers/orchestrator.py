# -*- coding: utf-8 -*-
from __future__ import absolute_import

from functools import wraps

from ..exceptions import DashboardException
from ..services.orchestrator import OrchClient
from . import ApiController, ControllerDoc, Endpoint, EndpointDoc, ReadPermission, RESTController

STATUS_SCHEMA = {
    "available": (bool, "Orchestrator status"),
    "message": (str, "Error message")
}


def raise_if_no_orchestrator(features=None):
    def inner(method):
        @wraps(method)
        def _inner(self, *args, **kwargs):
            orch = OrchClient.instance()
            if not orch.available():
                raise DashboardException(code='orchestrator_status_unavailable',  # pragma: no cover
                                         msg='Orchestrator is unavailable',
                                         component='orchestrator',
                                         http_status_code=503)
            if features is not None:
                missing = orch.get_missing_features(features)
                if missing:
                    msg = 'Orchestrator feature(s) are unavailable: {}'.format(', '.join(missing))
                    raise DashboardException(code='orchestrator_features_unavailable',
                                             msg=msg,
                                             component='orchestrator',
                                             http_status_code=503)
            return method(self, *args, **kwargs)
        return _inner
    return inner


@ApiController('/orchestrator')
@ControllerDoc("Orchestrator Management API", "Orchestrator")
class Orchestrator(RESTController):

    @Endpoint()
    @ReadPermission
    @EndpointDoc("Display Orchestrator Status",
                 responses={200: STATUS_SCHEMA})
    def status(self):
        return OrchClient.instance().status()
