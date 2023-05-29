# -*- coding: utf-8 -*-

from functools import wraps

from .. import mgr
from ..exceptions import DashboardException
from ..services.orchestrator import OrchClient
from . import APIDoc, Endpoint, EndpointDoc, ReadPermission, RESTController, UIRouter

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


@UIRouter('/orchestrator')
@APIDoc("Orchestrator Management API", "Orchestrator")
class Orchestrator(RESTController):

    @Endpoint()
    @ReadPermission
    @EndpointDoc("Display Orchestrator Status",
                 responses={200: STATUS_SCHEMA})
    def status(self):
        return OrchClient.instance().status()

    @Endpoint()
    def get_name(self):
        return mgr.get_module_option_ex('orchestrator', 'orchestrator')
