import logging
from typing import TYPE_CHECKING, Any, Dict, Tuple

import cherrypy

if TYPE_CHECKING:
    from cephadm.module import CephadmOrchestrator

logger = logging.getLogger(__name__)


class ACMEChallengeRoot:
    """CherryPy root serving ACME HTTP-01 challenge responses."""

    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr

    @cherrypy.expose
    def default(self, *args: str) -> str:
        # Expected public path:
        #   /.well-known/acme-challenge/<token>
        if len(args) != 3 or args[0] != '.well-known' or args[1] != 'acme-challenge':
            raise cherrypy.HTTPError(404)
        token = args[2]
        key_authorization = self.mgr.acme_mgr.get_http01_key_authorization(token)
        if not key_authorization:
            raise cherrypy.HTTPError(404)
        cherrypy.response.headers['Content-Type'] = 'text/plain'
        return key_authorization


class ACMEChallengesServer:
    """Small unauthenticated CherryPy server for ACME HTTP-01 challenges."""

    def __init__(self, mgr: "CephadmOrchestrator") -> None:
        self.mgr = mgr

    def configure(self) -> Tuple[Dict[str, Any], None]:
        config: Dict[str, Any] = {
            '/': {
                'environment': 'production',
                'tools.trailing_slash.on': False,
                'engine.autoreload.on': False,
                'request.show_tracebacks': False,
                'response.timeout': 30,
            }
        }
        return config, None
