import cherrypy

from dashboard.exceptions import DashboardException
from dashboard.services.auth.oauth2 import OAuth2

from . import Endpoint, RESTController, Router


@Router('/auth/oauth2', secure=False)
class Oauth2(RESTController):

    @Endpoint(json_response=False, version=None)
    def login(self):
        if not OAuth2.enabled():
            raise DashboardException(500, msg='Failed to login: SSO OAuth2 is not enabled')

        token = OAuth2.token(cherrypy.request)
        if not token:
            raise cherrypy.HTTPError()

        raise cherrypy.HTTPRedirect(OAuth2.login_redirect_url(token))

    @Endpoint(json_response=False, version=None)
    def logout(self):
        if not OAuth2.enabled():
            raise DashboardException(500, msg='Failed to logout: SSO OAuth2 is not enabled')

        token = OAuth2.token(cherrypy.request)
        if not token:
            raise cherrypy.HTTPError()

        raise cherrypy.HTTPRedirect(OAuth2.logout_redirect_url(token))
