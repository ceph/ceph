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

        user = OAuth2.get_user_from_headers(cherrypy.request)
        if not user:
            raise cherrypy.HTTPError(401, 'Missing proxy authentication headers')

        raise cherrypy.HTTPRedirect(OAuth2.get_login_redirect_url())

    @Endpoint(json_response=False, version=None)
    def logout(self):
        if not OAuth2.enabled():
            raise DashboardException(500, msg='Failed to logout: SSO OAuth2 is not enabled')

        raise cherrypy.HTTPRedirect(OAuth2.get_logout_redirect_url())
