# -*- coding: utf-8 -*-

import cherrypy

try:
    from onelogin.saml2.auth import OneLogin_Saml2_Auth
    from onelogin.saml2.errors import OneLogin_Saml2_Error
    from onelogin.saml2.settings import OneLogin_Saml2_Settings

    python_saml_imported = True
except ImportError:
    python_saml_imported = False

from .. import mgr
from ..exceptions import UserDoesNotExist
from ..services.auth import JwtManager
from ..tools import prepare_url_prefix
from . import BaseController, ControllerAuthMixin, Endpoint, Router, allow_empty_body


@Router('/auth/saml2', secure=False)
class Saml2(BaseController, ControllerAuthMixin):

    @staticmethod
    def _build_req(request, post_data):
        return {
            'https': 'on' if request.scheme == 'https' else 'off',
            'http_host': request.host,
            'script_name': request.path_info,
            'server_port': str(request.port),
            'get_data': {},
            'post_data': post_data
        }

    @staticmethod
    def _check_python_saml():
        if not python_saml_imported:
            raise cherrypy.HTTPError(400, 'Required library not found: `python3-saml`')
        try:
            OneLogin_Saml2_Settings(mgr.SSO_DB.saml2.onelogin_settings)
        except OneLogin_Saml2_Error:
            raise cherrypy.HTTPError(400, 'Single Sign-On is not configured.')

    @Endpoint('POST', path="", version=None)
    @allow_empty_body
    def auth_response(self, **kwargs):
        Saml2._check_python_saml()
        req = Saml2._build_req(self._request, kwargs)
        auth = OneLogin_Saml2_Auth(req, mgr.SSO_DB.saml2.onelogin_settings)
        auth.process_response()
        errors = auth.get_errors()

        if auth.is_authenticated():
            JwtManager.reset_user()
            username_attribute = auth.get_attribute(mgr.SSO_DB.saml2.get_username_attribute())
            if username_attribute is None:
                raise cherrypy.HTTPError(400,
                                         'SSO error - `{}` not found in auth attributes. '
                                         'Received attributes: {}'
                                         .format(
                                             mgr.SSO_DB.saml2.get_username_attribute(),
                                             auth.get_attributes()))
            username = username_attribute[0]
            url_prefix = prepare_url_prefix(mgr.get_module_option('url_prefix', default=''))
            try:
                mgr.ACCESS_CTRL_DB.get_user(username)
            except UserDoesNotExist:
                raise cherrypy.HTTPRedirect("{}/#/sso/404".format(url_prefix))

            token = JwtManager.gen_token(username)
            JwtManager.set_user(JwtManager.decode_token(token))

            # For backward-compatibility: PyJWT versions < 2.0.0 return bytes.
            token = token.decode('utf-8') if isinstance(token, bytes) else token

            self._set_token_cookie(url_prefix, token)
            raise cherrypy.HTTPRedirect("{}/#/login?access_token={}".format(url_prefix, token))

        return {
            'is_authenticated': auth.is_authenticated(),
            'errors': errors,
            'reason': auth.get_last_error_reason()
        }

    @Endpoint(xml=True, version=None)
    def metadata(self):
        Saml2._check_python_saml()
        saml_settings = OneLogin_Saml2_Settings(mgr.SSO_DB.saml2.onelogin_settings)
        return saml_settings.get_sp_metadata()

    @Endpoint(json_response=False, version=None)
    def login(self):
        Saml2._check_python_saml()
        req = Saml2._build_req(self._request, {})
        auth = OneLogin_Saml2_Auth(req, mgr.SSO_DB.saml2.onelogin_settings)
        raise cherrypy.HTTPRedirect(auth.login())

    @Endpoint(json_response=False, version=None)
    def slo(self):
        Saml2._check_python_saml()
        req = Saml2._build_req(self._request, {})
        auth = OneLogin_Saml2_Auth(req, mgr.SSO_DB.saml2.onelogin_settings)
        raise cherrypy.HTTPRedirect(auth.logout())

    @Endpoint(json_response=False, version=None)
    def logout(self, **kwargs):
        # pylint: disable=unused-argument
        Saml2._check_python_saml()
        JwtManager.reset_user()
        token = JwtManager.get_token_from_header()
        self._delete_token_cookie(token)
        url_prefix = prepare_url_prefix(mgr.get_module_option('url_prefix', default=''))
        raise cherrypy.HTTPRedirect("{}/#/login".format(url_prefix))
