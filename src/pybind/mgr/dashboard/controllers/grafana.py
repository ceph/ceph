# -*- coding: utf-8 -*-
from __future__ import absolute_import

import cherrypy
import requests
from six.moves.urllib.parse import urlparse  # pylint: disable=import-error

from . import ApiController, BaseController, Proxy, Endpoint
from .. import logger
from ..settings import Settings


class GrafanaRestClient(object):
    _instance = None

    def _raise_for_validation(self):
        msg = 'No {} found or misconfigured, please consult the ' \
              'documentation about how to configure Grafana for the dashboard.'

        o = urlparse(self._url)
        if not (o.netloc and o.scheme):
            raise LookupError(msg.format('URL'))

        auth_method = 'password' if self._token is None else 'token'
        if auth_method == 'password' and not all((self._user, self._password)):
            raise LookupError(msg.format('username and/or password'))
        elif auth_method == 'token' and not self._token:
            raise LookupError(msg.format('token'))

    def __init__(self, url, username=None, password=None, token=None):
        """
        :type url: str
        :type username: str
        :type password: str
        """
        self._url = url.rstrip('/')
        self._user = username
        self._password = password
        self._token = token

        self._raise_for_validation()

    @classmethod
    def instance(cls):
        """
        This method shall be used by default to create an instance and will use
        the settings to retrieve the required credentials.

        :rtype: GrafanaRestClient
        """
        if not cls._instance:
            kwargs = {}
            if Settings.GRAFANA_API_AUTH_METHOD.lower() == 'password':
                kwargs['username'] = Settings.GRAFANA_API_USERNAME
                kwargs['password'] = Settings.GRAFANA_API_PASSWORD
            elif Settings.GRAFANA_API_AUTH_METHOD.lower() == 'token':
                kwargs['token'] = Settings.GRAFANA_API_TOKEN
            else:
                raise LookupError('No or unknown authentication method '
                                  'provided. Please consult the documentation '
                                  'about how to configure the '
                                  'Grafana integration correctly.')
            cls._instance = GrafanaRestClient(Settings.GRAFANA_API_URL,
                                              **kwargs)

        return cls._instance

    def proxy_request(self, method, path, params, data):
        url = '{}/{}'.format(self._url, path.lstrip('/'))

        # Forwards some headers
        headers = {k: v for k, v in cherrypy.request.headers.items()
                   if k.lower() in ('content-type', 'accept')}

        auth = None
        if self._token:
            headers['Authorization'] = 'Bearer {}'.format(self._token)
        else:
            auth = (self._user, self._password)

        response = requests.request(
            method,
            url,
            params=params,
            data=data,
            headers=headers,
            auth=auth)
        logger.debug("proxying method=%s path=%s params=%s data=%s", method,
                     path, params, data)

        return response

    def is_service_online(self):
        try:
            response = self.instance().proxy_request('GET', '/', None, None)
            response.raise_for_status()
        except Exception as e:  # pylint: disable=broad-except
            logger.error(e)
            return False, str(e)

        return True, ''


@ApiController('/grafana')
class Grafana(BaseController):

    @Endpoint()
    def status(self):
        grafana = GrafanaRestClient.instance()
        available, msg = grafana.is_service_online()
        response = {'available': available}
        if msg:
            response['message'] = msg

        return response


@ApiController('/grafana/proxy')
class GrafanaProxy(BaseController):
    @Proxy()
    def __call__(self, path, **params):
        grafana = GrafanaRestClient.instance()
        method = cherrypy.request.method

        data = None
        if cherrypy.request.body.length:
            data = cherrypy.request.body.read()

        response = grafana.proxy_request(method, path, params, data)

        cherrypy.response.headers['Content-Type'] = response.headers[
            'Content-Type']

        return response.content
