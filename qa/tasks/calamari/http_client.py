#!/usr/bin/env python

import json
import logging
import requests

log = logging.getLogger(__name__)


class AuthenticatedHttpClient(requests.Session):
    """
    Client for the calamari REST API, principally exists to do
    authentication, but also helpfully prefixes
    URLs in requests with the API base URL and JSONizes
    POST data.
    """
    def __init__(self, api_url, username, password):
        super(AuthenticatedHttpClient, self).__init__()
        self._username = username
        self._password = password
        self._api_url = api_url
        self.headers = {
            'Content-type': "application/json; charset=UTF-8"
        }

    def request(self, method, url, **kwargs):
        if not url.startswith('/'):
            url = self._api_url + url
        response = super(AuthenticatedHttpClient, self).request(method, url, **kwargs)
        if response.status_code >= 400:
            # For the benefit of test logs
            print "%s: %s" % (response.status_code, response.content)
        return response

    def post(self, url, data=None, **kwargs):
        if isinstance(data, dict):
            data = json.dumps(data)
        return super(AuthenticatedHttpClient, self).post(url, data, **kwargs)

    def patch(self, url, data=None, **kwargs):
        if isinstance(data, dict):
            data = json.dumps(data)
        return super(AuthenticatedHttpClient, self).patch(url, data, **kwargs)

    def login(self):
        """
        Authenticate with the Django auth system as
        it is exposed in the Calamari REST API.
        """
        log.info("Logging in as %s" % self._username)
        response = self.get("auth/login/")
        response.raise_for_status()
        self.headers['X-XSRF-TOKEN'] = response.cookies['XSRF-TOKEN']

        self.post("auth/login/", {
            'next': "/",
            'username': self._username,
            'password': self._password
        })
        response.raise_for_status()

        # Check we're allowed in now.
        response = self.get("cluster")
        response.raise_for_status()

if __name__ == "__main__":

    import argparse

    p = argparse.ArgumentParser()
    p.add_argument('-u', '--uri', default='http://mira035/api/v1/')
    p.add_argument('--user', default='admin')
    p.add_argument('--pass', dest='password', default='admin')
    args, remainder = p.parse_known_args()

    c = AuthenticatedHttpClient(args.uri, args.user, args.password)
    c.login()
    response = c.request('GET', ''.join(remainder)).json()
    print json.dumps(response, indent=2)
