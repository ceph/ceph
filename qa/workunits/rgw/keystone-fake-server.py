#!/usr/bin/env python3
#
# Copyright (C) 2022 Binero
#
# Author: Tobias Urdin <tobias.urdin@binero.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.

from datetime import datetime, timedelta
import logging
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


DEFAULT_DOMAIN = {
    'id': 'default',
    'name': 'Default',
}


PROJECTS = {
    'admin': {
        'domain': DEFAULT_DOMAIN,
        'id': 'a6944d763bf64ee6a275f1263fae0352',
        'name': 'admin',
    },
    'deadbeef': {
        'domain': DEFAULT_DOMAIN,
        'id': 'b4221c214dd64ee6a464g2153fae3813',
        'name': 'deadbeef',
    },
}


USERS = {
    'admin': {
        'domain': DEFAULT_DOMAIN,
        'id': '51cc68287d524c759f47c811e6463340',
        'name': 'admin',
    },
    'deadbeef': {
        'domain': DEFAULT_DOMAIN,
        'id': '99gg485738df758349jf8d848g774392',
        'name': 'deadbeef',
    },
}


USERROLES = {
    'admin': [
        {
            'id': '51cc68287d524c759f47c811e6463340',
            'name': 'admin',
        }
    ],
    'deadbeef': [
        {
            'id': '98bd32184f854f393a72b932g5334124',
            'name': 'Member',
        }
    ],
}


TOKENS = {
    'admin-token-1': {
        'username': 'admin',
        'project': 'admin',
        'expired': False,
    },
    'user-token-1': {
        'username': 'deadbeef',
        'project': 'deadbeef',
        'expired': False,
    },
    'user-token-2': {
        'username': 'deadbeef',
        'project': 'deadbeef',
        'expired': True,
    },
}


def _generate_token_result(username, project, expired=False):
    userdata = USERS[username]
    projectdata = PROJECTS[project]
    userroles = USERROLES[username]

    if expired:
        then = datetime.now() - timedelta(hours=2)
        issued_at = then.strftime('%Y-%m-%dT%H:%M:%SZ')
        expires_at = (then + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')
    else:
        now = datetime.now()
        issued_at = now.strftime('%Y-%m-%dT%H:%M:%SZ')
        expires_at = (now + timedelta(seconds=10)).strftime('%Y-%m-%dT%H:%M:%SZ')

    result = {
        'token': {
            'audit_ids': ['3T2dc1CGQxyJsHdDu1xkcw'],
            'catalog': [],
            'expires_at': expires_at,
            'is_domain': False,
            'issued_at': issued_at,
            'methods': ['password'],
            'project': projectdata,
            'roles': userroles,
            'user': userdata,
        }
    }

    return result


COUNTERS = {
    'get_total': 0,
    'post_total': 0,
}


class HTTPRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        # This is not part of the Keystone API
        if self.path == '/stats':
            self._handle_stats()
            return

        if str(self.path).startswith('/v3/auth/tokens'):
            self._handle_get_auth()
        else:
            self.send_response(403)
            self.end_headers()

    def do_POST(self):
        if self.path == '/v3/auth/tokens':
            self._handle_post_auth()
        else:
            self.send_response(400)
            self.end_headers()

    def _get_data(self):
        length = int(self.headers.get('content-length'))
        data = self.rfile.read(length).decode('utf8')
        return json.loads(data)

    def _set_data(self, data):
        jdata = json.dumps(data)
        self.wfile.write(jdata.encode('utf8'))

    def _handle_stats(self):
        self.send_response(200)
        self.end_headers()
        self._set_data(COUNTERS)

    def _handle_get_auth(self):
        logging.info('Increasing get_total counter from %d -> %d' % (COUNTERS['get_total'], COUNTERS['get_total']+1))
        COUNTERS['get_total'] += 1
        auth_token = self.headers.get('X-Subject-Token', None)
        if auth_token and auth_token in TOKENS:
            tokendata = TOKENS[auth_token]
            if tokendata['expired'] and 'allow_expired=1' not in self.path:
                self.send_response(404)
                self.end_headers()
            else:
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                result = _generate_token_result(tokendata['username'], tokendata['project'], tokendata['expired'])
                self._set_data(result)
        else:
            self.send_response(404)
            self.end_headers()

    def _handle_post_auth(self):
        logging.info('Increasing post_total counter from %d -> %d' % (COUNTERS['post_total'], COUNTERS['post_total']+1))
        COUNTERS['post_total'] += 1
        data = self._get_data()
        user = data['auth']['identity']['password']['user']
        if user['name'] == 'admin' and user['password'] == 'ADMIN':
            self.send_response(201)
            self.send_header('Content-Type', 'application/json')
            self.send_header('X-Subject-Token', 'admin-token-1')
            self.end_headers()
            tokendata = TOKENS['admin-token-1']
            result = _generate_token_result(tokendata['username'], tokendata['project'], tokendata['expired'])
            self._set_data(result)
        else:
            self.send_response(401)
            self.end_headers()


def main():
    logging.basicConfig(level=logging.DEBUG)
    logging.info('Starting keystone-fake-server')
    server = HTTPServer(('localhost', 5000), HTTPRequestHandler)
    server.serve_forever()


if __name__ == '__main__':
    main()
