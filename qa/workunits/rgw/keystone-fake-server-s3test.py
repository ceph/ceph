#!/usr/bin/env python3
# Enhanced Keystone fake server for testing EC2Engine admin token retry.
#
# Simulates:
#   - POST /v3/auth/tokens         (admin token issuance)
#   - POST /v3/s3tokens            (EC2 signature validation)
#   - GET  /v3/users/{id}/credentials/OS-EC2/{access_key_id} (secret fetch)
#   - Fernet key rotation: invalidates the current admin token so that
#     s3tokens returns 401, forcing RGW to invalidate its cache and retry.

from datetime import datetime, timedelta
import logging
import json
import base64
import hashlib
import hmac
import threading
import os
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
        {'id': '51cc68287d524c759f47c811e6463340', 'name': 'admin'},
    ],
    'deadbeef': [
        {'id': '98bd32184f854f393a72b932g5334124', 'name': 'Member'},
    ],
}

# EC2 credentials: access_key_id -> (user_id, secret)
EC2_CREDS = {
    '04daaeb0960f46b9a2c9abbb25f1ad4a': {
        'user_id': '99gg485738df758349jf8d848g774392',
        'username': 'deadbeef',
        'project': 'deadbeef',
        'secret': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
    },
}

# Admin tokens: token_id -> valid (bool)
# After "Fernet rotation", the old admin token becomes invalid.
ADMIN_TOKENS = {
    'admin-token-1': True,
    'admin-token-2': True,
}

CURRENT_ADMIN_TOKEN = 'admin-token-1'
LOCK = threading.Lock()

# When True, s3tokens and credentials endpoints will return 401 for
# the old admin token, simulating a Fernet key rotation.
FERNET_ROTATED = False

COUNTERS = {
    'auth_post': 0,
    's3tokens_post': 0,
    'credentials_get': 0,
    's3tokens_401': 0,
    's3tokens_401_retry': 0,
    's3tokens_200': 0,
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
        expires_at = (now + timedelta(hours=24)).strftime('%Y-%m-%dT%H:%M:%SZ')

    return {
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


def _generate_s3token_result(access_key_id):
    cred = EC2_CREDS.get(access_key_id)
    if not cred:
        return None
    userdata = USERS[cred['username']]
    projectdata = PROJECTS[cred['project']]
    userroles = USERROLES[cred['username']]
    now = datetime.now()
    issued_at = now.strftime('%Y-%m-%dT%H:%M:%SZ')
    expires_at = (now + timedelta(hours=1)).strftime('%Y-%m-%dT%H:%M:%SZ')

    return {
        'token': {
            'audit_ids': ['3T2dc1CGQxyJsHdDu1xkcw'],
            'catalog': [],
            'expires_at': expires_at,
            'is_domain': False,
            'issued_at': issued_at,
            'methods': ['s3credentials'],
            'project': projectdata,
            'roles': userroles,
            'user': userdata,
        }
    }


class HTTPRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        logging.info("%s - %s" % (self.address_string(), format % args))

    def do_GET(self):
        if self.path == '/stats':
            self._handle_stats()
            return
        if self.path == '/rotate':
            self._handle_rotate()
            return
        if self.path.startswith('/v3/auth/tokens'):
            self._handle_get_auth()
            return
        if '/credentials/OS-EC2/' in self.path:
            self._handle_get_credentials()
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        if self.path == '/v3/auth/tokens':
            self._handle_post_auth()
            return
        if self.path == '/v3/s3tokens':
            self._handle_s3tokens()
            return
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
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        with LOCK:
            stats = dict(COUNTERS)
            stats['fernet_rotated'] = FERNET_ROTATED
            stats['current_admin_token'] = CURRENT_ADMIN_TOKEN
        self._set_data(stats)

    def _handle_rotate(self):
        global FERNET_ROTATED, CURRENT_ADMIN_TOKEN
        with LOCK:
            FERNET_ROTATED = True
            ADMIN_TOKENS['admin-token-1'] = False
            CURRENT_ADMIN_TOKEN = 'admin-token-2'
        logging.info("Fernet rotation simulated: admin-token-1 invalidated, admin-token-2 is now current")
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self._set_data({'status': 'rotated', 'new_token': 'admin-token-2'})

    def _is_valid_admin_token(self, token):
        with LOCK:
            return ADMIN_TOKENS.get(token, False)

    def _handle_get_auth(self):
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
                result = _generate_token_result(tokendata['username'],
                                                tokendata['project'],
                                                tokendata['expired'])
                self._set_data(result)
        else:
            self.send_response(404)
            self.end_headers()

    def _handle_post_auth(self):
        global COUNTERS
        with LOCK:
            COUNTERS['auth_post'] += 1
        data = self._get_data()
        user = data['auth']['identity']['password']['user']
        if user['name'] == 'admin' and user['password'] == 'ADMIN':
            with LOCK:
                token = CURRENT_ADMIN_TOKEN
            self.send_response(201)
            self.send_header('Content-Type', 'application/json')
            self.send_header('X-Subject-Token', token)
            self.end_headers()
            tokendata = TOKENS.get(token, TOKENS['admin-token-1'])
            result = _generate_token_result(tokendata['username'],
                                            tokendata['project'],
                                            tokendata['expired'])
            self._set_data(result)
        else:
            self.send_response(401)
            self.end_headers()

    def _handle_s3tokens(self):
        global COUNTERS
        with LOCK:
            COUNTERS['s3tokens_post'] += 1

        admin_token = self.headers.get('X-Auth-Token', '')
        logging.info("s3tokens: admin_token=%s, fernet_rotated=%s" % (admin_token, FERNET_ROTATED))

        # Check if admin token is valid
        if not self._is_valid_admin_token(admin_token):
            with LOCK:
                COUNTERS['s3tokens_401'] += 1
                if FERNET_ROTATED:
                    COUNTERS['s3tokens_401_retry'] += 1
            logging.info("s3tokens: returning 401 (admin token invalid: %s)" % admin_token)
            self.send_response(401)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self._set_data({'error': {'message': 'The request you have made requires authentication.',
                                       'code': 401, 'title': 'Unauthorized'}})
            return

        # Admin token is valid, validate the EC2 credentials
        data = self._get_data()
        cred = data.get('credentials', {})
        access_key_id = cred.get('access', '')
        string_to_sign_b64 = cred.get('token', '')
        signature = cred.get('signature', '')

        if access_key_id not in EC2_CREDS:
            self.send_response(404)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self._set_data({'error': {'message': 'Could not find access key.',
                                       'code': 404, 'title': 'Not Found'}})
            return

        # We don't fully verify the signature in the fake server,
        # we just return a valid token if the access key exists.
        result = _generate_s3token_result(access_key_id)
        if result:
            with LOCK:
                COUNTERS['s3tokens_200'] += 1
            logging.info("s3tokens: returning 200 for access_key=%s" % access_key_id)
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self._set_data(result)
        else:
            self.send_response(404)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self._set_data({'error': {'message': 'Could not find access key.',
                                       'code': 404, 'title': 'Not Found'}})

    def _handle_get_credentials(self):
        global COUNTERS
        with LOCK:
            COUNTERS['credentials_get'] += 1

        admin_token = self.headers.get('X-Auth-Token', '')
        logging.info("credentials: admin_token=%s, path=%s" % (admin_token, self.path))

        if not self._is_valid_admin_token(admin_token):
            logging.info("credentials: returning 401 (admin token invalid: %s)" % admin_token)
            self.send_response(401)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self._set_data({'error': {'message': 'The request you have made requires authentication.',
                                       'code': 401, 'title': 'Unauthorized'}})
            return

        # Extract access_key_id from path: /v3/users/{user_id}/credentials/OS-EC2/{access_key_id}
        parts = self.path.split('/')
        try:
            idx = parts.index('OS-EC2')
            access_key_id = parts[idx + 1]
        except (ValueError, IndexError):
            self.send_response(404)
            self.end_headers()
            return

        cred = EC2_CREDS.get(access_key_id)
        if cred:
            logging.info("credentials: returning 200 for access_key=%s" % access_key_id)
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self._set_data({'credential': {'secret': cred['secret'],
                                            'user_id': cred['user_id'],
                                            'access': access_key_id}})
        else:
            self.send_response(404)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self._set_data({'error': {'message': 'Could not find credential.',
                                       'code': 404, 'title': 'Not Found'}})


# Keep the old TOKENS dict for compatibility
TOKENS = {
    'admin-token-1': {
        'username': 'admin',
        'project': 'admin',
        'expired': False,
    },
    'admin-token-2': {
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


def main():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(levelname)s %(message)s')
    port = int(os.environ.get('KEYSTONE_PORT', '5000'))
    logging.info('Starting keystone-fake-server-s3test on port %d' % port)
    logging.info('Endpoints:')
    logging.info('  POST /v3/auth/tokens          - admin token issuance')
    logging.info('  POST /v3/s3tokens              - EC2 signature validation')
    logging.info('  GET  /v3/users/{id}/credentials/OS-EC2/{key} - secret fetch')
    logging.info('  GET  /rotate                   - simulate Fernet key rotation')
    logging.info('  GET  /stats                     - show counters')
    server = HTTPServer(('0.0.0.0', port), HTTPRequestHandler)
    server.serve_forever()


if __name__ == '__main__':
    main()
