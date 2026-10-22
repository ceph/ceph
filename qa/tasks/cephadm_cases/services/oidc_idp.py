#!/usr/bin/env python3
"""
Minimal OpenID Connect identity provider for the cephadm service tests.

It stands in for a real IdP (Keycloak, Dex...) so the oauth2-proxy tests
need nothing beyond the Ceph container image: it only uses the standard
library and python3-cryptography, both shipped in that image. The tests run
it as a cephadm custom container with the cluster's own Ceph image.

What it implements, enough for oauth2-proxy's "oidc" provider and the Ceph
dashboard:

  /.well-known/openid-configuration   discovery document
  /keys                               JWKS (one RSA key, generated at start)
  /auth                               authorization endpoint: a login form,
                                      then a redirect with ?code=&state=
  /token                              authorization_code (+PKCE S256/plain),
                                      refresh_token and password grants;
                                      client_secret_basic or _post
  /userinfo                           claims for a bearer access token
                                      (refused once expired)
  /logout                             end-session endpoint

ID and access tokens are RS256 JWTs carrying iss, sub, aud, exp, iat, jti,
email (verified), name, preferred_username, groups and roles, plus nonce on
the ID token when the client sent one.

Redirect URIs: a client accepts any https URI whose path is its
'redirect_path' (default /oauth2/callback), so the tests need not guess
every name cephadm may build the callback URL from. The tests assert on the
callback URL themselves.

Usage: oidc_idp.py CONFIG.json

  {
    "issuer": "https://10.0.0.3:5556",
    "listen": "0.0.0.0:5556",
    "tls_cert": "/etc/idp/tls.crt", "tls_key": "/etc/idp/tls.key",
    "clients": [{"id": "...", "secret": "...", "redirect_path": "/oauth2/callback"}],
    "users": [{"username": "...", "email": "...", "password": "...",
               "groups": [], "roles": []}]
  }

Every request is logged to stderr (the daemon's journal).
"""

import base64
import hashlib
import html
import http.server
import json
import secrets
import ssl
import sys
import threading
import time
import urllib.parse
import uuid
from typing import Any, Dict, List, Optional, Tuple

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa

CODE_TTL = 120
TOKEN_TTL = 3600


def b64u(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode()


def b64u_int(value: int) -> str:
    return b64u(value.to_bytes((value.bit_length() + 7) // 8, 'big'))


class Provider:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.issuer: str = config['issuer'].rstrip('/')
        self.clients = {c['id']: c for c in config['clients']}
        self.users = config['users']
        self.key = rsa.generate_private_key(public_exponent=65537, key_size=2048,
                                            backend=default_backend())
        self.kid = uuid.uuid4().hex[:16]
        self.lock = threading.Lock()
        self.codes: Dict[str, Dict[str, Any]] = {}
        self.refresh: Dict[str, Tuple[str, Dict[str, Any], str]] = {}
        # access token -> (user, expiry)
        self.access: Dict[str, Tuple[Dict[str, Any], float]] = {}

    # -- keys and tokens ------------------------------------------------
    def jwks(self) -> Dict[str, Any]:
        pub = self.key.public_key().public_numbers()
        return {'keys': [{'kty': 'RSA', 'use': 'sig', 'alg': 'RS256', 'kid': self.kid,
                          'n': b64u_int(pub.n), 'e': b64u_int(pub.e)}]}

    def sign(self, claims: Dict[str, Any]) -> str:
        header = {'alg': 'RS256', 'typ': 'JWT', 'kid': self.kid}
        signing_input = (b64u(json.dumps(header, separators=(',', ':')).encode()) + '.'
                         + b64u(json.dumps(claims, separators=(',', ':')).encode()))
        sig = self.key.sign(signing_input.encode(), padding.PKCS1v15(), hashes.SHA256())
        return signing_input + '.' + b64u(sig)

    def user_claims(self, user: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'sub': user.get('sub') or user['username'],
            'email': user['email'],
            'email_verified': True,
            'name': user.get('name') or user['username'],
            'preferred_username': user['username'],
            'groups': user.get('groups', []),
            'roles': user.get('roles', []),
        }

    def issue(self, client_id: str, user: Dict[str, Any], scope: str,
              nonce: Optional[str] = None) -> Dict[str, Any]:
        now = int(time.time())
        base = dict(self.user_claims(user), iss=self.issuer, aud=client_id,
                    iat=now, auth_time=now, exp=now + TOKEN_TTL)
        id_claims = dict(base, jti=str(uuid.uuid4()))
        if nonce:
            id_claims['nonce'] = nonce
        access_claims = dict(base, jti=str(uuid.uuid4()), scope=scope)
        refresh = secrets.token_urlsafe(32)
        access_token = self.sign(access_claims)
        with self.lock:
            self.refresh[refresh] = (client_id, user, scope)
            self.access[access_token] = (user, now + TOKEN_TTL)
            # forget expired access tokens and stale authorization codes
            self.access = {t: v for t, v in self.access.items() if v[1] > now}
            self.codes = {c: v for c, v in self.codes.items() if v['expires'] > now}
        return {'access_token': access_token, 'id_token': self.sign(id_claims),
                'refresh_token': refresh, 'token_type': 'Bearer',
                'expires_in': TOKEN_TTL, 'scope': scope}

    def discovery(self) -> Dict[str, Any]:
        i = self.issuer
        return {
            'issuer': i,
            'authorization_endpoint': f'{i}/auth',
            'token_endpoint': f'{i}/token',
            'jwks_uri': f'{i}/keys',
            'userinfo_endpoint': f'{i}/userinfo',
            'end_session_endpoint': f'{i}/logout',
            'response_types_supported': ['code'],
            'subject_types_supported': ['public'],
            'id_token_signing_alg_values_supported': ['RS256'],
            'scopes_supported': ['openid', 'email', 'profile', 'groups', 'offline_access'],
            'token_endpoint_auth_methods_supported': ['client_secret_basic',
                                                      'client_secret_post'],
            'grant_types_supported': ['authorization_code', 'refresh_token', 'password'],
            'code_challenge_methods_supported': ['S256', 'plain'],
            'claims_supported': ['iss', 'sub', 'aud', 'exp', 'iat', 'jti', 'nonce',
                                 'email', 'email_verified', 'name',
                                 'preferred_username', 'groups', 'roles'],
        }

    # -- lookups ----------------------------------------------------------
    def find_user(self, login: str, password: str) -> Optional[Dict[str, Any]]:
        for user in self.users:
            if login in (user['username'], user['email']) \
                    and secrets.compare_digest(password, user['password']):
                return user
        return None

    def redirect_ok(self, client: Dict[str, Any], uri: str) -> bool:
        parts = urllib.parse.urlsplit(uri)
        return parts.scheme == 'https' and bool(parts.netloc) \
            and parts.path == client.get('redirect_path', '/oauth2/callback')


class Handler(http.server.BaseHTTPRequestHandler):
    server_version = 'cephadm-qa-idp'
    provider: Provider

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write('%s %s\n' % (self.address_string(), fmt % args))
        sys.stderr.flush()

    # -- responses --------------------------------------------------------
    def reply(self, status: int, body: Any = b'', ctype: str = 'application/json',
              headers: Optional[Dict[str, str]] = None) -> None:
        if isinstance(body, (dict, list)):
            body = json.dumps(body).encode()
        elif isinstance(body, str):
            body = body.encode()
        self.send_response(status)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Cache-Control', 'no-store')
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def redirect(self, location: str) -> None:
        self.reply(303, b'', 'text/plain', {'Location': location})

    def oauth_error(self, status: int, error: str, desc: str = '') -> None:
        self.log_message('oauth error %s: %s', error, desc)
        self.reply(status, {'error': error, 'error_description': desc})

    # -- request parsing ----------------------------------------------------
    def query(self) -> Dict[str, str]:
        return dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(self.path).query))

    def form(self) -> Dict[str, str]:
        length = int(self.headers.get('Content-Length') or 0)
        return dict(urllib.parse.parse_qsl(self.rfile.read(length).decode()))

    def route(self) -> str:
        return urllib.parse.urlsplit(self.path).path.rstrip('/') or '/'

    def client_auth(self, form: Dict[str, str]) -> Optional[Dict[str, Any]]:
        cid, secret = form.get('client_id'), form.get('client_secret')
        auth = self.headers.get('Authorization', '')
        if auth.startswith('Basic '):
            raw = base64.b64decode(auth[6:]).decode()
            cid, _, secret = raw.partition(':')
            cid, secret = urllib.parse.unquote(cid), urllib.parse.unquote(secret)
        client = self.provider.clients.get(cid or '')
        if client and secret is not None and secrets.compare_digest(secret, client['secret']):
            return client
        return None

    # -- endpoints ------------------------------------------------------------
    def do_GET(self) -> None:
        p = self.provider
        route = self.route()
        if route == '/.well-known/openid-configuration':
            return self.reply(200, p.discovery())
        if route == '/keys':
            return self.reply(200, p.jwks())
        if route == '/auth':
            return self.authorize(self.query(), None)
        if route == '/userinfo':
            auth = self.headers.get('Authorization', '')
            entry = p.access.get(auth[7:]) if auth.startswith('Bearer ') else None
            user = entry[0] if entry and entry[1] > time.time() else None
            if not user:
                return self.reply(401, {'error': 'invalid_token'},
                                  headers={'WWW-Authenticate': 'Bearer'})
            return self.reply(200, p.user_claims(user))
        if route == '/logout':
            target = self.query().get('post_logout_redirect_uri')
            if target:
                return self.redirect(target)
            return self.reply(200, 'logged out', 'text/plain')
        if route == '/health':
            return self.reply(200, 'OK', 'text/plain')
        return self.reply(404, {'error': 'not_found'})

    def do_POST(self) -> None:
        route = self.route()
        if route == '/auth':
            return self.authorize(self.query(), self.form())
        if route == '/token':
            return self.token(self.form())
        return self.reply(404, {'error': 'not_found'})

    def authorize(self, q: Dict[str, str], form: Optional[Dict[str, str]]) -> None:
        p = self.provider
        client = p.clients.get(q.get('client_id', ''))
        redirect_uri = q.get('redirect_uri', '')
        if not client:
            return self.reply(400, 'unknown client_id', 'text/plain')
        if not p.redirect_ok(client, redirect_uri):
            return self.reply(400, f'redirect_uri not allowed: {redirect_uri}', 'text/plain')
        if q.get('response_type') != 'code':
            return self.redirect(self._with(redirect_uri, error='unsupported_response_type',
                                            state=q.get('state')))
        user = None
        error = ''
        if form is not None:
            user = p.find_user(form.get('login', ''), form.get('password', ''))
            error = '' if user else 'Invalid username or password.'
        if not user:
            # the form posts back here with the original query string
            action = html.escape('/auth?' + urllib.parse.urlencode(q), quote=True)
            page = ('<!doctype html><html><head><title>Log in</title></head><body>'
                    f'<p class="error">{html.escape(error)}</p>'
                    f'<form method="post" action="{action}">'
                    '<input name="login" placeholder="email or username"/>'
                    '<input name="password" type="password"/>'
                    '<button type="submit">Log in</button></form></body></html>')
            return self.reply(200, page, 'text/html; charset=utf-8')
        code = secrets.token_urlsafe(24)
        with p.lock:
            p.codes[code] = {'client_id': client['id'], 'redirect_uri': redirect_uri,
                             'user': user, 'scope': q.get('scope', 'openid'),
                             'nonce': q.get('nonce'),
                             'challenge': q.get('code_challenge'),
                             'method': q.get('code_challenge_method', 'plain'),
                             'expires': time.time() + CODE_TTL}
        self.log_message('authorized %s for %s -> %s', user['username'], client['id'],
                         redirect_uri)
        self.redirect(self._with(redirect_uri, code=code, state=q.get('state')))

    @staticmethod
    def _with(uri: str, **params: Optional[str]) -> str:
        extra = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
        return uri + ('&' if '?' in uri else '?') + extra

    def token(self, form: Dict[str, str]) -> None:
        p = self.provider
        client = self.client_auth(form)
        if not client:
            return self.oauth_error(401, 'invalid_client')
        grant = form.get('grant_type')
        if grant == 'authorization_code':
            with p.lock:
                entry = p.codes.pop(form.get('code', ''), None)
            if not entry or entry['expires'] < time.time() \
                    or entry['client_id'] != client['id']:
                return self.oauth_error(400, 'invalid_grant', 'unknown or expired code')
            if form.get('redirect_uri') and form['redirect_uri'] != entry['redirect_uri']:
                return self.oauth_error(400, 'invalid_grant', 'redirect_uri mismatch')
            if entry['challenge']:
                verifier = form.get('code_verifier', '')
                if entry['method'] == 'S256':
                    verifier = b64u(hashlib.sha256(verifier.encode()).digest())
                if not secrets.compare_digest(verifier, entry['challenge']):
                    return self.oauth_error(400, 'invalid_grant', 'PKCE verification failed')
            return self.reply(200, p.issue(client['id'], entry['user'], entry['scope'],
                                           entry['nonce']))
        if grant == 'refresh_token':
            with p.lock:
                found = p.refresh.pop(form.get('refresh_token', ''), None)
            if not found or found[0] != client['id']:
                return self.oauth_error(400, 'invalid_grant', 'unknown refresh token')
            return self.reply(200, p.issue(client['id'], found[1], found[2]))
        if grant == 'password':
            user = p.find_user(form.get('username', ''), form.get('password', ''))
            if not user:
                return self.oauth_error(400, 'invalid_grant', 'bad credentials')
            return self.reply(200, p.issue(client['id'], user,
                                           form.get('scope', 'openid email profile')))
        return self.oauth_error(400, 'unsupported_grant_type', str(grant))


def main(argv: List[str]) -> int:
    with open(argv[1]) as f:
        config = json.load(f)
    Handler.provider = Provider(config)
    host, _, port = config.get('listen', '0.0.0.0:5556').rpartition(':')
    httpd = http.server.ThreadingHTTPServer((host or '0.0.0.0', int(port)), Handler)
    if config.get('tls_cert'):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(config['tls_cert'], config['tls_key'])
        httpd.socket = ctx.wrap_socket(httpd.socket, server_side=True)
    sys.stderr.write(f"cephadm-qa-idp: issuer {config['issuer']} on {host}:{port}\n")
    sys.stderr.flush()
    httpd.serve_forever()
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
