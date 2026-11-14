#!/usr/bin/env python3
"""
A minimal OIDC issuer for testing AssumeRoleWithWebIdentity against RGW.

  GET /.well-known/openid-configuration   discovery document
  GET /jwks                               signing key as a bare RSA n/e key
  GET /token?sub=<sub>&aud=<aud>          a freshly signed RS256 JWT

Plain http on purpose: RGW verifies https issuers against the system trust
store, and with rgw_enable_jwks_url_verification off (the default) a bare n/e
key is accepted without a thumbprint check. Needs only python3 and openssl.
"""

import argparse
import base64
import json
import subprocess
import tempfile
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

KID = 'mock-key-1'
STATE = {}


def b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode()


def sign(data, keyfile):
    return subprocess.check_output(
        ['openssl', 'dgst', '-sha256', '-sign', keyfile], input=data)


def make_token(sub, aud, lifetime):
    now = int(time.time())
    header = {'alg': 'RS256', 'typ': 'JWT', 'kid': KID}
    claims = {'iss': STATE['issuer'], 'sub': sub, 'aud': aud, 'iat': now,
              'exp': now + lifetime}
    signing_input = (b64url(json.dumps(header).encode()) + '.' +
                     b64url(json.dumps(claims).encode()))
    sig = sign(signing_input.encode(), STATE['keyfile'])
    return signing_input + '.' + b64url(sig)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _reply(self, code, body, ctype='application/json'):
        data = body.encode() if isinstance(body, str) else json.dumps(body).encode()
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path == '/.well-known/openid-configuration':
            return self._reply(200, {'issuer': STATE['issuer'],
                                     'jwks_uri': STATE['issuer'] + '/jwks'})
        if url.path == '/jwks':
            return self._reply(200, {'keys': [{
                'kty': 'RSA', 'use': 'sig', 'alg': 'RS256', 'kid': KID,
                'n': STATE['n'], 'e': 'AQAB'}]})
        if url.path == '/token':
            q = parse_qs(url.query)
            sub = q.get('sub', ['mock-user'])[0]
            aud = q.get('aud', ['rgw-client'])[0]
            lifetime = int(q.get('lifetime', ['3600'])[0])
            return self._reply(200, make_token(sub, aud, lifetime), 'text/plain')
        self._reply(404, {})


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, default=8080)
    p.add_argument('--issuer', required=True,
                   help='issuer URL the tokens carry and RGW will fetch, '
                        'e.g. http://ext-authz-oidc.openshift-storage.svc:8080')
    args = p.parse_args()

    keyfile = tempfile.mkdtemp() + '/key.pem'
    subprocess.check_call(['openssl', 'genrsa', '-out', keyfile, '2048'],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    out = subprocess.check_output(
        ['openssl', 'rsa', '-in', keyfile, '-noout', '-modulus']).decode()
    modulus_hex = out.strip().split('=', 1)[1]
    STATE['n'] = b64url(bytes.fromhex(modulus_hex))
    STATE['keyfile'] = keyfile
    STATE['issuer'] = args.issuer.rstrip('/')

    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()


if __name__ == '__main__':
    main()
