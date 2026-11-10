#!/usr/bin/env python3
"""
A fake IBM IAM for the RGW STS external authorizer (rgw_ext_authz_ibm.cc).

  POST /identity/token   form: grant_type=...apikey&apikey=<key>
  POST /v2/authz         [{"action", "resource", "subject"}]

The decision is chosen with POST /mock/mode {"mode": "..."}:
  allow          permitted=true                       (default)
  deny           permitted=false
  error          HTTP 500
  malformed      HTTP 200 with a body that is not a decision
  slow           answer after 'delay' seconds
  unauthorized   HTTP 401 for /v2/authz whatever the token
POST /mock/rotate makes the token handed out so far invalid, as if IAM had
revoked it early. The next /identity/token call returns a new one. RGW must
get a new token and retry once, so the request still succeeds.
GET /mock/stats returns the number of token and authz calls and the last
authz request, so a test can check what RGW sent.

Serves https with a self-signed certificate that is written to --cert, which
is what the RGW 'tls_ca_bundle' option takes. Needs only python3 and openssl.
"""

import argparse
import json
import ssl
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

STATE = {'mode': 'allow', 'delay': 0.0, 'token_calls': 0, 'authz_calls': 0,
         'last_authz': None, 'last_headers': None, 'bad_token_calls': 0,
         'token_generation': 0}
LOCK = threading.Lock()
API_KEY = 'test-api-key'


def current_token():
    with LOCK:
        return 'mock-service-token-%d' % STATE['token_generation']


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def _reply(self, code, body=None):
        data = json.dumps(body).encode() if body is not None else b''
        self.send_response(code)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self):
        return self.rfile.read(int(self.headers.get('Content-Length', 0)))

    def do_GET(self):
        if self.path == '/mock/stats':
            with LOCK:
                return self._reply(200, dict(STATE))
        self._reply(404)

    def do_POST(self):
        raw = self._body()
        if self.path == '/mock/mode':
            req = json.loads(raw)
            with LOCK:
                STATE['mode'] = req['mode']
                STATE['delay'] = float(req.get('delay', 0))
            return self._reply(200, {})
        if self.path == '/mock/rotate':
            with LOCK:
                STATE['token_generation'] += 1
            return self._reply(200, {})
        if self.path == '/identity/token':
            form = parse_qs(raw.decode())
            with LOCK:
                STATE['token_calls'] += 1
            if form.get('apikey') != [API_KEY]:
                with LOCK:
                    STATE['bad_token_calls'] += 1
                return self._reply(400, {'errorCode': 'BXNIM0415E'})
            return self._reply(200, {'access_token': current_token(),
                                     'expires_in': 3600,
                                     'token_type': 'Bearer'})
        if self.path == '/v2/authz':
            with LOCK:
                STATE['authz_calls'] += 1
                STATE['last_authz'] = json.loads(raw)
                STATE['last_headers'] = {
                    'authorization': self.headers.get('Authorization'),
                    'transaction-id': self.headers.get('transaction-id')}
                mode, delay = STATE['mode'], STATE['delay']
            if self.headers.get('Authorization') != 'Bearer ' + current_token():
                return self._reply(401, {})
            if mode == 'slow':
                time.sleep(delay)
            if mode in ('error',):
                return self._reply(500, {})
            if mode == 'unauthorized':
                return self._reply(401, {})
            if mode == 'malformed':
                return self._reply(200, {'responses': []})
            permitted = mode != 'deny'
            return self._reply(200, {'responses': [
                {'authorizationDecision': {'permitted': permitted}}]})
        self._reply(404)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--port', type=int, default=8443)
    p.add_argument('--cert', default='/tmp/ext-authz-mock.pem')
    p.add_argument('--host', default='127.0.0.1',
                   help='address to listen on (0.0.0.0 to serve other hosts)')
    p.add_argument('--san', default='DNS:localhost,IP:127.0.0.1',
                   help='subjectAltName of the certificate, e.g. '
                        'DNS:mock.example,IP:10.0.0.5')
    args = p.parse_args()

    key = args.cert + '.key'
    subprocess.check_call([
        'openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
        '-keyout', key, '-out', args.cert, '-days', '2', '-subj',
        '/CN=localhost', '-addext', 'subjectAltName=' + args.san],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(args.cert, key)
    srv = ThreadingHTTPServer((args.host, args.port), Handler)
    srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    srv.serve_forever()


if __name__ == '__main__':
    main()
