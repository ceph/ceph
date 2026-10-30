#!/usr/bin/env python3
"""
HTTP/TLS client used by the cephadm service tests.

The test modules copy this file to the cluster host and run it inside
``cephadm shell``, so it only depends on what the Ceph container image already
ships (python3 + python3-cryptography). It is not imported by teuthology.

Redirects are never followed implicitly: the tests assert on them.

Sub-commands (every command prints a single JSON document on stdout):

  batch FILE
      Run a list of requests described in FILE (a JSON list of objects with
      the keys accepted by request(): url, method, user, password, bearer,
      cacert, cert, key, jar, headers, form) and print the list of results.
      One cephadm shell start-up for a whole set of checks.

  login URL --user U --password P --jar JAR
      Walk a browser-style OIDC login starting at URL: follow redirects,
      fill the identity provider's login form when one is served, and keep
      the cookies in JAR (Mozilla format) for later requests.

  token ISSUER --client-id ID --client-secret S --user U --password P
      OAuth2 password grant against ISSUER/token. Prints the tokens and the
      decoded ID token claims.

  selfsigned PREFIX --cn CN [--ip IP]... [--dns NAME]...
      Write a self-signed server certificate (PREFIX.crt, PREFIX.key).

Result of a request:
  {"url", "status", "location", "headers", "body", "json", "error"}
  status is 0 when no HTTP response was received (connection refused, TLS
  handshake or client certificate rejected...); error says why.
"""

import argparse
import base64
import datetime
import html
import http.client
import http.cookiejar
import ipaddress
import json
import os
import re
import socket
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

BODY_LIMIT = 64 * 1024
TIMEOUT = 20


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


def _ssl_context(cacert: Optional[str], cert: Optional[str], key: Optional[str]) -> ssl.SSLContext:
    # Daemons are reached by IP address or short host name; with a CA file
    # the chain is what is verified, not the name. Without one the peer is
    # not verified at all (check_hostname must go off before verify_mode).
    if cacert:
        ctx = ssl.create_default_context(cafile=cacert)
        ctx.check_hostname = False
        # Newer Pythons turn on strict X.509 checks by default (e.g. require
        # an Authority Key Identifier). The tests check which CA a daemon's
        # chain leads to, not certificate profile details.
        ctx.verify_flags &= ~getattr(ssl, 'VERIFY_X509_STRICT', 0)
    else:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    if cert:
        ctx.load_cert_chain(cert, key)
    return ctx


def _load_jar(path: Optional[str]) -> Optional[http.cookiejar.MozillaCookieJar]:
    if not path:
        return None
    jar = http.cookiejar.MozillaCookieJar(path)
    if os.path.exists(path):
        jar.load(ignore_discard=True, ignore_expires=True)
    return jar


def _save_jar(jar: Optional[http.cookiejar.MozillaCookieJar]) -> None:
    if jar is not None:
        jar.save(ignore_discard=True, ignore_expires=True)


def _result(url: str, status: int, headers: Any, body: bytes) -> Dict[str, Any]:
    text = body[:BODY_LIMIT].decode('utf-8', errors='replace')
    res: Dict[str, Any] = {
        'url': url,
        'status': status,
        'location': headers.get('Location') if headers else None,
        'headers': dict(headers.items()) if headers else {},
        'body': text,
        'json': None,
        'error': None,
    }
    try:
        res['json'] = json.loads(text)
    except ValueError:
        pass
    return res


def _error(url: str, exc: BaseException) -> Dict[str, Any]:
    reason = getattr(exc, 'reason', exc)
    return {'url': url, 'status': 0, 'location': None, 'headers': {}, 'body': '',
            'json': None, 'error': f'{type(reason).__name__}: {reason}'}


def request(url: str, method: str = 'GET', user: Optional[str] = None,
            password: Optional[str] = None, bearer: Optional[str] = None,
            cacert: Optional[str] = None, cert: Optional[str] = None,
            key: Optional[str] = None, jar: Any = None,
            headers: Optional[Dict[str, str]] = None,
            form: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    cookie_jar = _load_jar(jar) if isinstance(jar, str) else jar
    hdrs = dict(headers or {})
    if user is not None:
        token = base64.b64encode(f'{user}:{password or ""}'.encode()).decode()
        hdrs['Authorization'] = f'Basic {token}'
    if bearer:
        hdrs['Authorization'] = f'Bearer {bearer}'
    data = None
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
        hdrs.setdefault('Content-Type', 'application/x-www-form-urlencoded')
    handlers: List[Any] = [
        urllib.request.ProxyHandler({}),  # never go through an environment proxy
        urllib.request.HTTPSHandler(context=_ssl_context(cacert, cert, key)),
        _NoRedirect(),
    ]
    if cookie_jar is not None:
        handlers.append(urllib.request.HTTPCookieProcessor(cookie_jar))
    opener = urllib.request.build_opener(*handlers)
    req = urllib.request.Request(url, data=data, method=method, headers=hdrs)
    try:
        with opener.open(req, timeout=TIMEOUT) as resp:
            res = _result(url, resp.status, resp.headers, resp.read())
    except urllib.error.HTTPError as e:
        # 3xx/4xx/5xx: a real HTTP answer, which is what the tests look at
        res = _result(url, e.code, e.headers, e.read())
    except (urllib.error.URLError, ssl.SSLError, ConnectionError,
            socket.timeout, OSError, http.client.HTTPException) as e:
        res = _error(url, e)
    if isinstance(jar, str):
        _save_jar(cookie_jar)
    return res


def cmd_batch(args: argparse.Namespace) -> Any:
    with open(args.file) as f:
        reqs = json.load(f)
    return [request(**r) for r in reqs]


def _browser_url(url: str) -> str:
    """Drop a default port, as a browser does: oauth2-proxy compares the
    host:port of the post-login redirect with its allowlist."""
    parts = urllib.parse.urlsplit(url)
    default = {'https': 443, 'http': 80}.get(parts.scheme)
    if parts.port is not None and parts.port == default:
        host = parts.hostname or ''
        netloc = f'[{host}]' if ':' in host else host
        parts = parts._replace(netloc=netloc)
    return urllib.parse.urlunsplit(parts)


def cmd_login(args: argparse.Namespace) -> Dict[str, Any]:
    jar = _load_jar(args.jar)
    url = _browser_url(args.url)
    method = 'GET'
    form: Optional[Dict[str, str]] = None
    hops: List[Dict[str, Any]] = []
    redirect_uris: List[str] = []
    res: Dict[str, Any] = {}
    for _ in range(args.max_hops):
        res = request(url, method=method, jar=jar, form=form)
        hops.append({'url': url, 'method': method, 'status': res['status'],
                     'location': res['location'], 'error': res['error']})
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(url).query)
        redirect_uris += query.get('redirect_uri', [])
        if 300 <= res['status'] < 400 and res['location']:
            url = _browser_url(urllib.parse.urljoin(url, res['location']))
            method, form = 'GET', None
            continue
        body = res['body']
        if res['status'] == 200 and form is None and re.search(r'name="password"', body):
            # the identity provider's login page: post the credentials to the
            # form action (or back to the same URL when there is none)
            m = re.search(r'<form[^>]*action="([^"]*)"', body)
            action = html.unescape(m.group(1)) if m and m.group(1) else url
            url = urllib.parse.urljoin(url, action)
            method = 'POST'
            form = {'login': args.user, 'password': args.password}
            continue
        break
    _save_jar(jar)
    return {'final_url': url, 'status': res.get('status'), 'body': res.get('body', ''),
            'json': res.get('json'), 'error': res.get('error'), 'hops': hops,
            'redirect_uris': redirect_uris}


def _decode_jwt(token: str) -> Dict[str, Any]:
    payload = token.split('.')[1]
    payload += '=' * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload))


def cmd_token(args: argparse.Namespace) -> Dict[str, Any]:
    res = request(f'{args.issuer.rstrip("/")}/token', method='POST',
                  user=args.client_id, password=args.client_secret,
                  form={'grant_type': 'password', 'username': args.user,
                        'password': args.password, 'scope': args.scope})
    tokens = res.get('json') or {}
    if res['status'] != 200 or 'id_token' not in tokens:
        return {'ok': False, 'response': res}
    return {'ok': True, 'id_token': tokens['id_token'],
            'access_token': tokens.get('access_token'),
            'claims': _decode_jwt(tokens['id_token'])}


def cmd_selfsigned(args: argparse.Namespace) -> Dict[str, Any]:
    from cryptography import x509
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048,
                                   backend=default_backend())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, args.cn)])
    sans: List[x509.GeneralName] = [x509.DNSName(d) for d in args.dns]
    sans += [x509.IPAddress(ipaddress.ip_address(ip)) for ip in args.ip]
    now = datetime.datetime.now(datetime.timezone.utc)
    builder = (x509.CertificateBuilder()
               .subject_name(name).issuer_name(name)
               .public_key(key.public_key())
               .serial_number(x509.random_serial_number())
               .not_valid_before(now - datetime.timedelta(days=1))
               .not_valid_after(now + datetime.timedelta(days=30)))
    if sans:
        builder = builder.add_extension(x509.SubjectAlternativeName(sans), critical=False)
    cert = builder.sign(key, hashes.SHA256(), default_backend())
    with open(f'{args.prefix}.crt', 'wb') as f:
        f.write(cert.public_bytes(serialization.Encoding.PEM))
    with open(f'{args.prefix}.key', 'wb') as f:
        f.write(key.private_bytes(serialization.Encoding.PEM,
                                  serialization.PrivateFormat.PKCS8,
                                  serialization.NoEncryption()))
    os.chmod(f'{args.prefix}.key', 0o600)
    return {'crt': f'{args.prefix}.crt', 'key': f'{args.prefix}.key'}


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='cmd', required=True)

    s = sub.add_parser('batch')
    s.add_argument('file')
    s.set_defaults(func=cmd_batch)

    s = sub.add_parser('login')
    s.add_argument('url')
    s.add_argument('--user', required=True)
    s.add_argument('--password', required=True)
    s.add_argument('--jar', required=True)
    s.add_argument('--max-hops', type=int, default=25)
    s.set_defaults(func=cmd_login)

    s = sub.add_parser('token')
    s.add_argument('issuer')
    s.add_argument('--client-id', required=True)
    s.add_argument('--client-secret', required=True)
    s.add_argument('--user', required=True)
    s.add_argument('--password', required=True)
    s.add_argument('--scope', default='openid email profile')
    s.set_defaults(func=cmd_token)

    s = sub.add_parser('selfsigned')
    s.add_argument('prefix')
    s.add_argument('--cn', required=True)
    s.add_argument('--ip', action='append', default=[])
    s.add_argument('--dns', action='append', default=[])
    s.set_defaults(func=cmd_selfsigned)

    args = parser.parse_args(argv)
    json.dump(args.func(args), sys.stdout)
    sys.stdout.write('\n')
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
