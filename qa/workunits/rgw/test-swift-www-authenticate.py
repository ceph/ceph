#!/usr/bin/env python3
"""Check Swift 401 responses include WWW-Authenticate (RFC 9110)."""

import sys
import time

import requests

from common import exec_cmd

POLL_INTERVAL = 0.1
POLL_TIMEOUT = 30


def discover_swift_base():
    """Match qa/workunits/rgw/common.py boto_connect() port order."""
    for port, ssl in (('80', False), ('8000', False), ('443', True)):
        proto = 'https' if ssl else 'http'
        base = '%s://localhost:%s' % (proto, port)
        try:
            requests.get(base + '/swift/v1', timeout=5, verify=False)
        except requests.exceptions.ConnectionError:
            continue
        print('connected to', base)
        return base
    print('FAILED: could not connect to rgw on localhost:80, :8000, or :443')
    sys.exit(1)


def assert_401_challenges(resp, expected_challenges):
    if resp.status_code != 401:
        print('FAILED: expected 401, got %d' % resp.status_code)
        sys.exit(1)
    auth = resp.headers.get('WWW-Authenticate')
    if not auth:
        print('FAILED: missing WWW-Authenticate on 401')
        sys.exit(1)
    for challenge in expected_challenges:
        assert challenge in auth, 'expected %r in %r' % (challenge, auth)


def wait_for_config_change(host, path, headers, expected_substring):
    deadline = time.monotonic() + POLL_TIMEOUT
    while time.monotonic() < deadline:
        r = requests.get(host + path, headers=headers, verify=False)
        auth = r.headers.get('WWW-Authenticate', '')
        if expected_substring in auth:
            return r
        time.sleep(POLL_INTERVAL)
    print('FAILED: timed out waiting for challenge containing %r' % expected_substring)
    sys.exit(1)


def setup_swift_user():
    exec_cmd(
        './bin/radosgw-admin user create '
        '--uid=wwwauth --display-name=wwwauth '
        '--subuser=wwwauth:swift --key-type=swift --access=full --secret=testing '
        '--temp-url-key=repro-key',
        check=False,
    )
    exec_cmd('./bin/radosgw-admin user modify --uid=wwwauth --temp-url-key=repro-key', check=False)


def swift_token_and_base(host):
    r = requests.get(
        host + '/auth/1.0',
        headers={'X-Auth-User': 'wwwauth:swift', 'X-Auth-Key': 'testing'},
        verify=False,
    )
    token = r.headers.get('X-Auth-Token')
    storage = r.headers.get('X-Storage-Url', host + '/swift/v1')
    assert token, 'failed to get swift auth token'
    return token, storage.rstrip('/')


def setup_cors_container(storage, token, container, origin):
    requests.put(
        storage + '/' + container,
        headers={'X-Auth-Token': token},
        verify=False,
    ).raise_for_status()
    requests.post(
        storage + '/' + container,
        headers={
            'X-Auth-Token': token,
            'X-Container-Meta-Access-Control-Allow-Origin': origin,
            'X-Container-Meta-Access-Control-Allow-Methods': 'GET',
        },
        verify=False,
    ).raise_for_status()


BAD_TOKEN_CASES_DEFAULT = [
    ('/swift/v1', 'Swift realm="unknown"'),
    ('/swift/v1/AUTH_nobody', 'Swift realm="unknown"'),
]

BAD_TOKEN_CASES_ACCOUNT_IN_URL = [
    ('/swift/v1/AUTH_nobody', 'Swift realm="AUTH_nobody"'),
    ('/swift/v1/KEY_tester', 'Swift realm="KEY_tester"'),
    (
        '/swift/v1/AUTH_a%0d%0aX-Injected:%20y',
        'Swift realm="AUTH_a%0d%0aX-Injected:%20y"',
    ),
]


def main():
    host = discover_swift_base()
    headers_bad = {'X-Auth-Token': 'bogus'}

    for path, expected in BAD_TOKEN_CASES_DEFAULT:
        r = requests.get(host + path, headers=headers_bad, verify=False)
        assert_401_challenges(r, [expected])

    exec_cmd('ceph config set client rgw_swift_account_in_url true')
    try:
        wait_for_config_change(
            host,
            '/swift/v1/AUTH_nobody',
            headers_bad,
            'AUTH_nobody',
        )
        for path, expected in BAD_TOKEN_CASES_ACCOUNT_IN_URL:
            r = requests.get(host + path, headers=headers_bad, verify=False)
            assert_401_challenges(r, [expected])
            if 'X-Injected' in r.headers:
                print('FAILED: CRLF realm must not inject response headers')
                sys.exit(1)
    finally:
        exec_cmd('ceph config rm client rgw_swift_account_in_url')
        wait_for_config_change(
            host,
            '/swift/v1/AUTH_nobody',
            headers_bad,
            'unknown',
        )

    r = requests.get(
        host + '/auth/1.0',
        headers={'X-Auth-User': 'test:tester', 'X-Auth-Key': 'wrong'},
        verify=False,
    )
    assert_401_challenges(r, ['Swift realm="unknown"'])

    setup_swift_user()
    token, storage = swift_token_and_base(host)
    origin = 'https://repro-example.test'
    container = 'wwwauth-qa-cors'
    setup_cors_container(storage, token, container, origin)

    r = requests.get(
        storage + '/' + container + '/missing',
        headers={'Origin': origin},
        verify=False,
    )
    assert_401_challenges(r, ['Swift realm="unknown"'])
    acao = r.headers.get('Access-Control-Allow-Origin')
    assert acao == origin, 'expected ACAO %r, got %r' % (origin, acao)

    boundary = '----73209boundary'
    body = (
        '--%s\r\n'
        'Content-Disposition: form-data; name="signature"\r\n\r\n'
        'not-a-valid-signature\r\n'
        '--%s\r\n'
        'Content-Disposition: form-data; name="expires"\r\n\r\n'
        '9999999999\r\n'
        '--%s\r\n'
        'Content-Disposition: form-data; name="max_file_size"\r\n\r\n'
        '1024\r\n'
        '--%s\r\n'
        'Content-Disposition: form-data; name="max_file_count"\r\n\r\n'
        '1\r\n'
        '--%s\r\n'
        'Content-Disposition: form-data; name="file"; filename="x.txt"\r\n'
        'Content-Type: text/plain\r\n\r\n'
        'hello\r\n'
        '--%s--\r\n'
    ) % (boundary, boundary, boundary, boundary, boundary, boundary)
    r = requests.post(
        storage + '/' + container + '/formpost-bad.txt',
        headers={'Content-Type': 'multipart/form-data; boundary=' + boundary},
        data=body.encode(),
        verify=False,
    )
    assert r.status_code == 401, 'expected 401 from bad FormPost signature, got %d' % r.status_code
    assert_401_challenges(r, ['Swift realm="unknown"'])

    print('OK')


if __name__ == '__main__':
    main()