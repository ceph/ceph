#!/usr/bin/env python3
"""Check Swift 401 responses include WWW-Authenticate (RFC 9110)."""

import sys
import time

import requests

from common import exec_cmd

CEPH_CFG_PROPAGATION_WAIT = 5


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


def assert_swift_challenge(resp, realm_substring=None):
    if resp.status_code != 401:
        print('FAILED: expected 401, got %d' % resp.status_code)
        sys.exit(1)
    auth = resp.headers.get('WWW-Authenticate') or resp.headers.get('www-authenticate')
    if not auth:
        print('FAILED: missing WWW-Authenticate on 401')
        sys.exit(1)
    if 'Swift realm=' not in auth:
        print('FAILED: WWW-Authenticate lacks Swift challenge: %r' % auth)
        sys.exit(1)
    if realm_substring is not None and realm_substring not in auth:
        print('FAILED: expected realm containing %r in %r' % (realm_substring, auth))
        sys.exit(1)


def assert_no_crlf_injected_header(resp):
    if 'X-Injected' in resp.headers:
        print('FAILED: CRLF realm must not inject response headers: %r' % dict(resp.headers))
        sys.exit(1)


def main():
    host = discover_swift_base()

    # Default cluster config: realm is unknown when account is not in the URL.
    r = requests.get(
        host + '/swift/v1',
        headers={'X-Auth-Token': 'bogus'},
        verify=False,
    )
    assert_swift_challenge(r, 'unknown')

    exec_cmd('ceph config set client rgw_swift_account_in_url true')
    time.sleep(CEPH_CFG_PROPAGATION_WAIT)
    try:
        # Early auth failure (abort_early), not op send_response().
        r = requests.get(
            host + '/swift/v1/AUTH_nobody',
            headers={'X-Auth-Token': 'bogus'},
            verify=False,
        )
        assert_swift_challenge(r, 'AUTH_nobody')

        r = requests.get(
            host + '/swift/v1/AUTH_a%0d%0aX-Injected:%20y',
            headers={'X-Auth-Token': 'bogus'},
            verify=False,
        )
        assert_swift_challenge(r)
        assert_no_crlf_injected_header(r)

        r = requests.get(
            host + '/swift/v1/KEY_tester',
            headers={'X-Auth-Token': 'bogus'},
            verify=False,
        )
        assert_swift_challenge(r, 'KEY_tester')
    finally:
        exec_cmd('ceph config rm client rgw_swift_account_in_url')
        time.sleep(CEPH_CFG_PROPAGATION_WAIT)

    r = requests.get(
        host + '/auth/1.0',
        headers={'X-Auth-User': 'test:tester', 'X-Auth-Key': 'wrong'},
        verify=False,
    )
    assert_swift_challenge(r)

    print('OK')


if __name__ == '__main__':
    main()
