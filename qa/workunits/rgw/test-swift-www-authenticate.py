#!/usr/bin/env python3
"""Check Swift 401 responses include WWW-Authenticate (RFC 9110)."""

import sys

import requests

HOST = 'http://localhost:8000'


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


def main():
    r = requests.get(
        HOST + '/swift/v1/AUTH_nobody',
        headers={'X-Auth-Token': 'bogus'},
    )
    assert_swift_challenge(r, 'AUTH_nobody')

    r = requests.get(
        HOST + '/auth/1.0',
        headers={'X-Auth-User': 'test:tester', 'X-Auth-Key': 'wrong'},
    )
    assert_swift_challenge(r)

    print('OK')


if __name__ == '__main__':
    main()
