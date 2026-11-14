#!/usr/bin/env python3
"""Check an oversized request header section is answered with 431 and Date
(RFC 6585 section 5, RFC 9110 section 6.6.1)."""

import os
import sys

import requests

HOST = os.environ.get('RGW_ENDPOINT', 'http://localhost:8000')

# beast rejects a header section larger than max_header_size (16 KiB by
# default), use something well above that
OVERSIZED = 'a' * 70000


def main():
    r = requests.get(HOST + '/', headers={'X-Big': OVERSIZED})
    if r.status_code != 431:
        print('FAILED: expected 431 for an oversized header, got %d'
              % r.status_code)
        sys.exit(1)
    if not r.headers.get('Date'):
        print('FAILED: missing Date on the 431 response')
        sys.exit(1)
    if not r.headers['Date'].endswith(' GMT'):
        print('FAILED: Date is not an IMF-fixdate: %r' % r.headers['Date'])
        sys.exit(1)

    # a normal request on a fresh connection is unaffected
    r = requests.get(HOST + '/')
    if r.status_code >= 400 and r.status_code != 403:
        print('FAILED: unexpected status %d for a normal request'
              % r.status_code)
        sys.exit(1)

    print('OK')


if __name__ == '__main__':
    main()
