#!/usr/bin/env python3
#
# Copyright (C) 2026 SAP SE
#
# Author: Senol Colak <senol.colak@sap.com>
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
#
# Test that RGW enforces application credential access rules received from
# Keystone. Uses keystone-fake-server.py which provides two app-cred tokens:
#
#   appcred-token-readonly    - access rules: GET and HEAD on /v1/AUTH_**
#   appcred-token-unrestricted - no access rules (unrestricted app cred)

import sys
import requests


# b4221c214dd64ee6a464g2153fae3813 is ID of deadbeef project
SWIFT_BASE = 'http://localhost:8000/swift/v1/AUTH_b4221c214dd64ee6a464g2153fae3813'
CONTAINER = '%s/test-access-rules' % SWIFT_BASE
OBJECT = '%s/testobj' % CONTAINER


def fail(msg, status_code=None):
    if status_code is not None:
        print('FAILED: %s (status=%d)' % (msg, status_code))
    else:
        print('FAILED: %s' % msg)
    sys.exit(1)


def setup(admin_token):
    """Create a container and an object using the admin token."""
    r = requests.put(CONTAINER, headers={'X-Auth-Token': admin_token})
    # 409 Conflict is acceptable: container already exists from a previous run
    if r.status_code not in (201, 202, 409):
        fail('setup: create container', r.status_code)
    r = requests.put(OBJECT,
                     headers={'X-Auth-Token': admin_token,
                               'Content-Type': 'text/plain'},
                     data=b'hello')
    # 201 Created or 200 OK (overwrite) are both fine
    if r.status_code not in (200, 201):
        fail('setup: put object', r.status_code)
    print('setup: container and object created')


def teardown(admin_token):
    """Remove the object and container created during setup."""
    requests.delete(OBJECT, headers={'X-Auth-Token': admin_token})
    requests.delete(CONTAINER, headers={'X-Auth-Token': admin_token})
    print('teardown: cleaned up')


def test_readonly_appcred_permits_get():
    """GET with a read-only app-cred token must succeed (rule matches)."""
    r = requests.get(OBJECT, headers={'X-Auth-Token': 'appcred-token-readonly'})
    if r.status_code != 200:
        fail('readonly appcred: GET object should be permitted', r.status_code)
    print('PASSED: readonly appcred permits GET')


def test_readonly_appcred_permits_head():
    """HEAD with a read-only app-cred token must succeed (rule matches)."""
    r = requests.head(OBJECT, headers={'X-Auth-Token': 'appcred-token-readonly'})
    if r.status_code != 200:
        fail('readonly appcred: HEAD object should be permitted', r.status_code)
    print('PASSED: readonly appcred permits HEAD')


def test_readonly_appcred_denies_put():
    """PUT with a read-only app-cred token must be denied (no matching rule)."""
    r = requests.put(OBJECT,
                     headers={'X-Auth-Token': 'appcred-token-readonly',
                               'Content-Type': 'text/plain'},
                     data=b'should be denied')
    # RGW returns 403 (not 401) when the token is valid but an access rule
    # denies the request. 401 is reserved for missing or invalid tokens.
    if r.status_code != 403:
        fail('readonly appcred: PUT object should be denied (403)', r.status_code)
    print('PASSED: readonly appcred denies PUT')


def test_readonly_appcred_denies_delete():
    """DELETE with a read-only app-cred token must be denied (no matching rule)."""
    r = requests.delete(OBJECT, headers={'X-Auth-Token': 'appcred-token-readonly'})
    # RGW returns 403 (not 401) when the token is valid but an access rule
    # denies the request. 401 is reserved for missing or invalid tokens.
    if r.status_code != 403:
        fail('readonly appcred: DELETE object should be denied (403)', r.status_code)
    print('PASSED: readonly appcred denies DELETE')


def test_unrestricted_appcred_permits_all():
    """An unrestricted app-cred (no access rules) must allow all methods."""
    r = requests.get(OBJECT, headers={'X-Auth-Token': 'appcred-token-unrestricted'})
    if r.status_code != 200:
        fail('unrestricted appcred: GET should be permitted', r.status_code)

    r = requests.head(OBJECT, headers={'X-Auth-Token': 'appcred-token-unrestricted'})
    if r.status_code != 200:
        fail('unrestricted appcred: HEAD should be permitted', r.status_code)

    r = requests.put(OBJECT,
                     headers={'X-Auth-Token': 'appcred-token-unrestricted',
                               'Content-Type': 'text/plain'},
                     data=b'overwrite ok')
    if r.status_code not in (200, 201):
        fail('unrestricted appcred: PUT should be permitted', r.status_code)

    print('PASSED: unrestricted appcred permits GET, HEAD, and PUT')


def main():
    setup('admin-token-1')
    try:
        test_readonly_appcred_permits_get()
        test_readonly_appcred_permits_head()
        test_readonly_appcred_denies_put()
        test_readonly_appcred_denies_delete()
        test_unrestricted_appcred_permits_all()
    finally:
        teardown('admin-token-1')
    print('ALL TESTS PASSED')


if __name__ == '__main__':
    main()
