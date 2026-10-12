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
# Keystone. Uses keystone-fake-server.py which provides six app-cred tokens:
#
#   appcred-token-readonly             - access rules: GET and HEAD on /v1/AUTH_**
#   appcred-token-unrestricted         - unrestricted=True, no access_rules
#   appcred-token-restricted-no-rules  - restricted=True, no access_rules; permits
#   appcred-token-empty-rules          - access_rules: []; denies all
#   appcred-token-wrong-service        - GET rule for compute; denies Swift
#   appcred-token-missing-catalog-service - object-store rule without catalog service; denies

import sys
import requests


# b4221c214dd64ee6a464g2153fae3813 is ID of deadbeef project
SWIFT_BASE = 'http://localhost:8000/swift/v1/AUTH_b4221c214dd64ee6a464g2153fae3813'
CONTAINER = '%s/test-access-rules' % SWIFT_BASE
OBJECT = '%s/testobj' % CONTAINER
KEYSTONE_TOKENS = 'http://localhost:5000/v3/auth/tokens'


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


def test_keystone_requires_access_rules_header():
    """Keystone must hide an access-rule token from unaware services."""
    headers = {
        'X-Auth-Token': 'admin-token-1',
        'X-Subject-Token': 'appcred-token-readonly',
    }
    r = requests.get(KEYSTONE_TOKENS, headers=headers)
    if r.status_code != 404:
        fail('Keystone validation without access-rules header should fail',
             r.status_code)

    headers['OpenStack-Identity-Access-Rules'] = '0.9'
    r = requests.get(KEYSTONE_TOKENS, headers=headers)
    if r.status_code != 404:
        fail('Keystone validation with unsupported access-rules header should fail',
             r.status_code)

    headers['X-Subject-Token'] = 'appcred-token-empty-rules'
    headers.pop('OpenStack-Identity-Access-Rules')
    r = requests.get(KEYSTONE_TOKENS, headers=headers)
    if r.status_code != 404:
        fail('Keystone must also protect an explicitly empty rules field',
             r.status_code)

    headers['X-Subject-Token'] = 'appcred-token-restricted-no-rules'
    r = requests.get(KEYSTONE_TOKENS, headers=headers)
    if r.status_code != 200:
        fail('Keystone should validate a token with no access_rules field',
             r.status_code)

    print('PASSED: Keystone requires the supported access-rules header')


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
    """PUT with a read-only app-cred token must fail authentication."""
    r = requests.put(OBJECT,
                     headers={'X-Auth-Token': 'appcred-token-readonly',
                               'Content-Type': 'text/plain'},
                     data=b'should be denied')
    if r.status_code != 401:
        fail('readonly appcred: PUT object should be denied (401)', r.status_code)
    print('PASSED: readonly appcred denies PUT')


def test_readonly_appcred_denies_delete():
    """DELETE with a read-only app-cred token must fail authentication."""
    r = requests.delete(OBJECT, headers={'X-Auth-Token': 'appcred-token-readonly'})
    if r.status_code != 401:
        fail('readonly appcred: DELETE object should be denied (401)', r.status_code)
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


def test_restricted_no_rules_appcred_permits_all():
    """A restricted app-cred without access_rules must permit every request."""
    token = 'appcred-token-restricted-no-rules'
    r = requests.get(OBJECT, headers={'X-Auth-Token': token})
    if r.status_code != 200:
        fail('restricted-no-rules appcred: GET should be permitted', r.status_code)

    r = requests.head(OBJECT, headers={'X-Auth-Token': token})
    if r.status_code != 200:
        fail('restricted-no-rules appcred: HEAD should be permitted', r.status_code)

    r = requests.put(OBJECT,
                     headers={'X-Auth-Token': token,
                               'Content-Type': 'text/plain'},
                     data=b'overwrite ok')
    if r.status_code not in (200, 201):
        fail('restricted-no-rules appcred: PUT should be permitted', r.status_code)

    print('PASSED: restricted-no-rules appcred permits GET, HEAD, and PUT')


def test_empty_rules_appcred_denies_all():
    """An app-cred with access_rules: [] must fail authentication."""
    token = 'appcred-token-empty-rules'
    r = requests.get(OBJECT, headers={'X-Auth-Token': token})
    if r.status_code != 401:
        fail('empty-rules appcred: GET should be denied (401)', r.status_code)

    r = requests.head(OBJECT, headers={'X-Auth-Token': token})
    if r.status_code != 401:
        fail('empty-rules appcred: HEAD should be denied (401)', r.status_code)

    r = requests.put(OBJECT,
                     headers={'X-Auth-Token': token,
                               'Content-Type': 'text/plain'},
                     data=b'should be denied')
    if r.status_code != 401:
        fail('empty-rules appcred: PUT should be denied (401)', r.status_code)

    print('PASSED: empty-rules appcred denies GET, HEAD, and PUT')


def test_wrong_service_appcred_denies():
    """A rule for a non-accepted service type must not authorize Swift."""
    token = 'appcred-token-wrong-service'
    r = requests.get(OBJECT, headers={'X-Auth-Token': token})
    if r.status_code != 401:
        fail('wrong-service appcred: GET should be denied (401)', r.status_code)

    print('PASSED: wrong-service appcred denies GET')


def test_service_token_bypasses_method_and_path_rules():
    """A valid service token bypasses user-facing method/path matching."""
    r = requests.put(OBJECT,
                     headers={'X-Auth-Token': 'appcred-token-readonly',
                              'X-Service-Token': 'admin-token-1',
                              'Content-Type': 'text/plain'},
                     data=b'service request')
    if r.status_code not in (200, 201):
        fail('valid service token should bypass method/path rules', r.status_code)

    print('PASSED: valid service token bypasses method/path rules')


def test_missing_catalog_service_appcred_denies():
    """A configured service absent from the token catalog must be denied."""
    token = 'appcred-token-missing-catalog-service'
    r = requests.get(OBJECT, headers={'X-Auth-Token': token})
    if r.status_code != 401:
        fail('missing-catalog-service appcred: GET should be denied (401)',
             r.status_code)

    print('PASSED: missing-catalog-service appcred denies GET')

    r = requests.get(OBJECT,
                     headers={'X-Auth-Token': token,
                              'X-Service-Token': 'admin-token-1'})
    if r.status_code != 401:
        fail('service token must not bypass service-catalog validation (401)',
             r.status_code)

    print('PASSED: service token does not bypass catalog validation')


def main():
    test_keystone_requires_access_rules_header()
    setup('admin-token-1')
    try:
        test_readonly_appcred_permits_get()
        test_readonly_appcred_permits_head()
        test_readonly_appcred_denies_put()
        test_readonly_appcred_denies_delete()
        test_unrestricted_appcred_permits_all()
        test_restricted_no_rules_appcred_permits_all()
        test_empty_rules_appcred_denies_all()
        test_wrong_service_appcred_denies()
        test_service_token_bypasses_method_and_path_rules()
        test_missing_catalog_service_appcred_denies()
    finally:
        teardown('admin-token-1')
    print('ALL TESTS PASSED')


if __name__ == '__main__':
    main()
