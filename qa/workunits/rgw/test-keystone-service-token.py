#!/usr/bin/env python3
#
# Copyright (C) 2022 Binero
#
# Author: Tobias Urdin <tobias.urdin@binero.com>
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

import sys
import requests
import time


# b4221c214dd64ee6a464g2153fae3813 is ID of deadbeef project
SWIFT_URL = 'http://localhost:8000/swift/v1/AUTH_b4221c214dd64ee6a464g2153fae3813'
KEYSTONE_URL = 'http://localhost:5000'


def get_stats():
    stats_url = '%s/stats' % KEYSTONE_URL
    return requests.get(stats_url)


def test_list_containers():
    # Loop five list container requests with same token
    for i in range(0, 5):
        r = requests.get(SWIFT_URL, headers={'X-Auth-Token': 'user-token-1'})
        if r.status_code != 204:
            print('FAILED, status code is %d not 204' % r.status_code)
            sys.exit(1)

    # Get stats from fake Keystone server
    r = get_stats()
    if r.status_code != 200:
        print('FAILED, status code is %d not 200' % r.status_code)
        sys.exit(1)
    stats = r.json()

    # Verify admin token was cached
    if stats['post_total'] != 1:
        print('FAILED, post_total stat is %d not 1' % stats['post_total'])
        sys.exit(1)

    # Verify user token was cached
    if stats['get_total'] != 1:
        print('FAILED, get_total stat is %d not 1' % stats['get_total'])
        sys.exit(1)

    print('Wait for cache to be invalid')
    time.sleep(11)

    r = requests.get(SWIFT_URL, headers={'X-Auth-Token': 'user-token-1'})
    if r.status_code != 204:
        print('FAILED, status code is %d not 204' % r.status_code)
        sys.exit(1)

    # Get stats from fake Keystone server
    r = get_stats()
    if r.status_code != 200:
        print('FAILED, status code is %d not 200' % r.status_code)
        sys.exit(1)
    stats = r.json()

    if stats['post_total'] != 2:
        print('FAILED, post_total stat is %d not 2' % stats['post_total'])
        sys.exit(1)

    if stats['get_total'] != 2:
        print('FAILED, get_total stat is %d not 2' % stats['get_total'])
        sys.exit(1)


def test_expired_token():
    # Try listing containers with an expired token
    for i in range(0, 3):
        r = requests.get(SWIFT_URL, headers={'X-Auth-Token': 'user-token-2'})
        if r.status_code != 401:
            print('FAILED, status code is %d not 401' % r.status_code)
            sys.exit(1)

    # Get stats from fake Keystone server
    r = get_stats()
    if r.status_code != 200:
        print('FAILED, status code is %d not 200' % r.status_code)
        sys.exit(1)
    stats = r.json()

    # Verify admin token was cached
    if stats['post_total'] != 2:
        print('FAILED, post_total stat is %d not 2' % stats['post_total'])
        sys.exit(1)

    # Verify we got to fake Keystone server since expired tokens is not cached
    if stats['get_total'] != 5:
        print('FAILED, get_total stat is %d not 5' % stats['get_total'])
        sys.exit(1)


def test_expired_token_with_service_token():
    # Try listing containers with an expired token but with a service token
    for i in range(0, 3):
        r = requests.get(SWIFT_URL, headers={'X-Auth-Token': 'user-token-2', 'X-Service-Token': 'admin-token-1'})
        if r.status_code != 204:
            print('FAILED, status code is %d not 204' % r.status_code)
            sys.exit(1)

    # Get stats from fake Keystone server
    r = get_stats()
    if r.status_code != 200:
        print('FAILED, status code is %d not 200' % r.status_code)
        sys.exit(1)
    stats = r.json()

    # Verify admin token was cached
    if stats['post_total'] != 2:
        print('FAILED, post_total stat is %d not 2' % stats['post_total'])
        sys.exit(1)

    # Verify we got to fake Keystone server since expired tokens is not cached
    if stats['get_total'] != 7:
        print('FAILED, get_total stat is %d not 7' % stats['get_total'])
        sys.exit(1)

    print('Wait for cache to be invalid')
    time.sleep(11)

    r = requests.get(SWIFT_URL, headers={'X-Auth-Token': 'user-token-2', 'X-Service-Token': 'admin-token-1'})
    if r.status_code != 204:
        print('FAILED, status code is %d not 204' % r.status_code)
        sys.exit(1)

    # Get stats from fake Keystone server
    r = get_stats()
    if r.status_code != 200:
        print('FAILED, status code is %d not 200' % r.status_code)
        sys.exit(1)
    stats = r.json()

    if stats['post_total'] != 3:
        print('FAILED, post_total stat is %d not 3' % stats['post_total'])
        sys.exit(1)

    if stats['get_total'] != 9:
        print('FAILED, get_total stat is %d not 9' % stats['get_total'])
        sys.exit(1)


def test_expired_token_with_invalid_service_token():
    print('Wait for cache to be invalid')
    time.sleep(11)

    # Test with a token that doesn't have allowed role as service token
    for i in range(0, 3):
        r = requests.get(SWIFT_URL, headers={'X-Auth-Token': 'user-token-2', 'X-Service-Token': 'user-token-1'})
        if r.status_code != 401:
            print('FAILED, status code is %d not 401' % r.status_code)
            sys.exit(1)

    # Make sure we get user-token-1 cached
    r = requests.get(SWIFT_URL, headers={'X-Auth-Token': 'user-token-1'})
    if r.status_code != 204:
        print('FAILED, status code is %d not 204' % r.status_code)
        sys.exit(1)

    # Test that a cached token (that is invalid as service token) cannot be used as service token
    for i in range(0, 3):
        r = requests.get(SWIFT_URL, headers={'X-Auth-Token': 'user-token-2', 'X-Service-Token': 'user-token-1'})
        if r.status_code != 401:
            print('FAILED, status code is %d not 401' % r.status_code)
            sys.exit(1)


def main():
    test_list_containers()
    test_expired_token()
    test_expired_token_with_service_token()
    test_expired_token_with_invalid_service_token()


if __name__ == '__main__':
    main()
