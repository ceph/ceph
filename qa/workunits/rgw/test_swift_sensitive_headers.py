#!/usr/bin/env python3

import logging as log
import os
import requests
from common import exec_cmd

"""
Tests filtering of sensitive Swift headers for non-owners.
"""

OWNER_UID = "swift-owner"
OWNER_SUBUSER = "swift-owner:swift"
OWNER_SECRET = "ownersecret"

NON_OWNER_UID = "swift-nonowner"
NON_OWNER_SUBUSER = "swift-nonowner:swift"
NON_OWNER_SECRET = "nonownersecret"

CONTAINER_NAME = "test-sensitive-headers-container"


def setup_swift_user(uid, subuser, secret):
    exec_cmd(f'radosgw-admin user create --uid={uid} --display-name="Test {uid}"')
    exec_cmd(f"radosgw-admin subuser create --uid={uid} --subuser={subuser} --access=full")
    exec_cmd(f"radosgw-admin key create --subuser={subuser} --key-type=swift --secret={secret}")


def teardown_user(uid):
    exec_cmd(f"radosgw-admin user rm --uid={uid} --purge-data")


def get_swift_auth(endpoint, subuser, secret):
    headers = {"X-Auth-User": subuser, "X-Auth-Key": secret}
    resp = requests.get(f"{endpoint}/auth/1.0", headers=headers)
    resp.raise_for_status()
    return resp.headers["X-Storage-Url"], resp.headers["X-Auth-Token"]


def main():
    log.basicConfig(level=log.DEBUG)
    endpoint = os.environ.get("RGW_URL", "http://localhost:8000")

    setup_swift_user(OWNER_UID, OWNER_SUBUSER, OWNER_SECRET)
    setup_swift_user(NON_OWNER_UID, NON_OWNER_SUBUSER, NON_OWNER_SECRET)

    owner_url = None
    owner_token = None
    try:
        owner_url, owner_token = get_swift_auth(endpoint, OWNER_SUBUSER, OWNER_SECRET)
        non_owner_url, non_owner_token = get_swift_auth(endpoint, NON_OWNER_SUBUSER, NON_OWNER_SECRET)

        sensitive_headers = {
            "X-Auth-Token": owner_token,
            "X-Container-Read": ".r:*",
            "X-Container-Meta-Temp-Url-Key": "supersecretkey",
        }
        resp = requests.put(f"{owner_url}/{CONTAINER_NAME}", headers=sensitive_headers)
        resp.raise_for_status()

        log.debug("TEST: owner can see sensitive swift headers\n")
        owner_resp = requests.head(f"{owner_url}/{CONTAINER_NAME}", headers={"X-Auth-Token": owner_token})
        owner_resp.raise_for_status()

        headers_lower = {k.lower(): v for k, v in owner_resp.headers.items()}
        assert ("x-container-read" in headers_lower), "FAIL: Owner is missing X-Container-Read"
        assert ("x-container-meta-temp-url-key" in headers_lower), "FAIL: Owner is missing Temp-Url-Key"

        non_owner_resp = requests.head(f"{owner_url}/{CONTAINER_NAME}", headers={"X-Auth-Token": non_owner_token})
        non_owner_resp.raise_for_status()

        no_headers_lower = {k.lower(): v for k, v in non_owner_resp.headers.items()}
        assert ("x-container-read" not in no_headers_lower), "FAIL: Non-owner has access to X-Container-Read"
        assert ("x-container-meta-temp-url-key" not in no_headers_lower), "FAIL: Non-owner has access to Temp-Url-Key"
        assert ("x-account-meta-temp-url-key" not in no_headers_lower), "FAIL: Non-owner has access to Account Temp-Url-Key"

    finally:
        log.debug(f"Deleting container {CONTAINER_NAME}")
        if owner_url and owner_token:
            requests.delete(f"{owner_url}/{CONTAINER_NAME}", headers={"X-Auth-Token": owner_token})
        teardown_user(OWNER_UID)
        teardown_user(NON_OWNER_UID)


if __name__ == "__main__":
    main()
