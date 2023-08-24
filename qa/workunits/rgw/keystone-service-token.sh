#!/usr/bin/env bash
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
#
<<comment Running this script with vstart should be should have these options
MON=1 OSD=1 MDS=0 MGR=0 RGW=1 ../src/vstart.sh -n -d -o 'rgw_keystone_accepted_admin_roles="admin"
	rgw_keystone_accepted_roles="admin,Member"
	rgw_keystone_admin_domain="Default"
	rgw_keystone_admin_password="ADMIN"
	rgw_keystone_admin_project="admin"
	rgw_keystone_admin_user="admin"
	rgw_keystone_api_version=3
	rgw_keystone_expired_token_cache_expiration=10
	rgw_keystone_implicit_tenants=true
	rgw_keystone_service_token_accepted_roles="admin"
	rgw_keystone_service_token_enabled=true
	rgw_keystone_url="http://localhost:5000"
	rgw_swift_account_in_url=true
	rgw_swift_enforce_content_length=true
	rgw_swift_versioning_enabled=true'
comment

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

trap cleanup EXIT

function cleanup() {
  kill $KEYSTONE_FAKE_SERVER_PID
  wait
}

function run() {
  $CEPH_ROOT/qa/workunits/rgw//keystone-fake-server.py &
  KEYSTONE_FAKE_SERVER_PID=$!
  # Give fake Keystone server some seconds to startup
  sleep 5
  $CEPH_ROOT/qa/workunits/rgw/test-keystone-service-token.py
}

main keystone-service-token "$@"
