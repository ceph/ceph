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
