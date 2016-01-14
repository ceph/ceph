#!/bin/bash
#
# Copyright (C) 2015 Red Hat <contact@redhat.com>
#
# Author: David Zafman <dzafman@redhat.com>
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
CEPH_CLI_TEST_DUP_COMMAND=1 \
MON=1 OSD=3 CEPH_START='mon osd' CEPH_PORT=7205 $CEPH_ROOT/src/test/vstart_wrapper.sh \
    $CEPH_ROOT/src/test/test_rados_tool.sh
