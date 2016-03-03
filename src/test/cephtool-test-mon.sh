#!/bin/bash
#
# Copyright (C) 2014, 2015 Red Hat <contact@redhat.com>
# Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
#
# Author: Loic Dachary <loic@dachary.org>
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
# uses CEPH_PORT going from 7202 7203 and 7204 because
# it starts at 7202 and runs 3 mons (see vstart.sh)
MON=3 OSD=3 CEPH_START='mon osd' CEPH_PORT=7202 $CEPH_ROOT/src/test/vstart_wrapper.sh \
    $CEPH_ROOT/qa/workunits/cephtool/test.sh \
    --test-mon \
    --asok-does-not-need-root
