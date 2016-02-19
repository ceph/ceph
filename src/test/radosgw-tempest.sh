#!/bin/bash
#
# Copyright (C) 2015 Mirantis
#
# Author: Radoslaw Zarzynski <rzarzynski@mirantis.com>
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


RGW_PARAMS='--rgw_swift_enforce_content_length=true
            --rgw_keystone_url=http://127.0.0.1:35367
            --rgw keystone_accepted_roles=tempest,admin
            --rgw_keystone_admin_token=ADMIN' \
MON=1 OSD=3 CEPH_START='mon osd -r' CEPH_PORT=7212 test/vstart_wrapper.sh \
    ../qa/workunits/rgw/tempest.sh
