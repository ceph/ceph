#!/bin/bash
#
# Copyright (C) 2015 SUSE LINUX GmbH
# Copyright (C) 2016 <contact@redhat.com>
#
# Author: Owen Synge <osynge@suse.com>
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

# run from the ceph-detect-init directory or from its parent
: ${CEPH_DETECT_INIT_VIRTUALENV:=ceph-detect-init-virtualenv}
test -d ceph-detect-init && cd ceph-detect-init
source ${CEPH_DETECT_INIT_VIRTUALENV}/bin/activate
tox > ${CEPH_DETECT_INIT_VIRTUALENV}/tox.out 2>&1
status=$?
grep -v InterpreterNotFound < ${CEPH_DETECT_INIT_VIRTUALENV}/tox.out
exit $status
