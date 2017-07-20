#!/usr/bin/env bash
#
# Copyright (C) 2015 <contact@redhat.com>
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

# run from the ceph-disk directory or from its parent
: ${CEPH_DISK_VIRTUALENV:=/tmp/ceph-disk-virtualenv}
test -d ceph-disk && cd ceph-disk

if [ -e tox.ini ]; then
    TOX_PATH=`readlink -f tox.ini`
else
    TOX_PATH=`readlink -f $(dirname $0)/tox.ini`
fi

if [ -z $CEPH_BUILD_DIR ]; then
    export CEPH_BUILD_DIR=$(dirname ${TOX_PATH})
fi

source ${CEPH_DISK_VIRTUALENV}/bin/activate
tox -c ${TOX_PATH}
