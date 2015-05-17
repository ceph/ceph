#!/bin/bash
#
# Copyright (C) 2015 SUSE LINUX GmbH
# Copyright (C) 2015 <contact@redhat.com>
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
test -d ceph-detect-init && cd ceph-detect-init
trap "rm -fr make-check" EXIT
virtualenv make-check
. make-check/bin/activate
# older versions of pip will not install wrap_console scripts
# when using wheel packages
pip --log make-check/log.txt install --upgrade 'pip >= 6.1'
if test -d wheelhouse ; then
    export NO_INDEX=--no-index
fi
pip --log make-check/log.txt install $NO_INDEX --use-wheel --find-links=file://$(pwd)/wheelhouse --upgrade distribute
pip --log make-check/log.txt install $NO_INDEX --use-wheel --find-links=file://$(pwd)/wheelhouse 'tox >=1.9' 
tox > make-check/tox.out 2>&1 
status=$?
grep -v InterpreterNotFound < make-check/tox.out
exit $status
