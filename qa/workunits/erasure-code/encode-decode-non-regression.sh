#!/bin/bash -ex
#
# Copyright (C) 2014 Red Hat <contact@redhat.com>
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
source $(dirname $0)/../../../src/test/detect-build-env-vars.sh

: ${CORPUS:=https://github.com/ceph/ceph-erasure-code-corpus.git}
: ${DIRECTORY:=$CEPH_ROOT/ceph-erasure-code-corpus}

# when running from sources, the current directory must have precedence
export PATH=:$PATH

if ! test -d $DIRECTORY ; then
    git clone $CORPUS $DIRECTORY
fi

my_version=v$(ceph --version | cut -f3 -d ' ')

all_versions=$((ls -d $DIRECTORY/v* ; echo $DIRECTORY/$my_version ) | sort)

for version in $all_versions ; do
    if test -d $version ; then
        $version/non-regression.sh
    fi
    if test $version = $DIRECTORY/$my_version ; then
        break
    fi
done
