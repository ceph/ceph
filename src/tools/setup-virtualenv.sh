#!/usr/bin/env bash
#
# Copyright (C) 2016 <contact@redhat.com>
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

DIR=$1
rm -fr $DIR
mkdir -p $DIR
virtualenv --python python2.7 $DIR
. $DIR/bin/activate

if pip --help | grep -q disable-pip-version-check; then
    DISABLE_PIP_VERSION_CHECK=--disable-pip-version-check
else
    DISABLE_PIP_VERSION_CHECK=
fi

# older versions of pip will not install wrap_console scripts
# when using wheel packages
pip $DISABLE_PIP_VERSION_CHECK --log $DIR/log.txt install --upgrade 'pip >= 6.1'

# workaround of https://github.com/pypa/setuptools/issues/1042
pip $DISABLE_PIP_VERSION_CHECK --log $DIR/log.txt install --upgrade "setuptools < 36"

if pip --help | grep -q disable-pip-version-check; then
    DISABLE_PIP_VERSION_CHECK=--disable-pip-version-check
else
    DISABLE_PIP_VERSION_CHECK=
fi

if test -d wheelhouse ; then
    export NO_INDEX=--no-index
fi

pip $DISABLE_PIP_VERSION_CHECK --log $DIR/log.txt install $NO_INDEX --use-wheel --find-links=file://$(pwd)/wheelhouse 'tox >=1.9'
if test -f requirements.txt ; then
    pip $DISABLE_PIP_VERSION_CHECK --log $DIR/log.txt install $NO_INDEX --use-wheel --find-links=file://$(pwd)/wheelhouse -r requirements.txt
fi
