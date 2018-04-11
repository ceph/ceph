#!/usr/bin/env bash

# run from ./ or from ../
: ${MGR_DASHBOARD_VIRTUALENV:=/tmp/mgr-dashboard-virtualenv}
: ${WITH_PYTHON2:=ON}
: ${WITH_PYTHON3:=ON}
: ${CEPH_BUILD_DIR:=$PWD/.tox}
test -d dashboard && cd dashboard

if [ -e tox.ini ]; then
    TOX_PATH=`readlink -f tox.ini`
else
    TOX_PATH=`readlink -f $(dirname $0)/tox.ini`
fi

if [ -z $CEPH_BUILD_DIR ]; then
    export CEPH_BUILD_DIR=$(dirname ${TOX_PATH})
fi

source ${MGR_DASHBOARD_VIRTUALENV}/bin/activate

if [ "$WITH_PYTHON2" = "ON" ]; then
  ENV_LIST+="py27-cov,py27-lint,"
fi
if [ "$WITH_PYTHON3" = "ON" ]; then
  ENV_LIST+="py3-cov,py3-lint"
fi

tox -c ${TOX_PATH} -e $ENV_LIST --workdir ${CEPH_BUILD_DIR}
