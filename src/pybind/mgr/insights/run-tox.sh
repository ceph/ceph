#!/usr/bin/env bash

# run from ./ or from ../
: ${MGR_INSIGHTS_VIRTUALENV:=$CEPH_BUILD_DIR/mgr-insights-virtualenv}
: ${WITH_PYTHON2:=ON}
: ${WITH_PYTHON3:=ON}
: ${CEPH_BUILD_DIR:=$PWD/.tox}
test -d insights && cd insights

if [ -e tox.ini ]; then
    TOX_PATH=`readlink -f tox.ini`
else
    TOX_PATH=`readlink -f $(dirname $0)/tox.ini`
fi

# tox.ini will take care of this.
unset PYTHONPATH
export CEPH_BUILD_DIR=$CEPH_BUILD_DIR

source ${MGR_INSIGHTS_VIRTUALENV}/bin/activate

if [ "$WITH_PYTHON2" = "ON" ]; then
  ENV_LIST+="py27"
fi
if [ "$WITH_PYTHON3" = "ON" ]; then
  ENV_LIST+="py3"
fi

tox -c ${TOX_PATH} -e ${ENV_LIST}
