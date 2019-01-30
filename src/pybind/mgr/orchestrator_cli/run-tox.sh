#!/usr/bin/env bash

# run from ./ or from ../
: ${MGR_ORCHESTRATOR_CLI_VIRTUALENV:=$CEPH_BUILD_DIR/mgr-orchestrator_cli-virtualenv}
: ${WITH_PYTHON2:=ON}
: ${WITH_PYTHON3:=ON}
: ${CEPH_BUILD_DIR:=$PWD/.tox}
test -d orchestrator_cli && cd orchestrator_cli

if [ -e tox.ini ]; then
    TOX_PATH=$(readlink -f tox.ini)
else
    TOX_PATH=$(readlink -f $(dirname $0)/tox.ini)
fi

# tox.ini will take care of this.
unset PYTHONPATH
export CEPH_BUILD_DIR=$CEPH_BUILD_DIR

if [ -f ${MGR_ORCHESTRATOR_CLI_VIRTUALENV}/bin/activate ]
then
  source ${MGR_ORCHESTRATOR_CLI_VIRTUALENV}/bin/activate
fi

if [ "$WITH_PYTHON2" = "ON" ]; then
  ENV_LIST+="py27"
fi
if [ "$WITH_PYTHON3" = "ON" ]; then
  ENV_LIST+=",py3"
fi

tox -c ${TOX_PATH} -e ${ENV_LIST}
