#!/usr/bin/env bash

function dump_envvars {
  echo "WITH_PYTHON2: ->$WITH_PYTHON2<-"
  echo "WITH_PYTHON3: ->$WITH_PYTHON3<-"
  echo "TOX_PATH: ->$TOX_PATH<-"
  echo "ENV_LIST: ->$ENV_LIST<-"
}

# run from ./ or from ../
: ${CEPH_BUILD_DIR:=$PWD/.tox}
: ${MGR_DASHBOARD_VIRTUALENV:=$CEPH_BUILD_DIR/mgr-dashboard-virtualenv}
: ${WITH_PYTHON2:=ON}
: ${WITH_PYTHON3:=3}
test -d dashboard && cd dashboard

if [ -e tox.ini ]; then
    TOX_PATH=$(readlink -f tox.ini)
else
    TOX_PATH=$(readlink -f $(dirname $0)/tox.ini)
fi

# tox.ini will take care of this.
unset PYTHONPATH
export CEPH_BUILD_DIR=$CEPH_BUILD_DIR

source ${MGR_DASHBOARD_VIRTUALENV}/bin/activate

if [ "$WITH_PYTHON2" = "ON" ]; then
  if [[ -n "$@" ]]; then
    ENV_LIST+="py27-run,"
  else
    ENV_LIST+="py27-cov,py27-lint,"
  fi
fi
# WITH_PYTHON3 might be set to "ON" or to the python3 RPM version number
# prevailing on the system - e.g. "3", "36"
if [[ "$WITH_PYTHON3" =~ (^3|^ON) ]]; then
  if [[ -n "$@" ]]; then
    ENV_LIST+="py3-run,"
  else
    ENV_LIST+="py3-cov,py3-lint"
  fi
fi
# use bash string manipulation to strip off any trailing comma
ENV_LIST=${ENV_LIST%,}

tox -c "${TOX_PATH}" -e "${ENV_LIST}" "$@"
TOX_STATUS="$?"
test "$TOX_STATUS" -ne "0" && dump_envvars
exit $TOX_STATUS
