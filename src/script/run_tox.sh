#!/usr/bin/env bash

function dump_envvars {
  echo "WITH_PYTHON2: ->$WITH_PYTHON2<-"
  echo "WITH_PYTHON3: ->$WITH_PYTHON3<-"
  echo "ENV_LIST: ->$ENV_LIST<-"
}

get_cmake_variable() {
    grep "$1" $CEPH_BUILD_DIR/CMakeCache.txt | cut -d "=" -f 2
}

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
: ${CEPH_BUILD_DIR:=$script_dir/../../build}
: ${CYTHON_MODULES:=$CEPH_BUILD_DIR/lib/cython_modules}
: ${LD_LIBRARY_PATH:=$CEPH_BUILD_DIR/lib}
: ${CEPH_CONF:=$CEPH_BUILD_DIR/ceph.conf}
: ${WITH_PYTHON2:=$(get_cmake_variable WITH_PYTHON2)}
: ${WITH_PYTHON3:=$(get_cmake_variable WITH_PYTHON3)}

# tox.ini will take care of this.
export CEPH_BUILD_DIR=$CEPH_BUILD_DIR

if [ "$WITH_PYTHON2" = "ON" ]; then
  ENV_LIST+="py27,"
fi
# WITH_PYTHON3 might be set to "ON" or to the python3 RPM version number
# prevailing on the system - e.g. "3", "36"
if [[ "$WITH_PYTHON3" =~ (^3|^ON) ]]; then
  ENV_LIST+="py3,"
fi
# use bash string manipulation to strip off any trailing comma
ENV_LIST=${ENV_LIST%,}

CEPH_CONF=$CEPH_CONF LD_LIBRARY_PATH=$LD_LIBRARY_PATH CYTHON_MODULES=$CYTHON_MODULES tox -c tox.ini -e "${ENV_LIST}" "$@"
TOX_STATUS="$?"
test "$TOX_STATUS" -ne "0" && dump_envvars
exit $TOX_STATUS
