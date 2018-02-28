#!/usr/bin/env bash
set -e

if [ ! -e Makefile -o ! -d bin ]; then
    echo 'run this from the build dir'
    exit 1
fi

TEMP_DIR=${TMPDIR:-/tmp}
if [ ! -d $TEMP_DIR/ceph-disk-virtualenv -o ! -d $TEMP_DIR/ceph-detect-init-virtualenv ]; then
    echo '/tmp/*-virtualenv directories not built. Please run "make check" first.'
    exit 1
fi

function get_cmake_variable() {
    local variable=$1
    grep "$variable" CMakeCache.txt | cut -d "=" -f 2
}

function get_python_path() {
    local ceph_lib=$1
    shift
    local py_ver=$(get_cmake_variable MGR_PYTHON_VERSION | cut -d '.' -f1)
    if [ -z "${py_ver}" ]; then
        if [ $(get_cmake_variable WITH_PYTHON2) = ON ]; then
            py_ver=2
        else
            py_ver=3
        fi
    fi
    echo $(realpath ../src/pybind):$ceph_lib/cython_modules/lib.$py_ver
}

if [ `uname` = FreeBSD ]; then
    # otherwise module prettytable will not be found
    export PYTHONPATH=$(get_python_path):/usr/local/lib/python2.7/site-packages
    exec_mode=+111
    KERNCORE="kern.corefile"
    COREPATTERN="core.%N.%P"
else
    export PYTHONPATH=$(get_python_path)
    exec_mode=/111
    KERNCORE="kernel.core_pattern"
    COREPATTERN="core.%e.%p.%t"
fi

function finish() {
    if [ -n "$precore" ]; then
        sudo sysctl -w ${KERNCORE}=${precore}
    fi
    exit 0
}

trap finish TERM HUP INT

PATH=$(pwd)/bin:$PATH

# TODO: Use getops
dryrun=false
if [[ "$1" = "--dry-run" ]]; then
    dryrun=true
    shift
fi

all=false
if [ "$1" = "" ]; then
   all=true
fi

select=("$@")

location="../qa/standalone"

count=0
errors=0
userargs=""
precore="$(sysctl -n $KERNCORE)"
# If corepattern already set, avoid having to use sudo
if [ "$precore" = "$COREPATTERN" ]; then
    precore=""
else
    sudo sysctl -w ${KERNCORE}=${COREPATTERN}
fi
ulimit -c unlimited
for f in $(cd $location ; find . -perm $exec_mode -type f)
do
    f=$(echo $f | sed 's/\.\///')
    # This is tested with misc/test-ceph-helpers.sh
    if [[ "$f" = "ceph-helpers.sh" ]]; then
        continue
    fi
    if [[ "$all" = "false" ]]; then
        found=false
        for c in "${!select[@]}"
        do
            # Get command and any arguments of subset of tests ro tun
            allargs="${select[$c]}"
            arg1=$(echo "$allargs" | cut --delimiter " " --field 1)
            # Get user args for this selection for use below
            userargs="$(echo $allargs | cut -s --delimiter " " --field 2-)"
            if [[ "$arg1" = $(basename $f) ]]; then
                found=true
                break
            fi
            if [[ "$arg1" = "$f" ]]; then
                found=true
                break
            fi
        done
        if [[ "$found" = "false" ]]; then
            continue
        fi
    fi
    # Don't run test-failure.sh unless explicitly specified
    if [ "$all" = "true" -a "$f" = "special/test-failure.sh" ]; then
        continue
    fi

    cmd="$location/$f $userargs"
    count=$(expr $count + 1)
    echo "--- $cmd ---"
    if [[ "$dryrun" != "true" ]]; then
        if ! PATH=$PATH:bin \
	    CEPH_ROOT=.. \
	    CEPH_LIB=lib \
	    LOCALRUN=yes \
	    $cmd ; then
          echo "$f .............. FAILED"
          errors=$(expr $errors + 1)
        fi
    fi
done
if [ -n "$precore" ]; then
    sudo sysctl -w ${KERNCORE}=${precore}
fi

if [ "$errors" != "0" ]; then
    echo "$errors TESTS FAILED, $count TOTAL TESTS"
    exit 1
fi

echo "ALL $count TESTS PASSED"
exit 0
