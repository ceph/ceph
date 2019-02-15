#!/usr/bin/env bash
set -e

if [ ! -e Makefile -o ! -d bin ]; then
    echo 'run this from the build dir'
    exit 1
fi

function get_cmake_variable() {
    local variable=$1
    grep "$variable" CMakeCache.txt | cut -d "=" -f 2
}

function get_python_path() {
    local py_ver=$(get_cmake_variable MGR_PYTHON_VERSION | cut -d '.' -f1)
    if [ -z "${py_ver}" ]; then
        if [ $(get_cmake_variable WITH_PYTHON2) = ON ]; then
            py_ver=2
        else
            py_ver=3
        fi
    fi
    echo $(realpath ../src/pybind):$(pwd)/lib/cython_modules/lib.$py_ver
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

function cleanup() {
    if [ -n "$precore" ]; then
        sudo sysctl -w ${KERNCORE}=${precore}
    fi
}

function finish() {
    cleanup
    exit 0
}

trap finish TERM HUP INT

PATH=$(pwd)/bin:$PATH

# add /sbin and /usr/sbin to PATH to find sysctl in those cases where the
# user's PATH does not get these directories by default (e.g., tumbleweed)
PATH=$PATH:/sbin:/usr/sbin

export LD_LIBRARY_PATH="$(pwd)/lib"

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

if [[ "${precore:0:1}" = "|" ]]; then
  precore="${precore:1}"
fi

# If corepattern already set, avoid having to use sudo
if [ "$precore" = "$COREPATTERN" ]; then
    precore=""
else
    sudo sysctl -w ${KERNCORE}=${COREPATTERN}
fi
# Clean out any cores in core target directory (currently .)
if ls $(dirname $(sysctl -n $KERNCORE)) | grep -q '^core\|core$' ; then
    mkdir found.cores.$$ 2> /dev/null || true
    for i in $(ls $(dirname $(sysctl -n $KERNCORE)) | grep '^core\|core$'); do
	mv $i found.cores.$$
    done
    echo "Stray cores put in $(pwd)/found.cores.$$"
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
            # Get command and any arguments of subset of tests to run
            allargs="${select[$c]}"
            arg1=$(echo "$allargs" | cut --delimiter " " --field 1)
            # Get user args for this selection for use below
            userargs="$(echo $allargs | cut -s --delimiter " " --field 2-)"
            if [[ "$arg1" = $(basename $f) ]] || [[  "$arg1" = $(dirname $f) ]]; then
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
cleanup

if [ "$errors" != "0" ]; then
    echo "$errors TESTS FAILED, $count TOTAL TESTS"
    exit 1
fi

echo "ALL $count TESTS PASSED"
exit 0
