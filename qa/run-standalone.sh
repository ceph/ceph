#!/usr/bin/env bash
set -e

if [ ! -e CMakeCache.txt -o ! -d bin ]; then
    echo 'run this from the build dir'
    exit 1
fi

function get_cmake_variable() {
    local variable=$1
    grep "$variable" CMakeCache.txt | cut -d "=" -f 2
}

function get_python_path() {
    python_common=$(realpath ../src/python-common)
    echo $(realpath ../src/pybind):$(pwd)/lib/cython_modules/lib.3:$python_common
}

if [ `uname` = FreeBSD ]; then
    # otherwise module prettytable will not be found
    export PYTHONPATH=$(get_python_path):/usr/local/lib/python3.6/site-packages
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
        sudo sysctl -w "${KERNCORE}=${precore}"
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
# If corepattern already set, avoid having to use sudo
if [ "$precore" = "$COREPATTERN" ]; then
    precore=""
else
    sudo sysctl -w "${KERNCORE}=${COREPATTERN}"
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
for f in $(cd $location ; find . -mindepth 2 -perm $exec_mode -type f)
do
    f=$(echo $f | sed 's/\.\///')
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
	    time -f "Elapsed %E (%e seconds)" $cmd ; then
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
