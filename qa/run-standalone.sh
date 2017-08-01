#!/usr/bin/env bash
set -e

if [ ! -e Makefile -o ! -d bin ]; then
    echo 'run this from the build dir'
    exit 1
fi

if [ ! -d /tmp/ceph-disk-virtualenv -o ! -d /tmp/ceph-detect-init-virtualenv ]; then
    echo '/tmp/*-virtualenv directories not built'
    exit 1
fi

if [ `uname` = FreeBSD ]; then
    # otherwise module prettytable will not be found
    export PYTHONPATH=/usr/local/lib/python2.7/site-packages
    exec_mode=+111
else
    export PYTHONPATH=/usr/lib/python2.7/dist-packages
    exec_mode=/111
fi

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
            if [[ "${select[$c]}" = $(basename $f) ]]; then
                found=true
                break
            fi
            if [[ "${select[$c]}" = "$f" ]]; then
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

    count=$(expr $count + 1)
    echo "--- $f ---"
    if [[ "$dryrun" != "true" ]]; then
        if ! PATH=$PATH:bin \
	    CEPH_ROOT=.. \
	    CEPH_LIB=lib \
	    $location/$f ; then
          echo "$f .............. FAILED"
          errors=$(expr $errors + 1)
        fi
    fi
done

if [ "$errors" != "0" ]; then
    echo "$errors TESTS FAILED, $count TOTAL TESTS"
    exit 1
fi

echo "ALL $count TESTS PASSED"
exit 0
