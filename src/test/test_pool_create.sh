#!/bin/bash


#Generic create pool use crush rule  test
#

# Includes
source ../qa/workunits/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:17108"
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done
}

function TEST_pool_create() {
    local dir=$1
    setup $dir || return 1
    run_mon $dir a || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1

    local rulename=testrule
    local poolname=rulepool
    local var=`ceph osd crush rule dump|grep -w ruleset|sed -n '$p'|grep -o '[0-9]\+'`
    var=`expr  $var + 1 `
    ceph osd getcrushmap -o "$dir/map1"
    crushtool -d "$dir/map1" -o "$dir/map1.txt"

    local minsize=0
    local maxsize=1
    sed -i '/# end crush map/i\rule '$rulename' {\n ruleset \'$var'\n type replicated\n min_size \'$minsize'\n max_size \'$maxsize'\n step take default\n step choose firstn 0 type osd\n step emit\n }\n' "$dir/map1.txt"
    crushtool  -c "$dir/map1.txt" -o "$dir/map1.bin"
    ceph osd setcrushmap -i "$dir/map1.bin"
    ceph osd pool create $poolname 200 $rulename 2>"$dir/rev"
    local result=$(cat "$dir/rev" | grep "Error EINVAL: pool size")

    if [ "$result" = "" ];
    then
      ceph osd pool delete  $poolname $poolname  --yes-i-really-really-mean-it
      ceph osd crush rule rm $rulename
      return 1
    fi
    ceph osd crush rule rm $rulename
}

main testpoolcreate

