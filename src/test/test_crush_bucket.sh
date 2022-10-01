#!/usr/bin/env bash


#Generic test_crush_bucket  test
#

# Includes
source $(dirname $0)/detect-build-env-vars.sh
source ../qa/standalone/ceph-helpers.sh
function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:17119" # git grep '\<17119\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth-supported=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | ${SED} -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done
}

function TEST_crush_bucket() {
    local dir=$1
    setup $dir || return 1
    run_mon $dir a || return 1
    run_osd $dir 0 || return 1
    run_osd $dir 1 || return 1
    run_osd $dir 2 || return 1


    ceph osd getcrushmap -o "$dir/map1" || return 1
    crushtool -d "$dir/map1" -o "$dir/map1.txt"|| return 1
    local var=`ceph osd crush dump|grep -w id|grep '-'|grep -Eo '[0-9]+'|sort|uniq|${SED} -n '$p'`
    local id=`expr  $var + 1`
    local item=`${SED} -n '/^root/,/}/p' $dir/map1.txt|grep  'item'|head -1`
    local weight=`${SED} -n '/^root/,/}/p' $dir/map1.txt|grep  'item'|head -1|awk '{print $4}'`
    local bucket="host test {\n id -$id\n # weight $weight\n alg straw \n hash 0  # rjenkins1 \n $item\n}\n"
    ${SED} -i "/# buckets/a\ $bucket" "$dir/map1.txt"
    crushtool  -c "$dir/map1.txt" -o "$dir/map1.bin" 2>"$dir/rev"
    local result=$(cat "$dir/rev")
    if [ "$result" != "" ];
    then
      return 1
    fi

}

main testcrushbucket

