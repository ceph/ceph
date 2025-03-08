#!/bin/bash

. $(dirname $0)/../../standalone/ceph-helpers.sh

function test_pg_splitting() {
    create_pool foo 4
    wait_for_clean || return 1

    #ensure nopgchange is set to 0 to allow for change in pg_num
    ceph osd pool set foo nopgchange 0
    #increase pg_num to induce splitting
    ceph osd pool set foo pg_num 16

    wait_for_clean || return 1

    #get new PG stats
    stats=$(sudo ./bin/ceph pg ls-by-pool foo --format=json 2>/dev/null | jq -r ".pg_stats")
    new_pg_count=$(echo "$stats" | jq '. | length')
    test $new_pg_count == 16 || return 1

    timeout 60 rados bench -p foo 30 write -b 4096 --no-cleanup

    ceph osd pool set foo pg_num 32
    wait_for_clean || return 1
    stats=$(sudo ./bin/ceph pg ls-by-pool foo --format=json 2>/dev/null | jq -r ".pg_stats")
    new_pg_count=$(echo "$stats" | jq '. | length')
    test $new_pg_count == 32 || return 1

}

test_pg_splitting || exit 1

echo "OK"
