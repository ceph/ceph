#!/bin/bash

. $(dirname $0)/../../standalone/ceph-helpers.sh

set -x

function test_pg_splitting() {
    ceph osd pool delete foo foo --yes-i-really-really-mean-it || true
    create_pool foo 4
    wait_for_clean || return 1

    #ensure nopgchange is set to 0 to allow for change in pg_num
    ceph osd pool set foo nopgchange 0
    #increase pg_num to induce splitting
    ceph osd pool set foo pg_num 16
    wait_for_clean || return 1
    ceph osd pool set foo pgp_num 16
    wait_for_clean || return 1

    #get new PG stats
    stats=$(ceph pg ls-by-pool foo --format=json 2>/dev/null | jq -r ".pg_stats")
    new_pg_count=$(echo "$stats" | jq '. | length')
    echo "New PG Count: $new_pg_count"
    test "$new_pg_count" -eq 16 || return 1
}

test_pg_splitting || exit 1

echo "OK"
