#!/usr/bin/env bash
#
# Check that 'ceph osd pg-upmap-items' refuses mappings that would not
# take effect instead of reporting success and then dropping them.
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7173" # git grep '\<7173\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        setup $dir || return 1
        $func $dir || return 1
        teardown $dir || return 1
    done
}

function count_upmap_items() {
    local pgid=$1
    ceph osd dump -f json | \
        jq "[.pg_upmap_items[] | select(.pgid == \"$pgid\")] | length"
}

function TEST_pg_upmap_items_validation() {
    local dir=$1
    local osds=6

    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for osd in $(seq 0 $(expr $osds - 1)) ; do
        run_osd $dir $osd || return 1
    done

    ceph osd set-require-min-compat-client luminous || return 1
    ceph balancer off || return 1
    create_pool upmap-test 8 8 || return 1
    ceph osd pool set upmap-test size 3 || return 1
    wait_for_clean || return 1

    local poolid=$(ceph osd pool ls detail -f json | \
        jq '.[] | select(.pool_name == "upmap-test") | .pool_id')
    local pgid=$poolid.0
    local in_pg=($(ceph pg map $pgid -f json | jq '.up[]'))
    local not_in_pg=()
    for osd in $(seq 0 $(expr $osds - 1)) ; do
        if ! echo " ${in_pg[*]} " | grep -q " $osd " ; then
            not_in_pg+=($osd)
        fi
    done
    test ${#in_pg[@]} -eq 3 || return 1
    test ${#not_in_pg[@]} -eq 3 || return 1
    local a=${in_pg[0]}
    local b=${in_pg[1]}
    local x=${not_in_pg[0]}
    local y=${not_in_pg[1]}
    local z=${not_in_pg[2]}

    # an unknown pg is still ENOENT
    expect_failure $dir ENOENT ceph osd pg-upmap-items 99.ff $a $x || return 1

    # swap two osds and then swap them back
    expect_failure $dir EINVAL ceph osd pg-upmap-items $pgid $a $x $x $a || return 1
    expect_failure $dir EINVAL ceph osd pg-upmap-items $pgid $a $b $b $a || return 1
    expect_failure $dir EINVAL ceph osd pg-upmap-items $pgid $x $y $y $x || return 1
    # the same osd as source and target, alone or next to a valid pair
    expect_failure $dir "cannot remap osd.$a to itself" \
        ceph osd pg-upmap-items $pgid $a $a || return 1
    expect_failure $dir "cannot remap osd.$x to itself" \
        ceph osd pg-upmap-items $pgid $a $y $x $x || return 1
    # the source is not in the pg
    expect_failure $dir "is not in the CRUSH mapping" \
        ceph osd pg-upmap-items $pgid $x $y || return 1
    # the target is already in the pg
    expect_failure $dir "is already in the mapping" \
        ceph osd pg-upmap-items $pgid $a $b || return 1
    # the same source twice
    expect_failure $dir "already remapped by an earlier pair" \
        ceph osd pg-upmap-items $pgid $a $x $a $y || return 1
    # the target is marked out
    ceph osd out $z || return 1
    expect_failure $dir "is marked out" \
        ceph osd pg-upmap-items $pgid $a $z || return 1
    ceph osd in $z || return 1

    # none of the above left anything behind
    test "$(count_upmap_items $pgid)" = "0" || return 1

    # a valid mapping is accepted and takes effect
    ceph osd pg-upmap-items $pgid $a $x $b $y || return 1
    test "$(count_upmap_items $pgid)" = "1" || return 1
    local up=$(ceph pg map $pgid -f json | jq -c '.up | sort')
    local expected=$(echo "[$x,$y,${in_pg[2]}]" | jq -c 'sort')
    test "$up" = "$expected" || return 1

    ceph osd rm-pg-upmap-items $pgid || return 1
    test "$(count_upmap_items $pgid)" = "0" || return 1
}

main osd-pg-upmap-items "$@"

# Local Variables:
# compile-command: "cd ../../../build ; make -j4 && \
#    ../qa/run-standalone.sh osd-pg-upmap-items.sh"
# End:
