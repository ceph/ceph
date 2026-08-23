#!/usr/bin/env bash
#
# Regression for the `osd new` / cephx race: the command used to ack as soon as
# the osdmap version was stored, while KeyServer was still on the previous auth
# version. Immediate ceph-osd --mkfs --key then failed with EACCES and left no
# type file (vstart osd.2 on a loaded runner).
#
# Motivating CI:
#   https://github.com/ceph/ceph-nvmeof/actions/runs/32452671723/job/97162876156
#
source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7199" # git grep '\<7199\>' : there must be only one
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

function TEST_osd_new_then_immediate_mkfs() {
    local dir=$1

    # Real cephx: this is the auth path that raced in CI.
    ceph-authtool --create-keyring $dir/ceph.mon.keyring --gen-key -n mon. --cap mon 'allow *'
    ceph-authtool --create-keyring $dir/ceph.client.admin.keyring --gen-key -n client.admin \
        --cap mon 'allow *' --cap osd 'allow *' --cap mgr 'allow *'
    ceph-authtool $dir/ceph.mon.keyring --import-keyring $dir/ceph.client.admin.keyring

    local fsid=$(uuidgen)
    local base_args="--fsid=$fsid --mon-host=$CEPH_MON"
    base_args+=" --auth_cluster_required=cephx --auth_service_required=cephx --auth_client_required=cephx"
    CEPH_ARGS="$base_args --keyring=$dir/ceph.mon.keyring"
    run_mon $dir a || return 1
    CEPH_ARGS="$base_args --keyring=$dir/ceph.client.admin.keyring"
    timeout 60 ceph status || return 1

    local n
    for n in 0 1 2 3 4; do
        local osd_data=$dir/$n
        mkdir -p "$osd_data" || return 1
        local uuid=$(uuidgen)
        local secret=$(ceph-authtool --gen-print-key)
        echo "{\"cephx_secret\": \"$secret\"}" > "$osd_data/new.json"
        local id
        id=$(ceph osd new $uuid -i "$osd_data/new.json") || return 1
        rm -f "$osd_data/new.json"
        [[ "$id" == "$n" ]] || return 1

        # Same client, no delay: KeyServer must already have osd.N.
        ceph auth get osd.$id || return 1

        # Same sequence as vstart: osd new then immediate mkfs as that id.
        # Do not pass --no-mon-config; mkfs must authenticate to the mon as osd.N.
        # Isolate from CEPH_ARGS --keyring=client.admin so we actually use --key.
        ceph-osd -i $id \
            --mkfs --key $secret --name osd.$id --keyring /dev/null \
            --osd-uuid $uuid \
            --osd-data=$osd_data \
            --osd-journal=${osd_data}/journal \
            --osd-journal-size=100 \
            --osd-failsafe-full-ratio=.99 \
            --chdir= \
            --run-dir=$dir \
            --admin-socket=$(get_asok_path) \
            --debug-osd=20 \
            --debug-ms=1 \
            --debug-monc=20 \
            --log-file=$dir/\$name.log \
            --pid-file=$dir/\$name.pid \
            || return 1
        [[ -f "$osd_data/type" ]] || return 1
    done
}

main osd-new-mkfs "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && test/mon/osd-new-mkfs.sh"
# End:
