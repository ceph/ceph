#!/usr/bin/env bash
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#
# Coverage for the LRC "layer-plugin" profile key (tracker 81369). jerasure and
# isa diverge on later coding chunks, so the sub-plugin used for LRC layers must
# be recorded explicitly in the profile rather than inferred. These tests check
# that the monitor stamps the default (isa) into a new profile that does not
# set it, preserves an explicit value (also across an override that omits it),
# accepts only jerasure or isa and only on LRC profiles, warns when the value
# of a profile in use changes, that an OSD down during such a change builds the
# pool's PGs with the new value, and that pools built on such profiles are
# usable.
#
# A patched monitor never leaves an LRC profile keyless, so the keyless cases
# use the dev-only mon_debug_allow_lrc_without_layer_plugin option to reproduce
# a profile that predates the key. With it they check that the monitor raises
# LRC_LAYER_PLUGIN_UNSET, refuses a new pool and refuses to guess on an
# override, that a restarted OSD holding the pool's PGs defers them, does not
# boot and can be stopped cleanly while it waits, that other OSDs refuse to
# create the pool's PGs instead of crashing, that setting layer-plugin lets the
# deferred OSD load its PGs and boot without a restart, and that deleting the
# pool instead lets it tear them down and boot.
#
# This test asserts the monitor stamps layer-plugin=isa, so it only passes with
# the fix built in; do not backport it to branches that lack the fix.
#

source $CEPH_ROOT/qa/standalone/ceph-helpers.sh

function run() {
    local dir=$1
    shift

    export CEPH_MON="127.0.0.1:7160" # git grep '\<7160\>' : there must be only one
    export CEPH_ARGS
    CEPH_ARGS+="--fsid=$(uuidgen) --auth_cluster_required=none --auth_service_required=none --auth_client_required=none "
    CEPH_ARGS+="--mon-host=$CEPH_MON "

    setup $dir || return 1
    run_mon $dir a || return 1
    run_mgr $dir x || return 1
    for id in $(seq 0 9) ; do
        run_osd $dir $id || return 1
    done
    create_rbd_pool || return 1
    wait_for_clean || return 1

    local funcs=${@:-$(set | sed -n -e 's/^\(TEST_[0-9a-z_]*\) .*/\1/p')}
    for func in $funcs ; do
        $func $dir || return 1
    done

    teardown $dir || return 1
}

# An LRC profile created without layer-plugin is stamped with the isa default.
function TEST_layer_plugin_defaulted_to_isa() {
    local dir=$1
    local profile=lrc-default

    ceph osd erasure-code-profile set $profile \
        plugin=lrc k=4 m=2 l=3 \
        crush-failure-domain=osd || return 1
    ceph osd erasure-code-profile get $profile | \
        grep -q '^layer-plugin=isa$' || return 1

    ceph osd erasure-code-profile rm $profile || return 1
}

# An explicit layer-plugin is preserved, not overwritten by the default.
function TEST_layer_plugin_explicit_preserved() {
    local dir=$1
    local profile=lrc-jerasure

    ceph osd erasure-code-profile set $profile \
        plugin=lrc k=4 m=2 l=3 \
        layer-plugin=jerasure \
        crush-failure-domain=osd || return 1
    ceph osd erasure-code-profile get $profile | \
        grep -q '^layer-plugin=jerasure$' || return 1

    ceph osd erasure-code-profile rm $profile || return 1
}

# Only jerasure and isa are accepted, and only on an LRC profile.
function TEST_layer_plugin_validation() {
    local dir=$1

    if ceph osd erasure-code-profile set lrc-typo \
        plugin=lrc k=4 m=2 l=3 layer-plugin=jerasue \
        crush-failure-domain=osd > $dir/out 2>&1; then
        cat $dir/out
        return 1
    fi
    grep -q 'jerasue' $dir/out || return 1
    if ceph osd erasure-code-profile set jer-stray \
        plugin=jerasure k=2 m=1 layer-plugin=isa \
        crush-failure-domain=osd > $dir/out 2>&1; then
        cat $dir/out
        return 1
    fi
    grep -q 'only applies to the lrc plugin' $dir/out || return 1
    rm -f $dir/out
}

# An override that omits layer-plugin keeps the recorded value.
function TEST_layer_plugin_override_preserved() {
    local dir=$1
    local profile=lrc-override

    ceph osd erasure-code-profile set $profile \
        plugin=lrc k=4 m=2 l=3 \
        layer-plugin=jerasure \
        crush-failure-domain=osd || return 1
    ceph osd erasure-code-profile set $profile \
        plugin=lrc k=4 m=2 l=3 \
        crush-failure-domain=osd crush-device-class=hdd \
        --force --yes-i-really-mean-it || return 1
    ceph osd erasure-code-profile get $profile | \
        grep -q '^layer-plugin=jerasure$' || return 1
    ceph osd erasure-code-profile get $profile | \
        grep -q '^crush-device-class=hdd$' || return 1

    ceph osd erasure-code-profile rm $profile || return 1
}

# A pool built on a (defaulted) LRC profile activates and serves I/O, and
# changing the sub-plugin of its profile warns.
function TEST_layer_plugin_pool_put_get() {
    local dir=$1
    local profile=lrc-pool-profile
    local poolname=lrc-pool

    ceph osd erasure-code-profile set $profile \
        plugin=lrc k=4 m=2 l=3 \
        crush-failure-domain=osd || return 1
    create_pool $poolname 12 12 erasure $profile || return 1
    wait_for_clean || return 1

    echo "layer-plugin-roundtrip" > $dir/ORIGINAL
    rados --pool $poolname put SOMETHING $dir/ORIGINAL || return 1
    rados --pool $poolname get SOMETHING $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1

    # Changing the sub-plugin of a profile in use is allowed with --force, but
    # warns that existing data will be decoded with the new one
    ceph osd erasure-code-profile set $profile \
        plugin=lrc k=4 m=2 l=3 layer-plugin=jerasure \
        crush-failure-domain=osd --force --yes-i-really-mean-it \
        > $dir/out 2>&1 || return 1
    grep -q "WARNING: changed layer-plugin .* from isa to jerasure .*$poolname" \
        $dir/out || return 1

    delete_pool $poolname || return 1
    ceph osd erasure-code-profile rm $profile || return 1
    rm -f $dir/ORIGINAL $dir/COPY $dir/out
}

# An OSD that is down while the layer-plugin of a profile in use changes builds
# the pool's PGs with the new value when it starts, not with the value in the
# map it stored before going down.
function TEST_layer_plugin_change_while_osd_down() {
    local dir=$1
    local profile=lrc-change-down
    local poolname=lrc-change-down-pool
    local lrc="plugin=lrc k=4 m=2 l=3 crush-failure-domain=osd"

    ceph osd erasure-code-profile set $profile $lrc layer-plugin=isa || return 1
    create_pool $poolname 4 4 erasure $profile || return 1
    wait_for_clean || return 1
    echo "layer-plugin-change-while-down" > $dir/ORIGINAL
    rados --pool $poolname put SOMETHING $dir/ORIGINAL || return 1

    local osd=$(get_osds $poolname SOMETHING | awk '{print $1}')
    local pgid=$(ceph osd map $poolname SOMETHING -f json | jq -r .pgid)
    kill_daemons $dir TERM osd.$osd || return 1
    ceph osd down $osd
    ceph osd erasure-code-profile set $profile $lrc layer-plugin=jerasure \
        --force --yes-i-really-mean-it || return 1

    # Only look at what the OSD logs after it restarts
    local lines=$(wc -l < $dir/osd.$osd.log)
    activate_osd $dir $osd || return 1
    tail -n +$((lines + 1)) $dir/osd.$osd.log > $dir/restart.log
    grep -q "_make_pg ${pgid}s[0-9]* LRC layer-plugin=jerasure" \
        $dir/restart.log || return 1
    if grep -q "_make_pg ${pgid}s[0-9]* LRC layer-plugin=isa" \
        $dir/restart.log; then
        return 1
    fi

    wait_for_clean || return 1
    delete_pool $poolname || return 1
    ceph osd erasure-code-profile rm $profile || return 1
    rm -f $dir/ORIGINAL $dir/restart.log
}

# Wait until the OSD log, from line $2 on, says the OSD is not booting because
# an LRC profile has no valid layer-plugin
function wait_for_lrc_not_booting() {
    local log=$1
    local from=$2

    for i in $(seq 1 60); do
        if tail -n +$from $log | \
            grep -q "not booting: waiting for 'layer-plugin'"; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# A keyless LRC profile (as left by an upgrade from before layer-plugin
# existed): the monitor raises LRC_LAYER_PLUGIN_UNSET, refuses to create a pool
# on it and refuses to guess on an override, a restarted OSD holding
# the pool's PGs does not boot and can be stopped cleanly while it waits, other
# OSDs refuse to create the pool's PGs without crashing, and setting the key
# lets the OSD boot on its own.
function TEST_layer_plugin_keyless_profile() {
    local dir=$1
    local profile=lrc-keyless
    local poolname=lrc-keyless-pool
    local lrc="plugin=lrc k=4 m=2 l=3 crush-failure-domain=osd"

    ceph osd erasure-code-profile set $profile $lrc || return 1
    create_pool $poolname 4 4 erasure $profile || return 1
    wait_for_clean || return 1
    head -c 1048576 /dev/urandom > $dir/ORIGINAL
    rados --pool $poolname put SOMETHING $dir/ORIGINAL || return 1

    # Drop the key from the profile, as if it predated layer-plugin. Running
    # OSDs keep the codec they built their PGs with.
    ceph config set mon mon_debug_allow_lrc_without_layer_plugin true || return 1
    ceph osd erasure-code-profile set $profile $lrc \
        --force --yes-i-really-mean-it || return 1
    ceph config set mon mon_debug_allow_lrc_without_layer_plugin false || return 1
    if ceph osd erasure-code-profile get $profile | grep -q '^layer-plugin='; then
        return 1
    fi

    # The monitor raises LRC_LAYER_PLUGIN_UNSET as an error, since a pool uses
    # the profile
    local found=no
    for i in $(seq 1 30); do
        if ceph health detail | grep -q '\[ERR\] LRC_LAYER_PLUGIN_UNSET'; then
            found=yes
            break
        fi
        sleep 1
    done
    test $found = yes || return 1
    ceph health detail | grep -q "profile '$profile'.*$poolname" || return 1

    # A new pool cannot be created against the keyless profile
    if ceph osd pool create $poolname-new 4 4 erasure $profile > $dir/out 2>&1
    then
        cat $dir/out
        return 1
    fi
    grep -q 'layer-plugin' $dir/out || return 1

    # An override that omits the key is refused rather than guessed
    if ceph osd erasure-code-profile set $profile $lrc \
        --force --yes-i-really-mean-it > $dir/out 2>&1; then
        cat $dir/out
        return 1
    fi
    grep -q 'layer-plugin' $dir/out || return 1

    # A restarted OSD holding the pool's PGs defers them and does not boot.
    # Without fast shutdown the OSD tears down its map cache on exit, and with
    # debug_asserts_on_shutdown a map reference still held then aborts it.
    local osd=$(get_osds $poolname SOMETHING | awk '{print $1}')
    local osd_args="--osd-fast-shutdown=false --debug-asserts-on-shutdown=true"
    kill_daemons $dir TERM osd.$osd || return 1
    ceph osd down $osd
    local lines=$(wc -l < $dir/osd.$osd.log)
    activate_osd $dir $osd $osd_args > /dev/null 2>&1 &   # waits until it boots
    wait_for_lrc_not_booting $dir/osd.$osd.log $((lines + 1)) || return 1
    test "$(ceph osd dump -f json | \
        jq ".osds[] | select(.osd == $osd) | .up")" = 0 || return 1

    # Stopping it while it waits releases the maps its deferred PGs pinned
    lines=$(wc -l < $dir/osd.$osd.log)
    kill_daemons $dir TERM osd.$osd || return 1
    if tail -n +$((lines + 1)) $dir/osd.$osd.log | \
        grep -E 'leaked refs|Caught signal|FAILED ceph_assert'; then
        return 1
    fi
    lines=$(wc -l < $dir/osd.$osd.log)
    activate_osd $dir $osd > /dev/null 2>&1 &
    wait_for_lrc_not_booting $dir/osd.$osd.log $((lines + 1)) || return 1

    # Remapping its PGs onto other OSDs must not crash them: they refuse to
    # create PGs whose sub-plugin is unknown
    ceph osd out $osd || return 1
    sleep 15
    test "$(ceph osd stat -f json | jq .num_up_osds)" = 9 || return 1
    if grep -lE 'Caught signal|ceph_abort_msg|has no valid layer-plugin' $dir/osd.*.log; then
        return 1
    fi

    # Setting the key lets the deferred OSD load its PGs and boot on its own
    ceph osd erasure-code-profile set $profile $lrc layer-plugin=isa \
        --force --yes-i-really-mean-it || return 1
    wait_for_osd up $osd || return 1
    ceph osd in $osd || return 1
    wait_for_clean || return 1
    if ceph health detail | grep -q LRC_LAYER_PLUGIN_UNSET; then
        return 1
    fi
    rados --pool $poolname get SOMETHING $dir/COPY || return 1
    diff $dir/ORIGINAL $dir/COPY || return 1

    delete_pool $poolname || return 1
    ceph osd erasure-code-profile rm $profile || return 1
    rm -f $dir/ORIGINAL $dir/COPY $dir/out
}

# Deleting a keyless LRC pool, instead of setting the key, lets an OSD waiting
# on its PGs tear them down and boot without crashing.
function TEST_layer_plugin_keyless_pool_deleted() {
    local dir=$1
    local profile=lrc-keyless-deleted
    local poolname=lrc-keyless-deleted-pool
    local lrc="plugin=lrc k=4 m=2 l=3 crush-failure-domain=osd"

    ceph osd erasure-code-profile set $profile $lrc || return 1
    create_pool $poolname 4 4 erasure $profile || return 1
    wait_for_clean || return 1
    echo "layer-plugin-keyless-deleted" > $dir/ORIGINAL
    rados --pool $poolname put SOMETHING $dir/ORIGINAL || return 1

    ceph config set mon mon_debug_allow_lrc_without_layer_plugin true || return 1
    ceph osd erasure-code-profile set $profile $lrc \
        --force --yes-i-really-mean-it || return 1
    ceph config set mon mon_debug_allow_lrc_without_layer_plugin false || return 1

    local osd=$(get_osds $poolname SOMETHING | awk '{print $1}')
    kill_daemons $dir TERM osd.$osd || return 1
    ceph osd down $osd
    local lines=$(wc -l < $dir/osd.$osd.log)
    activate_osd $dir $osd > /dev/null 2>&1 &   # waits until the OSD boots
    wait_for_lrc_not_booting $dir/osd.$osd.log $((lines + 1)) || return 1

    delete_pool $poolname || return 1
    wait_for_osd up $osd || return 1
    if tail -n +$((lines + 1)) $dir/osd.$osd.log | \
        grep -E 'Caught signal|FAILED ceph_assert|ceph_abort_msg'; then
        return 1
    fi
    wait_for_clean || return 1

    ceph osd erasure-code-profile rm $profile || return 1
    rm -f $dir/ORIGINAL
}

main test-erasure-code-lrc-layer-plugin "$@"

# Local Variables:
# compile-command: "cd ../.. ; make -j4 && \
#    qa/standalone/erasure-code/test-erasure-code-lrc-layer-plugin.sh"
# End:
