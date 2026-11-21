#!/usr/bin/env bash
#
# Check that pools created before an upgrade end up with the split_reads
# pool flag once require_osd_release has reached umbrella.
#
# Usage:
#   test_upgrade_split_reads.sh setup   run before the upgrade
#   test_upgrade_split_reads.sh verify  run after the upgrade and after
#                                       "ceph osd require-osd-release" has been
#                                       run
#
# setup:  records require_osd_release, creates a replicated pool and an EC pool
#         with ec_optimizations, then checks their flags.
# verify: checks the same pools again.
#
# In both phases split_reads is expected if and only if the current
# require_osd_release is umbrella or later.
#
set -ex

PHASE=$1

REP_POOL=upgrade-split-reads-rep
EC_POOL=upgrade-split-reads-ec
EC_PROFILE=upgrade-split-reads-profile
# where "setup" records require_osd_release for "verify"
SETUP_RELEASE_KEY=upgrade_test/split_reads/setup_require_osd_release

get_require_osd_release() {
    ceph osd dump --format json | jq -re '.require_osd_release'
}

# Succeeds if split_reads is expected for require_osd_release $1.
# tentacle is the earliest release we can upgrade from in teuthology, all
# post tentacle release that supports split_reads.
expects_split_reads() {
    case "$1" in
        tentacle)
            return 1
            ;;
        *)
            return 0
            ;;
    esac
}

# Print the flags_names of pool $1
pool_flags() {
    ceph osd pool ls detail --format json \
        | jq -re --arg pool "$1" '.[] | select(.pool_name == $pool) | .flags_names'
}

# Succeeds if comma-separated flag_list $1 contains expected_flag $2
has_flag() {
    local flag_list=$1 expected_flag=$2
    echo "$flag_list" | grep -qw "$expected_flag"
}

# check_pool POOL REQUIRE_OSD_RELEASE [ec]
check_pool() {
    local pool=$1 release=$2 ec=$3 flags
    flags=$(pool_flags "$pool")
    echo "pool $pool (require_osd_release $release) flags: $flags"

    if [ "$ec" = ec ] && ! has_flag "$flags" ec_optimizations; then
        echo "FAIL: $pool does not have ec_optimizations"
        exit 1
    fi
    if expects_split_reads "$release"; then
        if ! has_flag "$flags" split_reads; then
            echo "FAIL: $pool is missing split_reads"
            exit 1
        fi
    elif has_flag "$flags" split_reads; then
        echo "FAIL: $pool has split_reads before umbrella"
        exit 1
    fi
}

check_pools() {
    local release=$1
    check_pool "$REP_POOL" "$release"
    check_pool "$EC_POOL" "$release" ec
}

setup() {
    local release
    release=$(get_require_osd_release)
    echo "recording require_osd_release $release"
    ceph config-key set "$SETUP_RELEASE_KEY" "$release"

    ceph osd pool create "$REP_POOL" 8 8 replicated
    ceph osd pool application enable "$REP_POOL" rados

    ceph osd erasure-code-profile set "$EC_PROFILE" \
        k=2 m=1 crush-failure-domain=osd \
        plugin=jerasure technique=reed_sol_van
    ceph osd pool create "$EC_POOL" 8 8 erasure "$EC_PROFILE"
    ceph osd pool set "$EC_POOL" allow_ec_overwrites true
    ceph osd pool set "$EC_POOL" allow_ec_optimizations true
    ceph osd pool application enable "$EC_POOL" rados

    check_pools "$release"
}

verify() {
    local setup_release verify_release
    if ! setup_release=$(ceph config-key get "$SETUP_RELEASE_KEY") || [ -z "$setup_release" ]; then
        echo "FAIL: could not retrieve $SETUP_RELEASE_KEY from config-key"
        exit 1
    fi
    verify_release=$(get_require_osd_release)
    echo "require_osd_release: $setup_release before, $verify_release after"

    check_pools "$verify_release"
}

case "$PHASE" in
    setup) setup ;;
    verify) verify ;;
    *)
        echo "usage: $0 setup|verify" >&2
        exit 2
        ;;
esac

echo OK
